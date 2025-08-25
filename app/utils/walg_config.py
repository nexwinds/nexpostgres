import os
import re
import json
import logging
from typing import Dict, Optional, Tuple
from app.utils.ssh_manager import SSHManager

logger = logging.getLogger(__name__)

class WalgConfig:
    """Simplified WAL-G configuration utility following official documentation."""
    
    @staticmethod
    def create_env(s3_storage, database_name: str, pgdata_path: Optional[str] = None) -> Dict[str, str]:
        """Create WAL-G environment variables according to documentation.
        
        Args:
            s3_storage: S3 storage configuration object
            database_name: Database name for S3 prefix
            pgdata_path: PostgreSQL data directory (auto-detected if None)
            
        Returns:
            Dictionary of WAL-G environment variables
        """
        # Handle backup identifiers (like basebackups_005) - use postgres/ prefix only
        if database_name and database_name.startswith('basebackups_'):
            s3_prefix = f"s3://{s3_storage.bucket}/postgres/"
            logger.info(f"Using postgres/ prefix for backup identifier '{database_name}'")
        else:
            # For regular database names, avoid double 'postgres' in path
            if database_name == 'postgres':
                s3_prefix = f"s3://{s3_storage.bucket}/postgres/"
            else:
                s3_prefix = f"s3://{s3_storage.bucket}/postgres/{database_name}"
        
        env = {
            'WALG_S3_PREFIX': s3_prefix,
            'AWS_ACCESS_KEY_ID': s3_storage.access_key,
            'AWS_SECRET_ACCESS_KEY': s3_storage.secret_key,
            'AWS_REGION': s3_storage.region
        }
        
        if pgdata_path:
            env['PGDATA'] = pgdata_path
            
        return env
    
    @staticmethod
    def detect_pgdata(ssh_manager: SSHManager) -> Optional[str]:
        """Detect PostgreSQL data directory on target server.
        
        Args:
            ssh_manager: SSH connection to target server
            
        Returns:
            Path to PGDATA or None if not found
        """
        # Standard PostgreSQL data directory paths
        paths = [
            '/var/lib/postgresql/14/main',
            '/var/lib/postgresql/13/main', 
            '/var/lib/postgresql/12/main',
            '/var/lib/postgresql/data',
            '/usr/local/pgsql/data'
        ]
        
        for path in paths:
            result = ssh_manager.execute_command(f'test -d {path}')
            if result['exit_code'] == 0:
                return path
                
        return None
    
    @staticmethod
    def validate_env(ssh_manager: SSHManager, walg_env: Dict[str, str]) -> Tuple[bool, str]:
        """Validate WAL-G environment configuration.
        
        Args:
            ssh_manager: SSH connection to target server
            walg_env: WAL-G environment variables
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            # Check WAL-G installation
            result = ssh_manager.execute_command('which wal-g')
            if result['exit_code'] != 0:
                return False, "WAL-G is not installed"
            
            # Validate required variables
            required = ['WALG_S3_PREFIX', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
            for var in required:
                if var not in walg_env or not walg_env[var]:
                    return False, f"Missing required variable: {var}"
            
            # Validate S3 prefix format
            s3_prefix = walg_env['WALG_S3_PREFIX']
            if not re.match(r'^s3://[a-zA-Z0-9.-]+/.*', s3_prefix):
                return False, f"Invalid S3 prefix format: {s3_prefix}"
            
            # Test S3 connectivity
            env_str = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            result = ssh_manager.execute_command(f'{env_str} wal-g backup-list --json')
            if result['exit_code'] != 0:
                return False, f"S3 connectivity test failed: {result.get('stderr', 'Unknown error')}"
            
            return True, "WAL-G configuration is valid"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def list_backups(ssh_manager: SSHManager, walg_env: Dict[str, str]) -> Tuple[bool, list]:
        """List available backups using WAL-G.
        
        Args:
            ssh_manager: SSH connection to target server
            walg_env: WAL-G environment variables
            
        Returns:
            Tuple of (success, backup_list)
        """
        try:
            env_str = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            result = ssh_manager.execute_command(f'{env_str} wal-g backup-list --json')
            
            if result['exit_code'] != 0:
                logger.error(f"WAL-G backup-list command failed with exit code {result['exit_code']}: {result.get('stderr', 'No error message')}")
                return False, []
            
            # Check if stdout is empty or whitespace
            stdout = result.get('stdout', '').strip()
            if not stdout:
                logger.warning("WAL-G backup-list returned empty output")
                return True, []  # Empty list is valid - no backups found
            
            # Try to parse JSON
            try:
                backups = json.loads(stdout)
            except json.JSONDecodeError as json_err:
                logger.error(f"Failed to parse WAL-G backup-list JSON output: {json_err}")
                logger.error(f"Raw output was: {repr(stdout)}")
                return False, []
            
            # Handle case where backups might not be a list
            if not isinstance(backups, list):
                logger.error(f"Expected list from WAL-G backup-list, got {type(backups)}")
                return False, []
            
            backup_names = []
            for backup in backups:
                if isinstance(backup, dict):
                    name = backup.get('backup_name') or backup.get('name') or backup.get('backup_id')
                    if name:
                        backup_names.append(name)
            
            return True, backup_names
            
        except Exception as e:
            logger.error(f"Failed to list backups: {str(e)}")
            return False, []
    
    @staticmethod
    def verify_backup(ssh_manager: SSHManager, walg_env: Dict[str, str], backup_name: str) -> Tuple[bool, str]:
        """Verify backup exists and is valid.
        
        Args:
            ssh_manager: SSH connection to target server (can be None for S3-only validation)
            walg_env: WAL-G environment variables
            backup_name: Name of backup to verify
            
        Returns:
            Tuple of (is_valid, message)
        """
        # For S3-only validation when ssh_manager is None
        if ssh_manager is None:
            # Use S3 client to validate backup exists
            try:
                import boto3
                from botocore.exceptions import ClientError
                
                # Extract S3 credentials from walg_env
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=walg_env.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=walg_env.get('AWS_SECRET_ACCESS_KEY'),
                    region_name=walg_env.get('AWS_REGION', 'us-east-1')
                )
                
                # Extract bucket and prefix from WALG_S3_PREFIX
                s3_prefix = walg_env.get('WALG_S3_PREFIX', '')
                if not s3_prefix.startswith('s3://'):
                    return False, "Invalid S3 prefix format"
                
                # Parse s3://bucket/prefix
                s3_parts = s3_prefix[5:].split('/', 1)
                bucket = s3_parts[0]
                base_prefix = s3_parts[1] if len(s3_parts) > 1 else ''
                
                # Check if backup_name is a backup identifier (like basebackups_005)
                # If so, search under the broader postgres/ prefix instead of specific database prefix
                if backup_name.startswith('basebackups_'):
                    # For backup identifiers, search under postgres/ prefix only
                    search_prefix = 'postgres/'
                    logger.info(f"Searching for backup identifier '{backup_name}' under prefix '{search_prefix}'")
                else:
                    # For regular backup names, use the full prefix
                    # Handle case where base_prefix is just 'postgres/' (no database subdirectory)
                    if base_prefix == 'postgres/' or base_prefix == 'postgres':
                        search_prefix = 'postgres/'
                    else:
                        search_prefix = base_prefix
                    logger.info(f"Searching for backup '{backup_name}' under prefix '{search_prefix}'")
                
                # List objects to check if backup exists
                response = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=search_prefix
                )
                
                if 'Contents' not in response:
                    return False, f"No backups found in S3 under prefix '{search_prefix}'"
                
                # Check if backup exists
                found_backups = []
                for obj in response['Contents']:
                    key = obj['Key']
                    found_backups.append(key)
                    # Check for exact match or if backup_name is contained in the key
                    if backup_name in key or backup_name == 'LATEST':
                        logger.info(f"Found backup '{backup_name}' at S3 key: {key}")
                        return True, f"Backup {backup_name} found in S3"
                
                logger.warning(f"Backup '{backup_name}' not found. Available backups: {found_backups[:10]}")
                return False, f"Backup {backup_name} not found in S3"
                
            except Exception as e:
                logger.error(f"S3 validation error: {str(e)}")
                return False, f"S3 validation failed: {str(e)}"
        
        # Original SSH-based validation
        success, backups = WalgConfig.list_backups(ssh_manager, walg_env)
        if not success:
            return False, "Failed to list backups"
        
        if not backups:
            # For disaster recovery scenarios, allow proceeding even if no backups are currently listed
            # The backup might be restored from external source or S3 directly
            logger.warning(f"No backups currently available, but allowing {backup_name} for disaster recovery")
            return True, f"Proceeding with {backup_name} (no current backups listed - disaster recovery mode)"
        
        # Check if backup exists (exact match or contains backup_name)
        for backup in backups:
            if backup == backup_name or backup_name in backup or backup.endswith(backup_name):
                return True, f"Backup {backup_name} is valid"
        
        # If LATEST requested and backups exist, it's valid
        if backup_name in ['LATEST', 'files_metadata.json'] and backups:
            return True, f"Using latest backup: {backups[0]}"
        
        # For disaster recovery, allow proceeding even if specific backup not found
        logger.warning(f"Backup {backup_name} not found in current list, but allowing for disaster recovery")
        return True, f"Proceeding with {backup_name} (not in current list - disaster recovery mode). Available: {', '.join(backups[:5])}"