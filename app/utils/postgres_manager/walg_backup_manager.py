"""Backup management for PostgreSQL using WAL-G."""

import os
import logging
import tempfile
import json
from typing import Dict, List, Optional, Tuple
from .constants import PostgresConstants
from .system_utils import SystemUtils
from .config_manager import PostgresConfigManager

class WalgBackupManager:
    """Manages PostgreSQL backups using WAL-G."""
    
    def __init__(self, ssh_manager, system_utils: SystemUtils, 
                 config_manager: PostgresConfigManager, logger=None):
        self.ssh = ssh_manager
        self.system_utils = system_utils
        self.config_manager = config_manager
        self.logger = logger or logging.getLogger(__name__)
    
    def is_walg_installed(self) -> bool:
        """Check if WAL-G is installed.
        
        Returns:
            bool: True if WAL-G is installed
        """
        result = self.ssh.execute_command(f"test -f {PostgresConstants.WALG['binary_path']} && echo 'exists'")
        return result['exit_code'] == 0 and 'exists' in result.get('stdout', '')
    
    def install_walg(self) -> Tuple[bool, str]:
        """Install WAL-G binary.
        
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Installing WAL-G...")
        
        if self.is_walg_installed():
            return True, "WAL-G is already installed"
        
        # Get package manager commands for dependencies
        pkg_commands = self.system_utils.get_package_manager_commands()
        if not pkg_commands:
            return False, "Unsupported operating system for automatic installation"
        
        # Install dependencies
        dependencies = PostgresConstants.PACKAGE_NAMES.get('debian', {}).get('dependencies', [])
        if dependencies:
            deps_str = ' '.join(dependencies)
            install_cmd = f"sudo {pkg_commands['install']} {deps_str}"
            result = self.ssh.execute_command(install_cmd)
            if result['exit_code'] != 0:
                return False, f"Failed to install dependencies: {result.get('stderr', 'Unknown error')}"
        
        # Download and install WAL-G binary
        download_url = PostgresConstants.WALG['download_url']
        install_commands = [
            "cd /tmp",
            f"sudo wget -O wal-g.tar.gz {download_url}",
            "sudo tar -xzf wal-g.tar.gz",
            f"sudo mv wal-g-pg-ubuntu-20.04-amd64 {PostgresConstants.WALG['binary_path']}",
            f"sudo chmod +x {PostgresConstants.WALG['binary_path']}",
            "sudo rm -f wal-g.tar.gz"
        ]
        
        for cmd in install_commands:
            result = self.ssh.execute_command(cmd)
            if result['exit_code'] != 0:
                return False, f"Failed to install WAL-G: {result.get('stderr', 'Unknown error')}"
        
        # Verify installation
        if self.is_walg_installed():
            self.logger.info("WAL-G installed successfully")
            return True, "WAL-G installed successfully"
        else:
            return False, "WAL-G installation verification failed"
    
    def setup_walg_directories(self) -> Tuple[bool, str]:
        """Set up WAL-G directories with proper permissions.
        
        Returns:
            tuple: (success, message)
        """
        directories = [
            PostgresConstants.WALG['config_dir'],
            PostgresConstants.WALG['log_dir']
        ]
        
        for directory in directories:
            # Create directory
            result = self.ssh.execute_command(f"sudo mkdir -p {directory}")
            if result['exit_code'] != 0:
                return False, f"Failed to create directory {directory}: {result.get('stderr', 'Unknown error')}"
            
            # Set ownership to postgres
            result = self.ssh.execute_command(f"sudo chown postgres:postgres {directory}")
            if result['exit_code'] != 0:
                return False, f"Failed to set ownership for {directory}: {result.get('stderr', 'Unknown error')}"
            
            # Set permissions
            result = self.ssh.execute_command(f"sudo chmod 750 {directory}")
            if result['exit_code'] != 0:
                return False, f"Failed to set permissions for {directory}: {result.get('stderr', 'Unknown error')}"
        
        return True, "WAL-G directories created successfully"
    
    def create_walg_config(self, s3_config: Optional[Dict] = None, backup_job=None) -> Tuple[bool, str]:
        """Create WAL-G environment configuration.
        
        Args:
            s3_config: S3 configuration dictionary
            backup_job: Backup job for encryption settings
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Creating WAL-G configuration...")
        
        # Ensure directories exist
        success, message = self.setup_walg_directories()
        if not success:
            return False, message
        
        if not s3_config:
            return False, "S3 configuration is required for WAL-G"
        
        # Create environment file content with sanitized values
        def sanitize_value(value, default=''):
            """Remove whitespace and line breaks from configuration values."""
            return str(value or default).strip().replace('\r', '').replace('\n', '')
        
        bucket_name = sanitize_value(s3_config.get('bucket'))
        access_key = sanitize_value(s3_config.get('access_key'))
        secret_key = sanitize_value(s3_config.get('secret_key'))
        region = sanitize_value(s3_config.get('region'))  # No default region
        
        # Validate required fields (no defaults allowed)
        if not bucket_name:
            return False, "S3 bucket name is required"
        if not access_key:
            return False, "AWS access key is required"
        if not secret_key:
            return False, "AWS secret key is required"
        if not region:
            return False, "AWS region is required"
        
        # Build environment configuration
        env_vars = [
            "# WAL-G Environment Configuration",
            f"export WALE_S3_PREFIX=s3://{bucket_name}/postgres/{backup_job.database.name if backup_job and backup_job.database else 'default'}",
            f"export AWS_ACCESS_KEY_ID={access_key}",
            f"export AWS_SECRET_ACCESS_KEY={secret_key}",
            f"export AWS_REGION={region}"
        ]
        
        # Add optional endpoint for S3-compatible storage
        endpoint = sanitize_value(s3_config.get('endpoint'))
        if endpoint:
            env_vars.append(f"export AWS_ENDPOINT={endpoint}")
        
        # Add WAL-G specific settings only if explicitly configured (no defaults)
        walg_settings = PostgresConstants.WALG_S3_ENV
        
        # Only add settings that are explicitly configured
        if walg_settings.get('WALG_COMPRESSION_METHOD'):
            env_vars.append(f"export WALG_COMPRESSION_METHOD={walg_settings['WALG_COMPRESSION_METHOD']}")
        
        if walg_settings.get('WALG_DELTA_MAX_STEPS'):
            env_vars.append(f"export WALG_DELTA_MAX_STEPS={walg_settings['WALG_DELTA_MAX_STEPS']}")
        
        if walg_settings.get('WALG_TAR_SIZE_THRESHOLD'):
            env_vars.append(f"export WALG_TAR_SIZE_THRESHOLD={walg_settings['WALG_TAR_SIZE_THRESHOLD']}")
        
        env_content = '\n'.join(env_vars) + '\n'
        
        # Deploy environment file efficiently
        env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
        remote_temp_file = '/tmp/walg.env'
        
        try:
            # Create temporary file with Unix line endings
            temp_file = os.path.join(tempfile.gettempdir(), 'walg.env')
            with open(temp_file, 'w', newline='\n') as f:
                f.write(env_content)
            
            # Upload configuration file
            if not self.ssh.upload_file(temp_file, remote_temp_file):
                return False, "Failed to upload WAL-G configuration"
            
            # Deploy with proper permissions in one operation
            deploy_cmd = f"sudo cp {remote_temp_file} {env_file} && sudo chown postgres:postgres {env_file} && sudo chmod 640 {env_file}"
            deploy_result = self.ssh.execute_command(deploy_cmd)
            if deploy_result['exit_code'] != 0:
                return False, f"Failed to deploy WAL-G config: {deploy_result.get('stderr', 'Unknown error')}"
            
        finally:
            # Clean up temporary files
            try:
                os.remove(temp_file)
            except (OSError, NameError):
                pass
            self.ssh.execute_command(f"rm -f {remote_temp_file}")
        
        return True, "WAL-G configuration created successfully"
    
    def configure_postgresql_archiving(self, db_name: str) -> Tuple[bool, str]:
        """Configure PostgreSQL for archiving with WAL-G.
        
        Args:
            db_name: Database name
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Configuring PostgreSQL for WAL-G archiving...")
        
        # Define optimized PostgreSQL settings for WAL-G
        walg_settings = {
            'wal_level': 'replica',
            'archive_mode': 'on', 
            'archive_command': "'wal-g wal-push %p'",
            'restore_command': "'wal-g wal-fetch %f %p'",
            'max_wal_senders': '3',
            'archive_timeout': '60'
        }
        
        # Update all settings efficiently
        failed_settings = []
        for setting, value in walg_settings.items():
            success, message = self.config_manager.update_postgresql_setting(setting, value)
            if not success:
                failed_settings.append(f"{setting}: {message}")
        
        if failed_settings:
            return False, f"Failed to update settings: {'; '.join(failed_settings)}"
        
        # Restart PostgreSQL to apply archive_mode changes
        self.logger.info("Restarting PostgreSQL to apply archiving configuration...")
        success, message = self.system_utils.restart_service('postgresql')
        if not success:
            return False, f"Configuration updated but PostgreSQL restart failed: {message}"
        
        return True, "PostgreSQL archiving configured successfully for WAL-G"
    
    def create_backup(self, backup_name: str) -> Tuple[bool, str]:
        """Create a backup with a specific name (WAL-G uses timestamps).
        
        Args:
            backup_name: Backup name (used for logging, WAL-G uses timestamps)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating backup '{backup_name}' using WAL-G...")
        
        # Get data directory
        data_dir = self.config_manager.get_data_directory()
        if not data_dir:
            return False, "Could not determine PostgreSQL data directory"
        
        # Source WAL-G environment
        env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
        
        # Perform backup using bash to handle 'source' command
        backup_cmd = f"bash -c 'source {env_file} && wal-g backup-push {data_dir}'"
        
        result = self.system_utils.execute_as_postgres_user(backup_cmd)
        
        if result['exit_code'] == 0:
            return True, f"Backup '{backup_name}' completed successfully"
        
        error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
        return False, f"Backup '{backup_name}' failed: {error_msg}"
    
    def restore_backup(self, backup_name: str) -> Tuple[bool, str]:
        """Restore from a specific backup.
        
        Args:
            backup_name: Backup name to restore from
            
        Returns:
            tuple: (success, message)
        """
        return self.restore_database('', backup_name)
    
    def delete_backup(self, backup_name: str) -> Tuple[bool, str]:
        """Delete a specific backup (WAL-G uses retention policies).
        
        Args:
            backup_name: Backup name to delete
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Deleting backup '{backup_name}' using WAL-G...")
        
        env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
        
        # WAL-G delete specific backup using bash to handle 'source' command
        result = self.system_utils.execute_as_postgres_user(
            f"bash -c 'source {env_file} && wal-g delete target {backup_name}'"
        )
        
        if result['exit_code'] == 0:
            return True, f"Backup '{backup_name}' deleted successfully"
        else:
            error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
            return False, f"Failed to delete backup '{backup_name}': {error_msg}"
    
    def perform_backup(self, db_name: str) -> Tuple[bool, str]:
        """Perform a backup using WAL-G.
        
        Args:
            db_name: Database name
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Performing incremental backup for {db_name} using WAL-G...")
        
        # Get data directory
        data_dir = self.config_manager.get_data_directory()
        if not data_dir:
            return False, "Could not determine PostgreSQL data directory"
        
        # Source WAL-G environment
        env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
        
        # WAL-G automatically determines if delta backup is possible
        backup_cmd = f"bash -c 'source {env_file} && wal-g backup-push {data_dir}'"
        
        result = self.system_utils.execute_as_postgres_user(backup_cmd)
        
        if result['exit_code'] == 0:
            return True, "Incremental backup completed successfully"
        
        error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
        return False, f"Incremental backup failed: {error_msg}"
    
    def list_backups(self, db_name: str) -> List[Dict]:
        """List available backups using WAL-G.
        
        Args:
            db_name: Database name
            
        Returns:
            list: List of backup information dictionaries
        """
        env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
        
        command = f"bash -c 'source {env_file} && wal-g backup-list --json --detail'"
        self.logger.info(f"[WALGBackupManager] Executing WAL-G command: {command}")
        result = self.system_utils.execute_as_postgres_user(command)
        
        self.logger.info(f"[WALGBackupManager] WAL-G command exit code: {result['exit_code']}")
        if result['stdout']:
            self.logger.info(f"[WALGBackupManager] WAL-G stdout: {result['stdout'][:1000]}...")  # First 1000 chars for debugging
        if result['stderr']:
            self.logger.warning(f"[WALGBackupManager] WAL-G stderr: {result['stderr']}")
        
        backups = []
        if result['exit_code'] == 0 and result['stdout'].strip():
            try:
                self.logger.info("[WALGBackupManager] Parsing JSON output...")
                backup_data = json.loads(result['stdout'])
                self.logger.info(f"[WALGBackupManager] Parsed {len(backup_data)} backups from WAL-G")
                
                for i, backup in enumerate(backup_data):
                    self.logger.info(f"[WALGBackupManager] Processing backup {i+1}: {backup.get('backup_name', 'unknown')}")
                    # Map WAL-G backup fields according to official documentation
                    # WAL-G --json --detail provides: backup_name, time, uncompressed_size, compressed_size, start_time, finish_time, etc.
                    backup_info = {
                        'name': backup.get('backup_name', ''),
                        'type': 'full' if backup.get('is_permanent', False) else 'delta',
                        'timestamp': backup.get('time', ''),
                        'size': backup.get('uncompressed_size', 0),  # Primary size field
                        'compressed_size': backup.get('compressed_size', 0),  # Additional size info
                        'is_permanent': backup.get('is_permanent', False),
                        'wal_file_name': backup.get('wal_file_name', ''),
                        'start_time': backup.get('start_time', ''),
                        'finish_time': backup.get('finish_time', ''),
                        'hostname': backup.get('hostname', ''),
                        'data_dir': backup.get('data_dir', ''),
                        'pg_version': backup.get('pg_version', ''),
                        'start_lsn': backup.get('start_lsn', ''),
                        'finish_lsn': backup.get('finish_lsn', ''),
                        'info': backup  # Keep full backup info for debugging
                    }
                    backups.append(backup_info)
                    self.logger.info(f"[WALGBackupManager] Added backup info: {backup_info['name']} at {backup_info['timestamp']}")
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.error(f"[WALGBackupManager] Failed to parse backup info: {str(e)}")
        else:
            self.logger.warning("[WALGBackupManager] WAL-G command failed or returned empty output")
        
        self.logger.info(f"[WALGBackupManager] Returning {len(backups)} backups")
        return backups
    
    def restore_database(self, db_name: str, backup_name: str = None) -> Tuple[bool, str]:
        """Restore a database from backup using WAL-G.
        
        Args:
            db_name: Database name
            backup_name: Specific backup to restore (LATEST if None)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Restoring database {db_name} using WAL-G...")
        
        # Stop PostgreSQL
        success, message = self.system_utils.stop_service('postgresql')
        if not success:
            return False, f"Failed to stop PostgreSQL: {message}"
        
        # Get data directory
        data_dir = self.config_manager.get_data_directory()
        if not data_dir:
            return False, "Could not determine PostgreSQL data directory"
        
        # Remove postmaster.pid if it exists
        self.ssh.execute_command(f"sudo rm -f {data_dir}/postmaster.pid")
        
        # Source WAL-G environment
        env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
        
        # Build restore command using bash to handle 'source' command
        backup_target = backup_name or 'LATEST'
        restore_cmd = f"bash -c 'source {env_file} && wal-g backup-fetch {data_dir} {backup_target}'"
        
        # Perform restore
        self.logger.info(f"Executing restore command: {restore_cmd}")
        result = self.system_utils.execute_as_postgres_user(restore_cmd)
        
        if result['exit_code'] != 0:
            error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
            self.logger.error(f"Restore command failed with exit code {result['exit_code']}: {error_msg}")
            return False, f"Restore failed: {error_msg}"
        
        # Start PostgreSQL (WAL replay will happen automatically)
        success, message = self.system_utils.start_service('postgresql')
        if not success:
            return False, f"Restore completed but failed to start PostgreSQL: {message}"
        
        return True, f"Database {db_name} restored successfully"
    
    def cleanup_old_backups(self, db_name: str, retention_count: int = None) -> Tuple[bool, str]:
        """Clean up old backups using WAL-G retention policy.
        
        Args:
            db_name: Database name
            retention_count: Number of backups to keep (required)
            
        Returns:
            tuple: (success, message)
        """
        if not retention_count:
            return False, "Retention count must be explicitly specified"
        
        retention = retention_count
        
        env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
        
        result = self.system_utils.execute_as_postgres_user(
            f"bash -c 'source {env_file} && wal-g delete retain {retention}'"
        )
        
        if result['exit_code'] == 0:
            return True, f"Cleaned up old backups, retained {retention} backups"
        else:
            error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
            return False, f"Cleanup failed: {error_msg}"
    
    def health_check(self, db_name: str) -> Tuple[bool, str, Dict]:
        """Perform a comprehensive health check of the WAL-G backup system.
        
        Args:
            db_name: Database name
            
        Returns:
            tuple: (success, message, health_info)
        """
        health_info = {
            'walg_installed': False,
            'walg_configured': False,
            'archiving_configured': False,
            'recent_backup_exists': False,
            'postgresql_running': False
        }
        
        issues = []
        
        # Check if WAL-G is installed
        health_info['walg_installed'] = self.is_walg_installed()
        if not health_info['walg_installed']:
            issues.append("WAL-G is not installed")
        
        # Check if PostgreSQL is running
        is_running, _ = self.system_utils.check_service_status('postgresql')
        health_info['postgresql_running'] = is_running
        if not is_running:
            issues.append("PostgreSQL is not running")
        
        # Check WAL-G configuration
        if health_info['walg_installed']:
            env_file = os.path.join(PostgresConstants.WALG['config_dir'], 'walg.env')
            config_check = self.ssh.execute_command(f"test -f {env_file} && echo 'exists'")
            health_info['walg_configured'] = config_check['exit_code'] == 0 and 'exists' in config_check.get('stdout', '')
            if not health_info['walg_configured']:
                issues.append("WAL-G configuration file not found")
        
        # Check archiving configuration
        if health_info['postgresql_running']:
            archive_mode = self.config_manager.get_postgresql_setting('archive_mode')
            archive_command = self.config_manager.get_postgresql_setting('archive_command')
            
            health_info['archiving_configured'] = (
                archive_mode == 'on' and 
                archive_command and 
                'wal-g' in archive_command
            )
            
            if not health_info['archiving_configured']:
                issues.append("PostgreSQL archiving is not properly configured for WAL-G")
        
        # Check for recent backups
        if health_info['walg_configured']:
            backups = self.list_backups(db_name)
            health_info['recent_backup_exists'] = len(backups) > 0
            if not health_info['recent_backup_exists']:
                issues.append("No backups found")
        
        success = len(issues) == 0
        message = "WAL-G backup system is healthy" if success else f"Issues found: {'; '.join(issues)}"
        
        return success, message, health_info
    
    def setup_log_rotation(self) -> Tuple[bool, str]:
        """Set up log rotation for WAL-G logs.
        
        Returns:
            tuple: (success, message)
        """
        # WAL-G logs to stdout/stderr, captured by systemd or cron
        # Basic log rotation setup for WAL-G log directory
        log_dir = PostgresConstants.WALG['log_dir']
        
        logrotate_content = f"""{log_dir}/*.log {{
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    copytruncate
    su postgres postgres
}}
"""
        
        # Create logrotate configuration
        temp_file = os.path.join(tempfile.gettempdir(), 'walg-logrotate')
        with open(temp_file, 'w') as f:
            f.write(logrotate_content)
        
        # Upload and install logrotate config
        remote_temp_file = '/tmp/walg-logrotate'
        upload_result = self.ssh.upload_file(temp_file, remote_temp_file)
        if not upload_result:
            return False, "Failed to upload logrotate configuration"
        
        install_result = self.ssh.execute_command(f"sudo cp {remote_temp_file} /etc/logrotate.d/walg")
        if install_result['exit_code'] != 0:
            return False, f"Failed to install logrotate config: {install_result.get('stderr', 'Unknown error')}"
        
        # Clean up temp files
        try:
            os.remove(temp_file)
        except OSError:
            pass
        self.ssh.execute_command(f"rm -f {remote_temp_file}")
        
        return True, "WAL-G log rotation configured successfully"