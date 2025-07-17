"""Backup management for PostgreSQL using pgBackRest."""

import os
import logging
import tempfile
import base64
import secrets
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from .constants import PostgresConstants
from .system_utils import SystemUtils
from .config_manager import PostgresConfigManager
from .logrotate_config import LogRotateManager

class PostgresBackupManager:
    """Manages PostgreSQL backups using pgBackRest."""
    
    def __init__(self, ssh_manager, system_utils: SystemUtils, 
                 config_manager: PostgresConfigManager, logger=None):
        self.ssh = ssh_manager
        self.system_utils = system_utils
        self.config_manager = config_manager
        self.logger = logger or logging.getLogger(__name__)
        self.logrotate_manager = LogRotateManager(ssh_manager, logger)
    
    def get_or_create_cipher_passphrase(self, backup_job=None) -> str:
        """Get existing cipher passphrase from backup job or create a new one if none exists.
        
        Args:
            backup_job: BackupJob instance containing encryption key
            
        Returns:
            str: Base64-encoded secure passphrase
        """
        # If backup_job is provided and has an encryption key, use it
        if backup_job and backup_job.encryption_key:
            self.logger.info("Using encryption key from database")
            return backup_job.encryption_key
        
        # Fallback: try to read existing passphrase from config file
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        result = self.ssh.execute_command(f"sudo grep -E '^repo1-cipher-pass=' {config_file} 2>/dev/null || true")
        if result['exit_code'] == 0 and result['stdout'].strip():
            # Extract existing passphrase
            line = result['stdout'].strip()
            if '=' in line:
                existing_passphrase = line.split('=', 1)[1]
                self.logger.info("Using existing cipher passphrase from config file")
                # Save to backup_job if provided
                if backup_job:
                    backup_job.encryption_key = existing_passphrase
                    from app.models.database import db
                    db.session.commit()
                    self.logger.info("Saved existing encryption key to backup job database")
                return existing_passphrase
        
        # Generate new passphrase if none exists
        self.logger.info("Generating new cipher passphrase")
        random_bytes = secrets.token_bytes(32)
        passphrase = base64.b64encode(random_bytes).decode('utf-8')
        
        # Save to backup_job if provided
        if backup_job:
            backup_job.encryption_key = passphrase
            from app.models.database import db
            db.session.commit()
            self.logger.info("Saved new encryption key to backup job database")
        
        return passphrase
    
    def is_pgbackrest_installed(self) -> bool:
        """Check if pgBackRest is installed.
        
        Returns:
            bool: True if pgBackRest is installed
        """
        result = self.ssh.execute_command("which pgbackrest")
        return result['exit_code'] == 0
    
    def install_pgbackrest(self) -> Tuple[bool, str]:
        """Install pgBackRest.
        
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Installing pgBackRest...")
        
        if self.is_pgbackrest_installed():
            return True, "pgBackRest is already installed"
        
        # Get package manager commands
        pkg_commands = self.system_utils.get_package_manager_commands()
        if not pkg_commands:
            return False, "Unsupported operating system for automatic installation"
        
        # pgBackRest has a standard package name across distributions
        pgbackrest_pkg = 'pgbackrest'
        
        # Update package list
        if 'update' in pkg_commands:
            self.logger.info("Updating package list...")
            update_result = self.ssh.execute_command(f"sudo {pkg_commands['update']}")
            if update_result['exit_code'] != 0:
                self.logger.warning(f"Package update failed: {update_result.get('stderr', 'Unknown error')}")
        
        # Install pgBackRest
        install_cmd = f"sudo {pkg_commands['install']} {pgbackrest_pkg}"
        result = self.ssh.execute_command(install_cmd)
        
        if result['exit_code'] == 0:
            self.logger.info("pgBackRest installed successfully")
            return True, "pgBackRest installed successfully"
        else:
            return False, f"Failed to install pgBackRest: {result.get('stderr', 'Unknown error')}"
    
    def setup_pgbackrest_directories(self) -> Tuple[bool, str]:
        """Create necessary pgBackRest directories.
        
        Returns:
            tuple: (success, message)
        """
        directories = [
            (PostgresConstants.PGBACKREST['config_dir'], 'postgres:postgres', '755'),
            (PostgresConstants.PGBACKREST['log_dir'], 'postgres:postgres', '755'),
            (PostgresConstants.PGBACKREST['backup_dir'], 'postgres:postgres', '755')
        ]
        
        for dir_path, owner, permissions in directories:
            success, message = self.system_utils.create_directory(dir_path, owner, permissions)
            if not success:
                return False, f"Failed to create directory {dir_path}: {message}"
        
        return True, "pgBackRest directories created successfully"
    
    def create_pgbackrest_config(self, s3_config: Optional[Dict] = None, backup_job=None) -> Tuple[bool, str]:
        """Create pgBackRest configuration following official recommendations.
        
        Args:
            s3_config: S3 configuration dictionary
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Creating pgBackRest configuration...")
        
        # Ensure directories exist
        success, message = self.setup_pgbackrest_directories()
        if not success:
            return False, message
        
        config_content = "[global]\n"
        config_content += f"log-path={PostgresConstants.PGBACKREST['log_dir']}\n"
        
        # Process configuration - let pgBackRest auto-detect optimal values
        if PostgresConstants.PGBACKREST['default_process_max'] != 'auto':
            config_content += f"process-max={PostgresConstants.PGBACKREST['default_process_max']}\n"
        
        # Compression settings - using recommended defaults
        config_content += f"compress-type={PostgresConstants.PGBACKREST['default_compress_type']}\n"
        config_content += f"compress-level={PostgresConstants.PGBACKREST['default_compress_level']}\n"
        
        # Log level settings
        config_content += f"log-level-console={PostgresConstants.PGBACKREST['log_level_console']}\n"
        config_content += f"log-level-file={PostgresConstants.PGBACKREST['log_level_file']}\n"
        # Start fast for quicker backups - recommended by pgBackRest
        config_content += "start-fast=y\n"
        
        # Get or create secure cipher passphrase
        cipher_passphrase = self.get_or_create_cipher_passphrase(backup_job)
        
        # Add S3 configuration if provided, otherwise use posix
        if s3_config:
            config_content += "\n# S3 Configuration\n"
            config_content += "repo1-type=s3\n"
            config_content += f"repo1-s3-bucket={s3_config.get('bucket', '')}\n"
            
            # Handle S3 region and endpoint
            region = s3_config.get('region', 'us-east-1')
            config_content += f"repo1-s3-region={region}\n"
            
            # Set endpoint - if not provided, use AWS S3 endpoint for the region
            endpoint = s3_config.get('endpoint', '')
            if not endpoint:
                endpoint = f"s3.{region}.amazonaws.com"
            config_content += f"repo1-s3-endpoint={endpoint}\n"
            
            config_content += f"repo1-s3-key={s3_config.get('access_key', '')}\n"
            config_content += f"repo1-s3-key-secret={s3_config.get('secret_key', '')}\n"
            
            # TLS verification - recommended to enable for security
            config_content += "repo1-s3-verify-tls=y\n"
            
            # S3 specific performance settings
            config_content += "repo1-s3-uri-style=path\n"
            
            # Encryption for S3 - recommended for security
            config_content += f"repo1-cipher-type={PostgresConstants.PGBACKREST['default_cipher_type']}\n"
            config_content += f"repo1-cipher-pass={cipher_passphrase}\n"
            
        else:
            config_content += "\n# Local Configuration\n"
            config_content += "repo1-type=posix\n"
            config_content += f"repo1-path={PostgresConstants.PGBACKREST['backup_dir']}\n"
            
            # Local encryption - recommended for security
            config_content += f"repo1-cipher-type={PostgresConstants.PGBACKREST['default_cipher_type']}\n"
            config_content += f"repo1-cipher-pass={cipher_passphrase}\n"
        
        # Retention settings - following pgBackRest recommendations
        config_content += "\n# Retention Policy - Recommended Values\n"
        config_content += f"repo1-retention-full={PostgresConstants.PGBACKREST['default_retention_full']}\n"
        config_content += f"repo1-retention-diff={PostgresConstants.PGBACKREST['default_retention_diff']}\n"
        
        # Write configuration file
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        # Create temporary file
        temp_file = os.path.join(tempfile.gettempdir(), 'pgbackrest.conf')
        with open(temp_file, 'w') as f:
            f.write(config_content)
        
        # Upload and move to final location
        remote_temp_file = '/tmp/pgbackrest.conf'
        upload_result = self.ssh.upload_file(temp_file, remote_temp_file)
        if not upload_result:
            return False, "Failed to upload pgBackRest configuration"
        
        move_result = self.ssh.execute_command(f"sudo cp {remote_temp_file} {config_file}")
        if move_result['exit_code'] != 0:
            return False, f"Failed to create pgBackRest config: {move_result.get('stderr', 'Unknown error')}"
        
        # Set permissions
        self.ssh.execute_command(f"sudo chown postgres:postgres {config_file}")
        self.ssh.execute_command(f"sudo chmod 640 {config_file}")
        
        # Clean up local temp file
        try:
            os.remove(temp_file)
        except OSError:
            pass  # Ignore if file doesn't exist
        
        # Clean up remote temp file
        self.ssh.execute_command(f"rm -f {remote_temp_file}")
        
        # Set up log rotation for pgBackRest logs
        log_success, log_message = self.setup_log_rotation()
        if not log_success:
            self.logger.warning(f"Log rotation setup failed: {log_message}")
        
        return True, "pgBackRest configuration created successfully"
    
    def create_stanza_config(self, db_name: str, data_directory: str) -> Tuple[bool, str]:
        """Create stanza configuration for a database.
        
        Args:
            db_name: Database name (stanza name)
            data_directory: PostgreSQL data directory
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating stanza configuration for {db_name}...")
        
        stanza_config = f"\n[{db_name}]\n"
        stanza_config += f"pg1-path={data_directory}\n"
        stanza_config += "pg1-port=5432\n"
        
        # Append to main config file
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        # Create temporary file with stanza config
        temp_file = os.path.join(tempfile.gettempdir(), 'stanza_config.conf')
        with open(temp_file, 'w') as f:
            f.write(stanza_config)
        
        # Upload and append
        remote_temp_file = '/tmp/stanza_config.conf'
        upload_result = self.ssh.upload_file(temp_file, remote_temp_file)
        if not upload_result:
            return False, "Failed to upload stanza configuration"
        
        append_result = self.ssh.execute_command(f"sudo tee -a {config_file} < {remote_temp_file}")
        if append_result['exit_code'] != 0:
            return False, f"Failed to append stanza config: {append_result.get('stderr', 'Unknown error')}"
        
        # Clean up local temp file
        try:
            os.remove(temp_file)
        except OSError:
            pass  # Ignore if file doesn't exist
        
        # Clean up remote temp file
        self.ssh.execute_command(f"rm -f {remote_temp_file}")
        
        return True, f"Stanza configuration for {db_name} created successfully"
    
    def cleanup_corrupted_stanza_files(self, db_name: str) -> Tuple[bool, str]:
        """Clean up corrupted pgBackRest stanza files following official recommendations.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Performing thorough cleanup of stanza files for: {db_name}")
        
        try:
            # Stop any running pgBackRest processes for this stanza
            self.ssh.execute_command(f"sudo pkill -f 'pgbackrest.*{db_name}' || true")
            
            # Use pgBackRest's built-in stop command to properly halt operations
            config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
            self.system_utils.execute_as_postgres_user(f"pgbackrest --config={config_file} --stanza={db_name} stop || true")
            
            # Remove all stanza-related files completely for fresh start
            stanza_paths = [
                f"/var/lib/pgbackrest/archive/{db_name}",
                f"/var/lib/pgbackrest/backup/{db_name}",
                f"{PostgresConstants.PGBACKREST['backup_dir']}/archive/{db_name}",
                f"{PostgresConstants.PGBACKREST['backup_dir']}/backup/{db_name}"
            ]
            
            self.logger.info(f"Removing all stanza files for complete cleanup: {db_name}")
            
            for path in stanza_paths:
                result = self.ssh.execute_command(f"sudo rm -rf {path}")
                if result['exit_code'] != 0:
                    self.logger.warning(f"Failed to remove {path}: {result.get('stderr', 'Unknown error')}")
                else:
                    self.logger.info(f"Successfully removed: {path}")
            
            # Also remove any lock files that might prevent stanza creation
            lock_paths = [
                f"/tmp/pgbackrest/{db_name}.lock",
                f"/var/lib/pgbackrest/{db_name}.lock",
                f"/run/pgbackrest/{db_name}.lock"
            ]
            
            for lock_path in lock_paths:
                self.ssh.execute_command(f"sudo rm -f {lock_path}")
            
            # Recreate directories with proper permissions
            recreate_commands = [
                "sudo mkdir -p /var/lib/pgbackrest/archive",
                "sudo mkdir -p /var/lib/pgbackrest/backup",
                "sudo chown -R postgres:postgres /var/lib/pgbackrest",
                "sudo chmod -R 750 /var/lib/pgbackrest"
            ]
            
            for cmd in recreate_commands:
                result = self.ssh.execute_command(cmd)
                if result['exit_code'] != 0:
                    return False, f"Failed to recreate directories: {result.get('stderr', 'Unknown error')}"
            
            # Start pgBackRest operations again
            self.system_utils.execute_as_postgres_user(f"pgbackrest --config={config_file} --stanza={db_name} start || true")
            
            return True, f"Successfully cleaned up corrupted files for stanza {db_name}"
            
        except Exception as e:
            return False, f"Error during cleanup: {str(e)}"
    
    def _clear_stop_files(self, db_name: str) -> Tuple[bool, str]:
        """Clear pgBackRest stop files using official commands.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            tuple: (success, message)
        """
        try:
            config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
            
            # Use pgBackRest's built-in start command to clear stop files
            start_cmd = f"pgbackrest --config={config_file} --stanza={db_name} start"
            result = self.system_utils.execute_as_postgres_user(start_cmd)
            
            if result['exit_code'] == 0:
                self.logger.info(f"Successfully cleared stop files for stanza {db_name}")
                return True, f"Stop files cleared for stanza {db_name}"
            else:
                # If stanza-specific start fails, try global start
                global_start_cmd = f"pgbackrest --config={config_file} start"
                global_result = self.system_utils.execute_as_postgres_user(global_start_cmd)
                
                if global_result['exit_code'] == 0:
                    self.logger.info("Successfully cleared stop files globally")
                    return True, "Stop files cleared globally"
                else:
                    self.logger.warning(f"Failed to clear stop files: {result.get('stderr', 'Unknown error')}")
                    return False, f"Failed to clear stop files: {result.get('stderr', 'Unknown error')}"
                
        except Exception as e:
            return False, f"Error clearing stop files: {str(e)}"
    
    def setup_pgbackrest_directories(self) -> Tuple[bool, str]:
        """Ensure pgBackRest directories exist with proper permissions.
        
        Returns:
            tuple: (success, message)
        """
        try:
            self.logger.info("Setting up pgBackRest directories...")
            
            # Create required directories
            setup_commands = [
                "sudo mkdir -p /var/lib/pgbackrest/archive",
                "sudo mkdir -p /var/lib/pgbackrest/backup",
                "sudo mkdir -p /var/lib/pgbackrest/stop",
                "sudo mkdir -p /var/log/pgbackrest",
                "sudo chown -R postgres:postgres /var/lib/pgbackrest",
                "sudo chown -R postgres:postgres /var/log/pgbackrest",
                "sudo chmod -R 750 /var/lib/pgbackrest",
                "sudo chmod -R 750 /var/log/pgbackrest"
            ]
            
            for cmd in setup_commands:
                result = self.ssh.execute_command(cmd)
                if result['exit_code'] != 0:
                    return False, f"Failed to execute: {cmd}. Error: {result.get('stderr', 'Unknown error')}"
            
            return True, "pgBackRest directories setup completed successfully"
            
        except Exception as e:
            return False, f"Error during directory setup: {str(e)}"
    
    def create_stanza(self, db_name: str) -> Tuple[bool, str]:
        """Create a pgBackRest stanza following official best practices.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating pgBackRest stanza: {db_name}")
        
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        # Pre-flight checks
        self.setup_pgbackrest_directories()
        self._clear_stop_files(db_name)
        
        # Verify PostgreSQL accessibility and start if needed
        pg_check = self.system_utils.execute_as_postgres_user("psql -c 'SELECT 1;' -t")
        if pg_check['exit_code'] != 0:
            self.logger.info("PostgreSQL is not accessible, checking service status...")
            
            # Check if PostgreSQL service is running
            is_running, status = self.system_utils.check_service_status('postgresql')
            if not is_running:
                self.logger.info("PostgreSQL service is not running, attempting to start...")
                start_success, start_message = self.system_utils.start_service('postgresql')
                if not start_success:
                    return False, f"PostgreSQL is not running and failed to start: {start_message}"
                
                # Wait a moment for PostgreSQL to fully start
                import time
                time.sleep(5)
                
                # Verify accessibility again after starting
                pg_recheck = self.system_utils.execute_as_postgres_user("psql -c 'SELECT 1;' -t")
                if pg_recheck['exit_code'] != 0:
                    return False, f"PostgreSQL started but still not accessible: {pg_recheck.get('stderr', 'Unknown error')}"
                
                self.logger.info("PostgreSQL service started successfully")
            else:
                return False, f"PostgreSQL service is running but not accessible: {pg_check.get('stderr', 'Unknown error')}"
        
        # Create stanza
        result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} stanza-create"
        )
        
        if result['exit_code'] == 0:
            return True, f"Stanza {db_name} created successfully"
        
        error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip()
        
        # Handle specific error cases
        if "already exists" in error_msg.lower():
            # Verify existing stanza
            check_success, _ = self.check_stanza(db_name)
            return (True, f"Stanza {db_name} already exists and is valid") if check_success else (False, f"Stanza {db_name} exists but is invalid")
        
        if "stop file exists" in error_msg.lower():
            # Clear stop files and retry once
            self._clear_stop_files(db_name)
            time.sleep(1)
            retry_result = self.system_utils.execute_as_postgres_user(
                f"pgbackrest --config={config_file} --stanza={db_name} stanza-create"
            )
            if retry_result['exit_code'] == 0:
                return True, f"Stanza {db_name} created successfully after clearing stop files"
            error_msg = retry_result.get('stderr', '').strip() or retry_result.get('stdout', '').strip()
        
        if ("FormatError" in error_msg or "corrupted" in error_msg.lower() or "CryptoError" in error_msg or 
            "unable to flush" in error_msg or "do not match the database" in error_msg or 
            "stanza-upgrade" in error_msg.lower()):
            
            # Handle database ID mismatch with stanza-upgrade
            if "do not match the database" in error_msg:
                self.logger.info(f"Database ID mismatch detected for {db_name}, attempting stanza-upgrade...")
                upgrade_result = self.system_utils.execute_as_postgres_user(
                    f"pgbackrest --config={config_file} --stanza={db_name} stanza-upgrade"
                )
                if upgrade_result['exit_code'] == 0:
                    self.logger.info(f"Stanza upgrade successful for {db_name}")
                    return True, f"Stanza {db_name} upgraded successfully to match database"
                else:
                    self.logger.warning(f"Stanza upgrade failed for {db_name}, proceeding with cleanup...")
            
            # Clean up corrupted/encrypted files and retry once
            self.logger.info(f"Detected corrupted, encrypted, or mismatched files for {db_name}, cleaning up...")
            cleanup_success, _ = self.cleanup_corrupted_stanza_files(db_name)
            if cleanup_success:
                time.sleep(2)  # Give more time for cleanup
                retry_result = self.system_utils.execute_as_postgres_user(
                    f"pgbackrest --config={config_file} --stanza={db_name} stanza-create"
                )
                if retry_result['exit_code'] == 0:
                    return True, f"Stanza {db_name} created successfully after cleanup"
                error_msg = retry_result.get('stderr', '').strip() or retry_result.get('stdout', '').strip()
        
        return False, f"Failed to create stanza {db_name}: {error_msg}"
    
    def check_stanza(self, db_name: str) -> Tuple[bool, str]:
        """Check a pgBackRest stanza.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            tuple: (success, message)
        """
        # Specify the config file path explicitly
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} check"
        )
        
        if result['exit_code'] == 0:
            return True, f"Stanza {db_name} check passed"
        else:
            return False, f"Stanza {db_name} check failed: {result.get('stderr', 'Unknown error')}"
    
    def configure_postgresql_archiving(self, db_name: str) -> Tuple[bool, str]:
        """Configure PostgreSQL for archiving with pgBackRest following official recommendations.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Configuring PostgreSQL for archiving...")
        
        # Get the config file path
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        # Update PostgreSQL settings following pgBackRest recommendations
        settings = {
            'wal_level': 'replica',
            'archive_mode': 'on',
            'archive_command': f"'pgbackrest --config={config_file} --stanza={db_name} archive-push %p'",
            'max_wal_senders': '3',
            # Archive timeout - recommended 60s for regular archiving
            'archive_timeout': '60',
            # Checkpoint settings for better backup performance
            'checkpoint_completion_target': '0.9',
            # WAL settings for better performance
            'wal_buffers': '16MB',
            'wal_writer_delay': '200ms'
        }
        
        for setting, value in settings.items():
            success, message = self.config_manager.update_postgresql_setting(setting, value)
            if not success:
                return False, f"Failed to update {setting}: {message}"
        
        return True, "PostgreSQL archiving configured successfully"
    
    def perform_backup(self, db_name: str, backup_type: str = 'incr') -> Tuple[bool, str]:
        """Perform a backup following pgBackRest best practices.
        
        Args:
            db_name: Database name (stanza name)
            backup_type: 'full', 'incr', or 'diff'
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Performing {backup_type} backup for {db_name}...")
        
        # Auto-promote to full backup if needed
        if backup_type == 'incr' and self._should_force_full_backup(db_name):
            backup_type = 'full'
            self.logger.info("Auto-promoting to full backup per retention policy")
        
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} --type={backup_type} backup"
        )
        
        if result['exit_code'] == 0:
            return True, f"{backup_type.capitalize()} backup completed successfully"
        
        error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
        return False, f"{backup_type.capitalize()} backup failed: {error_msg}"
    
    def _should_force_full_backup(self, db_name: str) -> bool:
        """Check if a full backup should be forced based on pgBackRest recommended policy.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            bool: True if full backup should be forced
        """
        try:
            backups = self.list_backups(db_name)
            if not backups:
                return True  # No backups exist, force full
            
            # Count recent backups using recommended policy (check last 7 backups)
            recent_backups = backups[-7:]
            full_backup_count = sum(1 for backup in recent_backups 
                                  if backup.get('type') == 'full')
            
            # Force full backup if no full backups in recent history
            return full_backup_count == 0
            
        except Exception as e:
            self.logger.warning(f"Could not determine backup policy, forcing full backup: {str(e)}")
            return True
    
    def list_backups(self, db_name: str) -> List[Dict]:
        """List available backups for a database.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            list: List of backup information dictionaries
        """
        # Specify the config file path explicitly
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} info --output=json"
        )
        
        backups = []
        if result['exit_code'] == 0 and result['stdout'].strip():
            try:
                import json
                info_data = json.loads(result['stdout'])
                
                for stanza in info_data:
                    if stanza.get('name') == db_name:
                        for backup in stanza.get('backup', []):
                            backups.append({
                                'name': backup.get('label', ''),
                                'type': backup.get('type', ''),
                                'timestamp': backup.get('timestamp', {}).get('stop', ''),
                                'size': backup.get('info', {}).get('size', 0),
                                'info': backup
                            })
                        break
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.error(f"Failed to parse backup info: {str(e)}")
        
        return backups
    
    def restore_database(self, db_name: str, backup_label: str = None) -> Tuple[bool, str]:
        """Restore a database from backup.
        
        Args:
            db_name: Database name (stanza name)
            backup_label: Specific backup to restore (latest if None)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Restoring database {db_name}...")
        
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
        
        # Specify the config file path explicitly
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        # Build restore command
        restore_cmd = f"pgbackrest --config={config_file} --stanza={db_name} --delta restore"
        if backup_label:
            restore_cmd += f" --set={backup_label}"
        
        # Perform restore
        result = self.system_utils.execute_as_postgres_user(restore_cmd)
        
        if result['exit_code'] != 0:
            return False, f"Restore failed: {result.get('stderr', 'Unknown error')}"
        
        # Start PostgreSQL
        success, message = self.system_utils.start_service('postgresql')
        if not success:
            return False, f"Restore completed but failed to start PostgreSQL: {message}"
        
        return True, f"Database {db_name} restored successfully"
    
    def cleanup_old_backups(self, db_name: str, retention_count: int = None) -> Tuple[bool, str]:
        """Apply retention policy using pgBackRest's built-in expire command.
        
        Args:
            db_name: Database name (stanza name)
            retention_count: Number of backups to keep (uses config if None)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Applying retention policy for stanza {db_name}...")
        
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        # Use pgBackRest's built-in expire command which respects retention settings in config
        result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} expire"
        )
        
        if result['exit_code'] == 0:
            return True, "Retention policy applied successfully"
        else:
            error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
            return False, f"Failed to apply retention policy: {error_msg}"
    
    def health_check(self, db_name: str) -> Tuple[bool, str, Dict]:
        """Perform a comprehensive health check of the backup system.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            tuple: (success, message, health_info)
        """
        health_info = {
            'pgbackrest_installed': False,
            'stanza_exists': False,
            'archiving_configured': False,
            'recent_backup_exists': False,
            'postgresql_running': False
        }
        
        issues = []
        
        # Check if pgBackRest is installed
        health_info['pgbackrest_installed'] = self.is_pgbackrest_installed()
        if not health_info['pgbackrest_installed']:
            issues.append("pgBackRest is not installed")
        
        # Check if PostgreSQL is running
        is_running, _ = self.system_utils.check_service_status('postgresql')
        health_info['postgresql_running'] = is_running
        if not is_running:
            issues.append("PostgreSQL is not running")
        
        # Check stanza
        if health_info['pgbackrest_installed']:
            success, _ = self.check_stanza(db_name)
            health_info['stanza_exists'] = success
            if not success:
                issues.append(f"Stanza {db_name} does not exist or is not configured properly")
        
        # Check archiving configuration
        if health_info['postgresql_running']:
            archive_mode = self.config_manager.get_postgresql_setting('archive_mode')
            archive_command = self.config_manager.get_postgresql_setting('archive_command')
            
            health_info['archiving_configured'] = (
                archive_mode == 'on' and 
                archive_command and 
                'pgbackrest' in archive_command
            )
            
            if not health_info['archiving_configured']:
                issues.append("PostgreSQL archiving is not properly configured")
        
        # Check for recent backups
        if health_info['stanza_exists']:
            backups = self.list_backups(db_name)
            health_info['recent_backup_exists'] = len(backups) > 0
            if not health_info['recent_backup_exists']:
                issues.append("No backups found")
        
        success = len(issues) == 0
        message = "Backup system is healthy" if success else f"Issues found: {'; '.join(issues)}"
        
        return success, message, health_info
    
    def setup_log_rotation(self) -> Tuple[bool, str]:
         """Set up log rotation for pgBackRest logs.
         
         Returns:
             tuple: (success, message)
         """
         try:
             # Check and create log directory first
             dir_success, dir_message = self.logrotate_manager.check_log_directory()
             if not dir_success:
                 return False, f"Log directory setup failed: {dir_message}"
             
             # Set up logrotate configuration
             success, message = self.logrotate_manager.setup_pgbackrest_logrotate()
             if success:
                 self.logger.info("pgBackRest log rotation configured successfully")
             else:
                 self.logger.error(f"Failed to configure pgBackRest log rotation: {message}")
             
             return success, message
             
         except Exception as e:
             error_msg = f"Error setting up log rotation: {str(e)}"
             self.logger.error(error_msg)
             return False, error_msg