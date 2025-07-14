"""Backup management for PostgreSQL using pgBackRest."""

import os
import logging
import tempfile
import base64
import secrets
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
    
    def generate_cipher_passphrase(self) -> str:
        """Generate a secure base64-encoded cipher passphrase as recommended by pgBackRest.
        
        Returns:
            str: Base64-encoded secure passphrase
        """
        # Generate 32 random bytes (256 bits) for strong encryption
        random_bytes = secrets.token_bytes(32)
        # Encode as base64 as recommended by pgBackRest documentation
        return base64.b64encode(random_bytes).decode('utf-8')
    
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
            (PostgresConstants.PGBACKREST['conf_d_dir'], 'postgres:postgres', '755'),
            (PostgresConstants.PGBACKREST['log_dir'], 'postgres:postgres', '755'),
            (PostgresConstants.PGBACKREST['backup_dir'], 'postgres:postgres', '755')
        ]
        
        for dir_path, owner, permissions in directories:
            success, message = self.system_utils.create_directory(dir_path, owner, permissions)
            if not success:
                return False, f"Failed to create directory {dir_path}: {message}"
        
        return True, "pgBackRest directories created successfully"
    
    def create_pgbackrest_config(self, s3_config: Optional[Dict] = None) -> Tuple[bool, str]:
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
        
        # Process configuration - using recommended defaults
        config_content += f"process-max={PostgresConstants.PGBACKREST['default_process_max']}\n"
        
        # Archive timeout - using recommended value
        config_content += f"archive-timeout={PostgresConstants.PGBACKREST['archive_timeout']}\n"
        
        # Compression settings - using recommended defaults
        config_content += f"compress-type={PostgresConstants.PGBACKREST['default_compress_type']}\n"
        config_content += f"compress-level={PostgresConstants.PGBACKREST['default_compress_level']}\n"
        
        # Log level settings
        config_content += f"log-level-console={PostgresConstants.PGBACKREST['log_level_console']}\n"
        config_content += f"log-level-file={PostgresConstants.PGBACKREST['log_level_file']}\n"
        config_content += f"log-level-stderr={PostgresConstants.PGBACKREST['log_level_stderr']}\n"
        
        # Delta optimization for faster restores
        config_content += f"delta={'y' if PostgresConstants.PGBACKREST['delta_enabled'] else 'n'}\n"
        
        # Start fast for quicker backups - recommended by pgBackRest
        config_content += "start-fast=y\n"
        
        # Archive header check for safety - recommended by pgBackRest
        config_content += "archive-header-check=y\n"
        
        # Generate secure cipher passphrase
        cipher_passphrase = self.generate_cipher_passphrase()
        self.logger.info("Generated secure cipher passphrase for encryption")
        
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
        
        # Archive retention - configurable type and value
        config_content += f"repo1-retention-archive-type={PostgresConstants.PGBACKREST['default_retention_archive_type']}\n"
        config_content += f"repo1-retention-archive={PostgresConstants.PGBACKREST['default_retention_archive']}\n"
        
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
    
    def create_stanza(self, db_name: str) -> Tuple[bool, str]:
        """Create a pgBackRest stanza.
        
        Args:
            db_name: Database name (stanza name)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating pgBackRest stanza: {db_name}")
        
        # Specify the config file path explicitly
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        # First, ensure pgBackRest is started (remove any stop files)
        self.logger.info(f"Starting pgBackRest for stanza: {db_name}")
        start_result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} start"
        )
        
        if start_result['exit_code'] != 0:
            self.logger.warning(f"Failed to start pgBackRest for stanza {db_name}: {start_result.get('stderr', 'Unknown error')}")
            # Try to start for all stanzas
            global_start_result = self.system_utils.execute_as_postgres_user(
                f"pgbackrest --config={config_file} start"
            )
            if global_start_result['exit_code'] != 0:
                return False, f"Failed to start pgBackRest: {global_start_result.get('stderr', 'Unknown error')}"
        
        # Now attempt to create the stanza
        result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} stanza-create"
        )
        
        # Log the full result for debugging
        self.logger.info(f"Stanza creation result - Exit code: {result['exit_code']}, Stdout: {result.get('stdout', '')}, Stderr: {result.get('stderr', '')}")
        
        if result['exit_code'] == 0:
            return True, f"Stanza {db_name} created successfully"
        else:
            # Capture both stdout and stderr for better error reporting
            error_msg = result.get('stderr', '').strip()
            stdout_msg = result.get('stdout', '').strip()
            
            # If stderr is empty, use stdout
            if not error_msg and stdout_msg:
                error_msg = stdout_msg
            elif not error_msg and not stdout_msg:
                error_msg = f"Command failed with exit code {result['exit_code']} but no error message was provided"
            
            # Check for common errors and provide helpful messages
            if "stop file exists" in error_msg:
                return False, f"Failed to create stanza {db_name}: pgBackRest stop file exists. Try running 'pgbackrest start' first. Error: {error_msg}"
            elif "unable to find primary cluster" in error_msg:
                return False, f"Failed to create stanza {db_name}: PostgreSQL cluster not found or not running. Ensure PostgreSQL is running and accessible. Error: {error_msg}"
            elif "permission denied" in error_msg.lower():
                return False, f"Failed to create stanza {db_name}: Permission denied. Check file permissions and ownership. Error: {error_msg}"
            elif "already exists" in error_msg.lower():
                # Stanza already exists, check if it's valid
                check_success, check_msg = self.check_stanza(db_name)
                if check_success:
                    return True, f"Stanza {db_name} already exists and is valid"
                else:
                    return False, f"Stanza {db_name} exists but is invalid: {check_msg}"
            else:
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
        """Perform a backup.
        
        Args:
            db_name: Database name (stanza name)
            backup_type: 'full', 'incr', or 'diff'
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Performing {backup_type} backup for {db_name}...")
        
        # Check if we need to force a full backup
        if backup_type == 'incr' and self._should_force_full_backup(db_name):
            backup_type = 'full'
            self.logger.info("Forcing full backup due to policy")
        
        # Specify the config file path explicitly
        config_file = os.path.join(PostgresConstants.PGBACKREST['config_dir'], 'pgbackrest.conf')
        
        result = self.system_utils.execute_as_postgres_user(
            f"pgbackrest --config={config_file} --stanza={db_name} --type={backup_type} backup"
        )
        
        if result['exit_code'] == 0:
            return True, f"{backup_type.capitalize()} backup completed successfully"
        else:
            # Get error details from stderr and stdout
            stderr = result.get('stderr', '').strip()
            stdout = result.get('stdout', '').strip()
            exit_code = result.get('exit_code', 'unknown')
            
            # Build comprehensive error message
            error_parts = []
            if stderr:
                error_parts.append(f"Error: {stderr}")
            if stdout:
                error_parts.append(f"Output: {stdout}")
            error_parts.append(f"Exit code: {exit_code}")
            
            error_message = " | ".join(error_parts) if error_parts else "Unknown error occurred"
            
            # Log the full result for debugging
            self.logger.error(f"Backup command failed. Full result: {result}")
            
            return False, f"{backup_type.capitalize()} backup failed: {error_message}"
    
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
            
            # Count recent backups using recommended policy
            recent_backups = backups[-PostgresConstants.PGBACKREST['max_backups_before_full']:]
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
    
    def cleanup_old_backups(self, db_name: str, retention_count: int) -> Tuple[bool, str]:
        """Apply retention policy to existing backups.
        
        Args:
            db_name: Database name (stanza name)
            retention_count: Number of backups to keep
            
        Returns:
            tuple: (success, message)
        """
        try:
            backups = self.list_backups(db_name)
            
            if not backups or len(backups) <= retention_count:
                return True, f"No cleanup needed. Current backups ({len(backups)}) within retention limit ({retention_count})"
            
            # Sort backups by timestamp (newest first)
            for backup in backups:
                if 'timestamp' in backup:
                    try:
                        backup['datetime'] = datetime.strptime(backup['timestamp'], '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        backup['datetime'] = datetime.now()
                else:
                    backup['datetime'] = datetime.now()
            
            backups.sort(key=lambda x: x['datetime'], reverse=True)
            
            # Keep the newest backups up to the retention limit
            backups_to_delete = backups[retention_count:]
            
            if not backups_to_delete:
                return True, "No backups to delete"
            
            # Check if the 'expire' command is supported
            check_expire = self.ssh.execute_command("sudo -u postgres pgbackrest help | grep expire")
            expire_supported = check_expire['exit_code'] == 0 and 'expire' in check_expire['stdout']
            
            if not expire_supported:
                return True, "Using pgBackRest built-in retention mechanism (expire command not available)"
            
            deleted_count = 0
            for backup in backups_to_delete:
                backup_name = backup['name']
                result = self.system_utils.execute_as_postgres_user(
                    f"pgbackrest --stanza={db_name} expire --set={backup_name}"
                )
                
                if result['exit_code'] == 0:
                    self.logger.info(f"Successfully deleted old backup: {backup_name}")
                    deleted_count += 1
                else:
                    self.logger.error(f"Failed to delete backup {backup_name}: {result.get('stderr', 'Unknown error')}")
            
            return True, f"Successfully deleted {deleted_count} old backups"
            
        except Exception as e:
            self.logger.error(f"Error in cleanup_old_backups: {str(e)}")
            return False, f"Error cleaning up old backups: {str(e)}"
    
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