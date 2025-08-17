"""Simplified PostgreSQL management using secure best practices."""

import logging
import secrets
import string
from typing import Any, Dict, List, Optional, Tuple
from .walg_backup_manager import WalgBackupManager
from .config_manager import PostgresConfigManager
from .system_utils import SystemUtils
from .error_handler import PostgresErrorHandler
from .user_manager import PostgresUserManager


class PostgresManager:
    """Simplified PostgreSQL management using secure best practices."""
    
    def __init__(self, ssh_manager, logger=None):
        self.ssh = ssh_manager
        self.logger = logger or logging.getLogger(__name__)
        self.error_handler = PostgresErrorHandler(self.logger)
        
        # Initialize components
        self.system_utils = SystemUtils(ssh_manager, logger)
        self.config_manager = PostgresConfigManager(ssh_manager, self.system_utils, logger)
        self.user_manager = PostgresUserManager(ssh_manager, self.system_utils, logger)
        self.backup_manager = WalgBackupManager(ssh_manager, self.system_utils, self.config_manager, logger)
        
        # Cache for expensive operations
        self._postgres_version = None
        self._postgres_installed = None
    
    # ===== INSTALLATION & STATUS =====
    
    def is_installed(self) -> bool:
        """Check if PostgreSQL is installed."""
        if self._postgres_installed is not None:
            return self._postgres_installed
            
        result = self.ssh.execute_command("which psql")
        self._postgres_installed = result['exit_code'] == 0
        return self._postgres_installed
    
    def get_version(self) -> Optional[str]:
        """Get PostgreSQL version."""
        if self._postgres_version:
            return self._postgres_version
            
        result = self.system_utils.execute_postgres_sql("SELECT version()")
        if result['exit_code'] == 0 and result['stdout']:
            version_line = result['stdout'].strip()
            # Extract version number from "PostgreSQL 15.4 on ..."
            if 'PostgreSQL' in version_line:
                parts = version_line.split()
                for i, part in enumerate(parts):
                    if part == 'PostgreSQL' and i + 1 < len(parts):
                        version = parts[i + 1]
                        self._postgres_version = version
                        return version
        return None
    
    def get_postgres_version(self) -> Optional[str]:
        """Get PostgreSQL version (alias for get_version for backward compatibility)."""
        return self.get_version()
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive PostgreSQL status."""
        status = {
            'installed': self.is_installed(),
            'version': self.get_version(),
            'service_status': self.system_utils.check_postgresql_service()
        }
        
        if status['installed']:
            # Get basic settings
            settings_sql = """SELECT name, setting FROM pg_settings 
                             WHERE name IN ('max_connections', 'shared_buffers', 'port')"""
            result = self.system_utils.execute_postgres_sql(settings_sql)
            if result['exit_code'] == 0:
                status['settings'] = {}
                for line in result['stdout'].strip().split('\n'):
                    if '|' in line:
                        name, value = line.split('|', 1)
                        status['settings'][name.strip()] = value.strip()
        
        return status
    
    # ===== DATABASE OPERATIONS =====
    
    def create_database(self, database_name: str) -> Tuple[bool, str]:
        """Create a database."""
        self.logger.info(f"Creating database '{database_name}'")
        
        # Check if database exists
        if self._check_database_exists(database_name):
            return False, f"Database '{database_name}' already exists"
        
        # Use quoted identifier to prevent SQL injection
        quoted_name = self._quote_identifier(database_name)
        sql = f"CREATE DATABASE {quoted_name}"
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            self.logger.info(f"Database '{database_name}' created successfully")
            return True, f"Database '{database_name}' created successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            self.logger.error(f"Failed to create database '{database_name}': {error_msg}")
            return False, f"Failed to create database: {error_msg}"
    
    def delete_database(self, database_name: str) -> Tuple[bool, str]:
        """Delete a database."""
        self.logger.info(f"Deleting database '{database_name}'")
        
        if not self._check_database_exists(database_name):
            return False, f"Database '{database_name}' does not exist"
        
        # Terminate active connections first
        terminate_sql = f"""SELECT pg_terminate_backend(pid) 
                           FROM pg_stat_activity 
                           WHERE datname = '{database_name}' AND pid != pg_backend_pid()"""
        self.system_utils.execute_postgres_sql(terminate_sql)
        
        # Use quoted identifier to prevent SQL injection
        quoted_name = self._quote_identifier(database_name)
        sql = f"DROP DATABASE {quoted_name}"
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            self.logger.info(f"Database '{database_name}' deleted successfully")
            return True, f"Database '{database_name}' deleted successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            self.logger.error(f"Failed to delete database '{database_name}': {error_msg}")
            return False, f"Failed to delete database: {error_msg}"
    
    def list_databases(self) -> List[Dict[str, str]]:
        """List all databases."""
        sql = """SELECT datname as name, pg_get_userbyid(datdba) as owner,
                        pg_encoding_to_char(encoding) as encoding
                 FROM pg_database WHERE datistemplate = false"""
        result = self.system_utils.execute_postgres_sql(sql)
        
        databases = []
        if result['exit_code'] == 0 and result['stdout']:
            for line in result['stdout'].strip().split('\n'):
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 3:
                        databases.append({
                            'name': parts[0].strip(),
                            'owner': parts[1].strip(),
                            'encoding': parts[2].strip()
                        })
        return databases
    
    # ===== HELPER METHODS =====
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote PostgreSQL identifier to prevent SQL injection."""
        # Replace any double quotes with double-double quotes and wrap in quotes
        return f'"{identifier.replace('"', '""')}"'
    
    def _check_database_exists(self, database_name: str) -> bool:
        """Check if a database exists."""
        sql = f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"
        result = self.system_utils.execute_postgres_sql(sql)
        # Check if query succeeded and returned the value '1' (indicating database exists)
        return result['exit_code'] == 0 and result['stdout'] and '1' in result['stdout'] and '(1 row)' in result['stdout']
    
    def _check_user_exists(self, username: str) -> bool:
        """Check if a user exists."""
        sql = f"SELECT 1 FROM pg_roles WHERE rolname = '{username}'"
        result = self.system_utils.execute_postgres_sql(sql)
        # Check if query succeeded and returned the value '1' (indicating user exists)
        return result['exit_code'] == 0 and result['stdout'] and '1' in result['stdout'] and '(1 row)' in result['stdout']
    
    # ===== USER MANAGEMENT =====
    
    def create_user(self, username: str, password: str, permissions: str = 'login') -> Tuple[bool, str]:
        """Create a PostgreSQL user."""
        self.logger.info(f"Creating user '{username}' with permissions '{permissions}'")
        
        if self._check_user_exists(username):
            return False, f"User '{username}' already exists"
        
        # Build CREATE ROLE command with permissions
        quoted_username = self._quote_identifier(username)
        role_options = []
        
        if 'login' in permissions.lower():
            role_options.append('LOGIN')
        if 'createdb' in permissions.lower():
            role_options.append('CREATEDB')
        if 'superuser' in permissions.lower():
            role_options.append('SUPERUSER')
        
        options_str = ' '.join(role_options) if role_options else 'LOGIN'
        sql = f"CREATE ROLE {quoted_username} WITH {options_str} PASSWORD '{password}'"
        
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            self.logger.info(f"User '{username}' created successfully")
            return True, f"User '{username}' created successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            self.logger.error(f"Failed to create user '{username}': {error_msg}")
            return False, f"Failed to create user: {error_msg}"
    
    def update_user_password(self, username: str, password: str) -> Tuple[bool, str]:
        """Update user password."""
        self.logger.info(f"Updating password for user '{username}'")
        
        if not self._check_user_exists(username):
            return False, f"User '{username}' does not exist"
        
        quoted_username = self._quote_identifier(username)
        sql = f"ALTER ROLE {quoted_username} WITH PASSWORD '{password}'"
        
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            self.logger.info(f"Password updated for user '{username}'")
            return True, f"Password updated for user '{username}'"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            self.logger.error(f"Failed to update password for user '{username}': {error_msg}")
            return False, f"Failed to update password: {error_msg}"
    
    def delete_user(self, username: str) -> Tuple[bool, str]:
        """Delete a PostgreSQL user."""
        self.logger.info(f"Deleting user '{username}'")
        
        if not self._check_user_exists(username):
            return False, f"User '{username}' does not exist"
        
        quoted_username = self._quote_identifier(username)
        sql = f"DROP ROLE {quoted_username}"
        
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            self.logger.info(f"User '{username}' deleted successfully")
            return True, f"User '{username}' deleted successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            self.logger.error(f"Failed to delete user '{username}': {error_msg}")
            return False, f"Failed to delete user: {error_msg}"
    
    def grant_permissions(self, username: str, database: str, permission_level: str) -> Tuple[bool, str]:
        """Grant database permissions to user."""
        self.logger.info(f"Granting '{permission_level}' permissions to user '{username}' on database '{database}'")
        
        if not self._check_user_exists(username):
            return False, f"User '{username}' does not exist"
        
        if not self._check_database_exists(database):
            return False, f"Database '{database}' does not exist"
        
        quoted_username = self._quote_identifier(username)
        quoted_database = self._quote_identifier(database)
        
        if permission_level.lower() == 'read_only':
            sql = f"GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_username}; GRANT USAGE ON SCHEMA public TO {quoted_username}; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {quoted_username}"
        elif permission_level.lower() == 'read_write':
            sql = f"GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_username}; GRANT USAGE, CREATE ON SCHEMA public TO {quoted_username}; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {quoted_username}"
        elif permission_level.lower() == 'all':
            sql = f"GRANT ALL PRIVILEGES ON DATABASE {quoted_database} TO {quoted_username}"
        else:
            return False, f"Invalid permission level: {permission_level}"
        
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            return True, "Permissions granted successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            return False, f"Failed to grant permissions: {error_msg}"
    
    def revoke_permissions(self, username: str, database: str) -> Tuple[bool, str]:
        """Revoke database permissions from user."""
        self.logger.info(f"Revoking permissions from user '{username}' on database '{database}'")
        
        quoted_username = self._quote_identifier(username)
        quoted_database = self._quote_identifier(database)
        
        sql = f"REVOKE ALL PRIVILEGES ON DATABASE {quoted_database} FROM {quoted_username}"
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            return True, "Permissions revoked successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            return False, f"Failed to revoke permissions: {error_msg}"
    
    def get_user_permissions(self, username: str, database: str) -> str:
        """Get user permissions for a database."""
        sql = f"""SELECT has_database_privilege('{username}', '{database}', 'CONNECT') as can_connect,
                        has_database_privilege('{username}', '{database}', 'CREATE') as can_create"""
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0 and result['stdout']:
            line = result['stdout'].strip()
            if '|' in line:
                parts = line.split('|')
                if len(parts) >= 2:
                    can_connect = parts[0].strip().lower() == 't'
                    can_create = parts[1].strip().lower() == 't'
                    
                    if can_create:
                        return 'all'
                    elif can_connect:
                        return 'read_write'
                    else:
                        return 'no_access'
        
        return 'no_access'
    
    def list_users(self) -> List[Dict[str, str]]:
        """List all PostgreSQL users."""
        sql = """SELECT rolname as username, 
                        CASE WHEN rolsuper THEN 'superuser'
                             WHEN rolcreatedb THEN 'createdb'
                             WHEN rolcanlogin THEN 'login'
                             ELSE 'no_login' END as permissions
                 FROM pg_roles WHERE rolname NOT LIKE 'pg_%'"""
        result = self.system_utils.execute_postgres_sql(sql)
        
        users = []
        if result['exit_code'] == 0 and result['stdout']:
            for line in result['stdout'].strip().split('\n'):
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 2:
                        users.append({
                            'username': parts[0].strip(),
                            'permissions': parts[1].strip()
                        })
        return users
    
    def list_database_users(self, db_name: str) -> List[Dict[str, str]]:
        """List all PostgreSQL users with access to a specific database.
        
        Args:
            db_name: Database name to check users for
            
        Returns:
            list: List of users with their roles/permissions
        """
        return self.user_manager.list_database_users(db_name)
    
    def grant_individual_permissions(self, username: str, db_name: str, permissions: dict) -> Tuple[bool, str]:
        """Grant individual permissions to a user on a database.
        
        Args:
            username: Username to grant permissions to
            db_name: Database name
            permissions: Dict with keys: connect, select, insert, update, delete, create
        
        Returns:
            Tuple of (success, message)
        """
        return self.user_manager.grant_individual_permissions(username, db_name, permissions)
    
    def get_user_individual_permissions(self, username: str, db_name: str) -> Dict[str, bool]:
        """Get detailed individual permissions for a user on a specific database.
        
        Args:
            username: Username to check
            db_name: Database name
            
        Returns:
            dict: Dictionary with individual permission flags
        """
        return self.user_manager.get_user_individual_permissions(username, db_name)
    
    def generate_password(self, length: int = 16) -> str:
        """Generate a secure password."""
        alphabet = string.ascii_letters + string.digits + '!@#$%^&*'
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    def create_database_user(self, username: str, password: str, db_name: str, permission_level: str = 'all') -> Tuple[bool, str]:
        """Create a user and grant permissions to a specific database."""
        # Delegate to the user_manager which handles existing users properly
        return self.user_manager.create_database_user(username, password, db_name, permission_level)
    
    def delete_database_user(self, username: str) -> Tuple[bool, str]:
        """Delete a database user."""
        # Delegate to the user_manager
        return self.user_manager.delete_user(username)
    
    # ===== CONFIGURATION =====
    
    def enable_external_connections(self, allowed_ips: List[str] = None) -> Tuple[bool, str]:
        """Enable external connections to PostgreSQL."""
        self.logger.info("Enabling external connections")
        
        # Update listen_addresses
        result = self.update_setting('listen_addresses', "'*'")
        if not result[0]:
            return result
        
        # Configure pg_hba.conf for allowed IPs
        if allowed_ips:
            for ip in allowed_ips:
                # This would require modifying pg_hba.conf file directly
                # For now, just log the requirement
                self.logger.info(f"Would add pg_hba.conf entry for IP: {ip}")
        
        return self.reload_config()
    
    def configure_ssl_tls(self, enable_ssl: bool = True, cert_path: str = None, key_path: str = None, auto_generate: bool = True) -> Tuple[bool, str, Dict]:
        """Configure SSL/TLS for PostgreSQL.
        
        Args:
            enable_ssl: Whether to enable SSL/TLS
            cert_path: Path to SSL certificate file
            key_path: Path to SSL private key file
            auto_generate: Whether to auto-generate self-signed certificates
            
        Returns:
            tuple: (success, message, changes_made)
        """
        return self.config_manager.configure_ssl_tls(enable_ssl, cert_path, key_path, auto_generate)
    
    def update_setting(self, setting_name: str, value: str) -> Tuple[bool, str]:
        """Update a PostgreSQL setting."""
        self.logger.info(f"Updating setting '{setting_name}' to '{value}'")
        
        sql = f"ALTER SYSTEM SET {setting_name} = {value}"
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            self.logger.info(f"Setting '{setting_name}' updated successfully")
            return True, f"Setting '{setting_name}' updated successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            self.logger.error(f"Failed to update setting '{setting_name}': {error_msg}")
            return False, f"Failed to update setting: {error_msg}"
    
    def update_settings(self, settings: Dict[str, str]) -> Tuple[bool, str]:
        """Update multiple PostgreSQL settings."""
        self.logger.info(f"Updating {len(settings)} settings")
        
        for setting_name, value in settings.items():
            result = self.update_setting(setting_name, value)
            if not result[0]:
                return result
        
        return self.reload_config()
    
    def get_setting(self, setting_name: str) -> Optional[str]:
        """Get a PostgreSQL setting value."""
        sql = f"SHOW {setting_name}"
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0 and result['stdout']:
            return result['stdout'].strip()
        return None
    
    def get_settings(self, setting_names: List[str]) -> Dict[str, str]:
        """Get multiple PostgreSQL settings."""
        settings = {}
        for setting_name in setting_names:
            value = self.get_setting(setting_name)
            if value is not None:
                settings[setting_name] = value
        return settings
    
    def configure_for_walg(self) -> Tuple[bool, str]:
        """Configure PostgreSQL for WAL-G."""
        self.logger.info("Configuring PostgreSQL for WAL-G")
        
        # Required settings for WAL-G
        settings = {
            'archive_mode': 'on',
            'archive_command': "'source /etc/wal-g/walg.env && wal-g wal-push %p'",
            'max_wal_senders': '3',
            'wal_level': 'replica'
        }
        
        return self.update_settings(settings)
    
    def reload_config(self) -> Tuple[bool, str]:
        """Reload PostgreSQL configuration."""
        self.logger.info("Reloading PostgreSQL configuration")
        
        sql = "SELECT pg_reload_conf()"
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            self.logger.info("Configuration reloaded successfully")
            return True, "Configuration reloaded successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            self.logger.error(f"Failed to reload configuration: {error_msg}")
            return False, f"Failed to reload configuration: {error_msg}"
    
    def create_secure_hba_config(self, allowed_networks: List[str] = None) -> Tuple[bool, str]:
        """Create secure pg_hba.conf."""
        self.logger.info("Creating secure pg_hba.conf configuration")
        
        # This would require direct file manipulation
        # For now, just return a placeholder implementation
        if allowed_networks:
            self.logger.info(f"Would configure pg_hba.conf for networks: {allowed_networks}")
        
        return True, "Secure pg_hba.conf configuration created (placeholder)"
    
    # ===== CONNECTION MANAGEMENT =====
    
    def configure_connection_pooling(self, max_connections: int = 100, 
                                   superuser_reserved: int = 3) -> Tuple[bool, str]:
        """Configure connection pooling settings."""
        self.logger.info(f"Configuring connection pooling: max_connections={max_connections}, superuser_reserved={superuser_reserved}")
        
        settings = {
            'max_connections': str(max_connections),
            'superuser_reserved_connections': str(superuser_reserved)
        }
        
        return self.update_settings(settings)
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        sql = """SELECT 
                    count(*) as total_connections,
                    count(*) FILTER (WHERE state = 'active') as active_connections,
                    count(*) FILTER (WHERE state = 'idle') as idle_connections,
                    count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
                 FROM pg_stat_activity WHERE pid != pg_backend_pid()"""
        
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0 and result['stdout']:
            line = result['stdout'].strip()
            if '|' in line:
                parts = line.split('|')
                if len(parts) >= 4:
                    return {
                        'total_connections': int(parts[0].strip()),
                        'active_connections': int(parts[1].strip()),
                        'idle_connections': int(parts[2].strip()),
                        'idle_in_transaction': int(parts[3].strip())
                    }
        
        return {'total_connections': 0, 'active_connections': 0, 'idle_connections': 0, 'idle_in_transaction': 0}
    
    def list_active_connections(self) -> List[Dict[str, Any]]:
        """List active connections."""
        sql = """SELECT pid, usename, datname, client_addr, state, 
                        query_start, state_change
                 FROM pg_stat_activity 
                 WHERE pid != pg_backend_pid() AND state != 'idle'"""
        
        result = self.system_utils.execute_postgres_sql(sql)
        connections = []
        
        if result['exit_code'] == 0 and result['stdout']:
            for line in result['stdout'].strip().split('\n'):
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 7:
                        connections.append({
                            'pid': int(parts[0].strip()),
                            'username': parts[1].strip(),
                            'database': parts[2].strip(),
                            'client_addr': parts[3].strip(),
                            'state': parts[4].strip(),
                            'query_start': parts[5].strip(),
                            'state_change': parts[6].strip()
                        })
        
        return connections
    
    def terminate_connection(self, pid: int, force: bool = False) -> Tuple[bool, str]:
        """Terminate a connection."""
        self.logger.info(f"Terminating connection with PID {pid}")
        
        if force:
            sql = f"SELECT pg_terminate_backend({pid})"
        else:
            sql = f"SELECT pg_cancel_backend({pid})"
        
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            action = "terminated" if force else "cancelled"
            return True, f"Connection {pid} {action} successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            return False, f"Failed to terminate connection: {error_msg}"
    
    def cleanup_idle_connections(self) -> Tuple[bool, str]:
        """Clean up idle connections."""
        self.logger.info("Cleaning up idle connections")
        
        sql = """SELECT pg_terminate_backend(pid)
                  FROM pg_stat_activity 
                  WHERE state = 'idle' 
                    AND state_change < now() - interval '30 minutes'
                    AND pid != pg_backend_pid()"""
        
        result = self.system_utils.execute_postgres_sql(sql)
        
        if result['exit_code'] == 0:
            return True, "Idle connections cleaned up successfully"
        else:
            error_msg = result.get('stderr', 'Unknown error')
            return False, f"Failed to clean up idle connections: {error_msg}"
    
    def monitor_connection_health(self) -> Dict[str, Any]:
        """Monitor connection health."""
        stats = self.get_connection_stats()
        max_conn = self.get_setting('max_connections')
        max_connections = int(max_conn) if max_conn else 100
        
        usage_percentage = (stats['total_connections'] / max_connections) * 100
        
        return {
            'connection_usage_percentage': round(usage_percentage, 2),
            'available_connections': max_connections - stats['total_connections'],
            'health_status': 'healthy' if usage_percentage < 80 else 'warning' if usage_percentage < 95 else 'critical',
            'max_connections': max_connections,
            **stats
        }
    
    def optimize_for_workload(self, workload_type: str) -> Tuple[bool, str]:
        """Optimize connections for workload type."""
        self.logger.info(f"Optimizing for workload type: {workload_type}")
        
        if workload_type.lower() == 'oltp':
            settings = {
                'max_connections': '200',
                'shared_buffers': '256MB',
                'effective_cache_size': '1GB'
            }
        elif workload_type.lower() == 'olap':
            settings = {
                'max_connections': '50',
                'shared_buffers': '1GB',
                'effective_cache_size': '4GB',
                'work_mem': '256MB'
            }
        elif workload_type.lower() == 'mixed':
            settings = {
                'max_connections': '100',
                'shared_buffers': '512MB',
                'effective_cache_size': '2GB'
            }
        else:
            return False, f"Unknown workload type: {workload_type}"
        
        return self.update_settings(settings)
    
    # ===== BACKUP OPERATIONS =====
    
    def create_backup(self, backup_name: str) -> Tuple[bool, str]:
        """Create a backup."""
        return self.backup_manager.create_backup(backup_name)
    
    def restore_backup(self, backup_name: str) -> Tuple[bool, str]:
        """Restore from backup."""
        return self.backup_manager.restore_backup(backup_name)
    
    def list_backups(self, db_name: str) -> List[Dict[str, Any]]:
        """List available backups."""
        return self.backup_manager.list_backups(db_name)
    
    def delete_backup(self, backup_name: str) -> Tuple[bool, str]:
        """Delete a backup."""
        return self.backup_manager.delete_backup(backup_name)
    
    def perform_backup(self, db_name: str, backup_type: str = 'delta') -> Tuple[bool, str]:
        """Perform a backup."""
        return self.backup_manager.perform_backup(db_name, backup_type)
    
    def restore_database(self, db_name: str, backup_label: str = None) -> Tuple[bool, str]:
        """Restore a database from backup."""
        return self.backup_manager.restore_database(db_name, backup_label)
    
    def cleanup_old_backups(self, db_name: str, retention_count: int = None) -> Tuple[bool, str]:
        """Clean up old backups using WAL-G retention policy."""
        return self.backup_manager.cleanup_old_backups(db_name, retention_count)
    
    def check_health(self, db_name: str) -> Tuple[bool, str, Dict]:
        """Perform a comprehensive health check of the backup system."""
        return self.backup_manager.health_check(db_name)
    
    def setup_log_rotation(self) -> Tuple[bool, str]:
        """Set up log rotation for WAL-G logs."""
        return self.backup_manager.setup_log_rotation()
    
    def configure_postgresql_archiving(self, db_name: str) -> Tuple[bool, str]:
        """Configure PostgreSQL for archiving with WAL-G."""
        return self.backup_manager.configure_postgresql_archiving(db_name)
    
    # ===== INSTALLATION =====
    
    def install_postgres(self, version: str = None) -> Tuple[bool, str]:
        """Install PostgreSQL."""
        try:
            if self.is_installed():
                return False, "PostgreSQL is already installed"
            
            # Use system utils for installation
            result = self.system_utils.install_postgresql(version)
            
            # Clear cache after installation
            self._postgres_installed = None
            self._postgres_version = None
            
            return result
        except Exception as e:
            self.logger.error(f"Installation failed: {str(e)}")
            return False, f"Installation failed: {str(e)}"
    
    def install_postgres_with_streaming(self, version: str = None, callback=None) -> Tuple[bool, str]:
        """Install PostgreSQL with streaming output.
        
        Args:
            version: PostgreSQL major version (e.g., '15', '16')
            callback: Optional callback function to receive streaming output
        
        Returns:
            tuple: (success, message)
        """
        try:
            if self.is_installed():
                return False, "PostgreSQL is already installed"
            
            if callback:
                callback("Starting PostgreSQL installation...\n")
            
            # Use system utils for installation with streaming
            result = self.system_utils.install_postgresql(version)
            
            if callback:
                if result[0]:
                    callback(f"PostgreSQL installation completed successfully: {result[1]}\n")
                else:
                    callback(f"PostgreSQL installation failed: {result[1]}\n")
            
            # Clear cache after installation
            self._postgres_installed = None
            self._postgres_version = None
            
            return result
        except Exception as e:
            error_msg = f"Installation failed: {str(e)}"
            self.logger.error(error_msg)
            if callback:
                callback(f"Error: {error_msg}\n")
            return False, error_msg
    
    def uninstall_postgres(self) -> Tuple[bool, str]:
        """Uninstall PostgreSQL."""
        try:
            if not self.is_installed():
                return False, "PostgreSQL is not installed"
            
            result = self.system_utils.uninstall_postgresql()
            
            # Clear cache after uninstallation
            self._postgres_installed = None
            self._postgres_version = None
            
            return result
        except Exception as e:
            self.logger.error(f"Uninstallation failed: {str(e)}")
            return False, f"Uninstallation failed: {str(e)}"
    
    # ===== SYSTEM UTILITIES =====
    
    def get_system_info(self) -> Dict[str, str]:
        """Get system information."""
        return self.system_utils.get_system_info()
    
    def check_service_status(self) -> Dict[str, Any]:
        """Check PostgreSQL service status."""
        return self.system_utils.check_postgresql_service()
    
    def start_service(self) -> Tuple[bool, str]:
        """Start PostgreSQL service."""
        return self.system_utils.start_postgresql_service()
    
    def stop_service(self) -> Tuple[bool, str]:
        """Stop PostgreSQL service."""
        return self.system_utils.stop_postgresql_service()
    
    def restart_service(self) -> Tuple[bool, str]:
        """Restart PostgreSQL service."""
        return self.system_utils.restart_postgresql_service()
    
    def restart_postgres(self) -> Tuple[bool, str]:
        """Restart PostgreSQL service (alias for restart_service)."""
        return self.restart_service()
    
    # ===== EXTERNAL CONNECTIONS =====
    
    def check_and_fix_external_connections(self, allowed_ips: List[str] = None, auth_method: str = 'scram-sha-256') -> Tuple[bool, str, Dict]:
        """Check and fix external connections configuration.
        
        Args:
            allowed_ips: List of allowed IP addresses/CIDR blocks. If None, allows all IPs
            auth_method: Authentication method (scram-sha-256, md5, etc.)
        
        Returns:
            tuple: (success, message, changes_made)
        """
        return self.config_manager.configure_external_connections(allowed_ips, auth_method)
    
    def get_postgresql_setting(self, setting_name: str) -> Optional[str]:
        """Get a PostgreSQL setting value from postgresql.conf.
        
        Args:
            setting_name: Name of the setting to retrieve
            
        Returns:
            Setting value or None if not found
        """
        return self.config_manager.get_postgresql_setting(setting_name)
    
    def get_pg_hba_entries(self) -> List[Dict[str, str]]:
        """Get current pg_hba.conf entries.
        
        Returns:
            List of pg_hba entries as dictionaries
        """
        return self.config_manager.get_pg_hba_entries()