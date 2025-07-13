"""Core PostgreSQL management functionality."""

import logging
from typing import Dict, List, Optional, Tuple
from .constants import PostgresConstants
from .system_utils import SystemUtils
from .config_manager import PostgresConfigManager
from .backup_manager import PostgresBackupManager
from .user_manager import PostgresUserManager

class PostgresManager:
    """Main PostgreSQL management class using modular components."""
    
    def __init__(self, ssh_manager, logger=None):
        self.ssh = ssh_manager
        self.logger = logger or logging.getLogger(__name__)
        
        # Initialize modular components
        self.system_utils = SystemUtils(ssh_manager, logger)
        self.config_manager = PostgresConfigManager(ssh_manager, self.system_utils, logger)
        self.backup_manager = PostgresBackupManager(ssh_manager, self.system_utils, self.config_manager, logger)
        self.user_manager = PostgresUserManager(ssh_manager, self.system_utils, logger)
        
        # Cache for expensive operations
        self._postgres_version = None
        self._postgres_installed = None
    
    def check_postgres_installed(self) -> bool:
        """Check if PostgreSQL is installed.
        
        Returns:
            bool: True if PostgreSQL is installed
        """
        if self._postgres_installed is not None:
            return self._postgres_installed
            
        result = self.ssh.execute_command("which psql")
        self._postgres_installed = result['exit_code'] == 0
        return self._postgres_installed
    
    def get_postgres_version(self) -> Optional[str]:
        """Get PostgreSQL version.
        
        Returns:
            str: PostgreSQL version or None if not installed
        """
        if self._postgres_version:
            return self._postgres_version
            
        if not self.check_postgres_installed():
            return None
            
        result = self.ssh.execute_command("psql --version")
        if result['exit_code'] == 0 and result['stdout']:
            # Extract version from output like "psql (PostgreSQL) 13.7"
            version_line = result['stdout'].strip()
            if 'PostgreSQL' in version_line:
                parts = version_line.split()
                for part in parts:
                    if part.replace('.', '').isdigit():
                        self._postgres_version = part
                        return part
        
        return None
    
    def get_postgres_major_version(self) -> Optional[str]:
        """Get PostgreSQL major version.
        
        Returns:
            str: Major version (e.g., '13', '14') or None
        """
        version = self.get_postgres_version()
        if version:
            return version.split('.')[0]
        return None
    
    def is_postgres_latest_version(self) -> Tuple[bool, str]:
        """Check if PostgreSQL is the latest available version.
        
        Returns:
            tuple: (is_latest, message)
        """
        current_version = self.get_postgres_version()
        if not current_version:
            return False, "PostgreSQL is not installed"
        
        # Get package manager commands
        pkg_commands = self.system_utils.get_package_manager_commands()
        if not pkg_commands:
            return False, "Cannot check latest version on this OS"
        
        # Get package names
        pkg_names = self.system_utils.get_postgres_package_names()
        postgres_pkg = pkg_names.get('postgresql', 'postgresql')
        
        # Check available version
        if 'search' in pkg_commands:
            result = self.ssh.execute_command(f"{pkg_commands['search']} {postgres_pkg}")
            if result['exit_code'] == 0:
                # This is a simplified check - in practice, you'd parse the output
                # to compare versions properly
                return True, f"Current version: {current_version}"
        
        return True, f"Current version: {current_version} (version check not implemented for this OS)"
    
    def get_data_directory(self) -> Optional[str]:
        """Get PostgreSQL data directory.
        
        Returns:
            str: Data directory path or None
        """
        return self.config_manager.get_data_directory()
    
    def list_databases(self) -> List[str]:
        """List all PostgreSQL databases.
        
        Returns:
            list: List of database names
        """
        if not self.check_postgres_installed():
            return []
        
        result = self.system_utils.execute_postgres_sql(
            "SELECT datname FROM pg_database WHERE datistemplate = false;"
        )
        
        databases = []
        if result['exit_code'] == 0 and result['stdout']:
            for line in result['stdout'].strip().split('\n'):
                db_name = line.strip()
                if db_name and db_name != 'datname':  # Skip header
                    databases.append(db_name)
        
        return databases
    
    def install_postgres(self, version: str = None) -> Tuple[bool, str]:
        """Install PostgreSQL.
        
        Args:
            version: Specific version to install (latest if None)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Installing PostgreSQL {version or 'latest'}...")
        
        if self.check_postgres_installed():
            return True, "PostgreSQL is already installed"
        
        # Get package manager commands
        pkg_commands = self.system_utils.get_package_manager_commands()
        if not pkg_commands:
            return False, "Unsupported operating system for automatic installation"
        
        # Get package names
        pkg_names = self.system_utils.get_postgres_package_names()
        postgres_pkg = pkg_names.get('postgresql', 'postgresql')
        contrib_pkg = pkg_names.get('postgresql_contrib', 'postgresql-contrib')
        
        # Add version suffix if specified, otherwise try without version first
        if version:
            postgres_pkg += f"-{version}"
            contrib_pkg += f"-{version}"
        
        # Update package list
        if 'update' in pkg_commands:
            self.logger.info("Updating package list...")
            update_result = self.ssh.execute_command(f"sudo {pkg_commands['update']}")
            if update_result['exit_code'] != 0:
                self.logger.warning(f"Package update failed: {update_result.get('stderr', 'Unknown error')}")
        
        # Install PostgreSQL
        install_cmd = f"sudo {pkg_commands['install']} {postgres_pkg} {contrib_pkg}"
        result = self.ssh.execute_command(install_cmd)
        
        # If installation with specific version failed, try without version suffix
        if result['exit_code'] != 0 and version:
            self.logger.warning(f"Failed to install PostgreSQL {version}, trying default version...")
            # Reset package names to default (without version)
            postgres_pkg = pkg_names.get('postgresql', 'postgresql')
            contrib_pkg = pkg_names.get('postgresql_contrib', 'postgresql-contrib')
            install_cmd = f"sudo {pkg_commands['install']} {postgres_pkg} {contrib_pkg}"
            result = self.ssh.execute_command(install_cmd)
        
        if result['exit_code'] != 0:
            return False, f"Failed to install PostgreSQL: {result.get('stderr', 'Unknown error')}"
        
        # Initialize database if needed (for RHEL-based systems)
        os_type = self.system_utils.detect_os()
        if os_type == 'rhel':
            major_version = version or self.get_postgres_major_version() or '13'
            init_result = self.ssh.execute_command(f"sudo postgresql-{major_version}-setup initdb")
            if init_result['exit_code'] != 0:
                self.logger.warning(f"Database initialization may have failed: {init_result.get('stderr', 'Unknown error')}")
        
        # Start and enable PostgreSQL service
        success, message = self.system_utils.start_service('postgresql')
        if not success:
            return False, f"PostgreSQL installed but failed to start: {message}"
        
        # Enable service to start on boot
        self.ssh.execute_command("sudo systemctl enable postgresql")
        
        # Clear cache
        self._postgres_installed = None
        self._postgres_version = None
        
        return True, "PostgreSQL installed and started successfully"
    
    def start_postgres(self) -> Tuple[bool, str]:
        """Start PostgreSQL service.
        
        Returns:
            tuple: (success, message)
        """
        return self.system_utils.start_service('postgresql')
    
    def stop_postgres(self) -> Tuple[bool, str]:
        """Stop PostgreSQL service.
        
        Returns:
            tuple: (success, message)
        """
        return self.system_utils.stop_service('postgresql')
    
    def restart_postgres(self) -> Tuple[bool, str]:
        """Restart PostgreSQL service.
        
        Returns:
            tuple: (success, message)
        """
        return self.system_utils.restart_service('postgresql')
    
    def create_database(self, db_name: str) -> Tuple[bool, str]:
        """Create a new PostgreSQL database.
        
        Args:
            db_name: Name of the database to create
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating database: {db_name}")
        
        if not self.check_postgres_installed():
            return False, PostgresConstants.ERROR_MESSAGES['postgres_not_installed']
        
        # Check if database already exists
        existing_dbs = self.list_databases()
        if db_name in existing_dbs:
            return False, f"Database '{db_name}' already exists"
        
        result = self.system_utils.execute_postgres_sql(f"CREATE DATABASE {db_name};")
        
        if result['exit_code'] == 0:
            return True, f"Database '{db_name}' created successfully"
        else:
            return False, f"Failed to create database '{db_name}': {result.get('stderr', 'Unknown error')}"
    
    def delete_database(self, db_name: str) -> Tuple[bool, str]:
        """Delete a PostgreSQL database.
        
        Args:
            db_name: Name of the database to delete
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Deleting database: {db_name}")
        
        if not self.check_postgres_installed():
            return False, PostgresConstants.ERROR_MESSAGES['postgres_not_installed']
        
        # Check if database exists
        existing_dbs = self.list_databases()
        if db_name not in existing_dbs:
            return False, f"Database '{db_name}' does not exist"
        
        result = self.system_utils.execute_postgres_sql(f"DROP DATABASE {db_name};")
        
        if result['exit_code'] == 0:
            return True, f"Database '{db_name}' deleted successfully"
        else:
            return False, f"Failed to delete database '{db_name}': {result.get('stderr', 'Unknown error')}"
    
    def upgrade_postgres(self) -> Tuple[bool, str]:
        """Upgrade PostgreSQL to the latest version.
        
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Upgrading PostgreSQL...")
        
        if not self.check_postgres_installed():
            return False, PostgresConstants.ERROR_MESSAGES['postgres_not_installed']
        
        os_type = self.system_utils.detect_os()
        
        if os_type == 'debian':
            # For Debian/Ubuntu, use pg_upgradecluster
            current_version = self.get_postgres_major_version()
            if not current_version:
                return False, "Could not determine current PostgreSQL version"
            
            # This is a simplified upgrade - in practice, you'd need to handle
            # version detection and cluster management more carefully
            result = self.ssh.execute_command(f"sudo pg_upgradecluster {current_version} main")
            
            if result['exit_code'] == 0:
                return True, "PostgreSQL upgraded successfully"
            else:
                return False, f"PostgreSQL upgrade failed: {result.get('stderr', 'Unknown error')}"
        
        elif os_type == 'rhel':
            return False, "PostgreSQL upgrade on RHEL-based systems requires manual data migration"
        
        else:
            return False, "PostgreSQL upgrade not supported on this operating system"
    
    def initialize_server(self, postgres_version: str = None) -> Tuple[bool, str]:
        """Initialize a new PostgreSQL server with basic setup.
        
        Args:
            postgres_version: Specific PostgreSQL version to install (latest if None)
        
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Initializing PostgreSQL server with version {postgres_version or 'latest'}...")
        
        # Install PostgreSQL if not already installed
        if not self.check_postgres_installed():
            success, message = self.install_postgres(postgres_version)
            if not success:
                return False, f"Failed to install PostgreSQL: {message}"
        
        # Install pgBackRest
        success, message = self.backup_manager.install_pgbackrest()
        if not success:
            self.logger.warning(f"Failed to install pgBackRest: {message}")
        
        # Restart PostgreSQL to ensure it's running
        success, message = self.restart_postgres()
        if not success:
            return False, f"Failed to start PostgreSQL: {message}"
        
        return True, "PostgreSQL server initialized successfully"
    
    # Delegate backup operations to backup manager
    def setup_pgbackrest(self, s3_config: Optional[Dict] = None) -> Tuple[bool, str]:
        """Setup pgBackRest for backups."""
        return self.backup_manager.create_pgbackrest_config(s3_config)
    
    def create_backup_stanza(self, db_name: str) -> Tuple[bool, str]:
        """Create a backup stanza for a database."""
        data_dir = self.get_data_directory()
        if not data_dir:
            return False, "Could not determine PostgreSQL data directory"
        
        # Create stanza configuration
        success, message = self.backup_manager.create_stanza_config(db_name, data_dir)
        if not success:
            return False, message
        
        # Configure PostgreSQL archiving
        success, message = self.backup_manager.configure_postgresql_archiving(db_name)
        if not success:
            return False, message
        
        # Restart PostgreSQL to apply archiving settings
        success, message = self.restart_postgres()
        if not success:
            return False, f"Configuration updated but PostgreSQL restart failed: {message}"
        
        # Create the stanza
        return self.backup_manager.create_stanza(db_name)
    
    def perform_backup(self, db_name: str, backup_type: str = 'incr') -> Tuple[bool, str]:
        """Perform a database backup."""
        return self.backup_manager.perform_backup(db_name, backup_type)
    
    def list_backups(self, db_name: str) -> List[Dict]:
        """List available backups for a database."""
        return self.backup_manager.list_backups(db_name)
    
    def restore_database(self, db_name: str, backup_label: str = None) -> Tuple[bool, str]:
        """Restore a database from backup."""
        return self.backup_manager.restore_database(db_name, backup_label)
    
    def cleanup_old_backups(self, db_name: str, retention_count: int) -> Tuple[bool, str]:
        """Clean up old backups according to retention policy."""
        return self.backup_manager.cleanup_old_backups(db_name, retention_count)
    
    def backup_health_check(self, db_name: str) -> Tuple[bool, str, Dict]:
        """Perform a comprehensive backup system health check."""
        return self.backup_manager.health_check(db_name)
    
    # Delegate user operations to user manager
    def create_database_user(self, username: str, password: str, db_name: str, 
                           permission_level: str = 'read_write') -> Tuple[bool, str]:
        """Create a database user with specified permissions."""
        return self.user_manager.create_database_user(username, password, db_name, permission_level)
    
    def update_user_password(self, username: str, password: str) -> Tuple[bool, str]:
        """Update a user's password."""
        return self.user_manager.update_user_password(username, password)
    
    def delete_database_user(self, username: str) -> Tuple[bool, str]:
        """Delete a database user."""
        return self.user_manager.delete_user(username)
    
    def list_database_users(self, db_name: str) -> List[Dict[str, str]]:
        """List users with access to a database."""
        return self.user_manager.list_database_users(db_name)
    
    # Delegate configuration operations to config manager
    def check_and_fix_external_connections(self) -> Tuple[bool, str, Dict]:
        """Configure PostgreSQL to allow external connections."""
        return self.config_manager.configure_external_connections()
    
    def get_postgresql_setting(self, setting: str) -> Optional[str]:
        """Get a PostgreSQL configuration setting."""
        return self.config_manager.get_postgresql_setting(setting)
    
    def update_postgresql_setting(self, setting: str, value: str) -> Tuple[bool, str]:
        """Update a PostgreSQL configuration setting."""
        return self.config_manager.update_postgresql_setting(setting, value)
    
    # System utilities
    def get_system_info(self) -> Dict[str, str]:
        """Get system information."""
        return {
            'os_type': self.system_utils.detect_os(),
            'postgres_version': self.get_postgres_version() or 'Not installed',
            'postgres_installed': str(self.check_postgres_installed()),
            'pgbackrest_installed': str(self.backup_manager.is_pgbackrest_installed())
        }