"""Core PostgreSQL management functionality."""

import logging
from typing import Any, Dict, List, Optional, Tuple
from .constants import PostgresConstants
from .system_utils import SystemUtils
from .config_manager import PostgresConfigManager
from .backup_manager import PostgresBackupManager
from .user_manager import PostgresUserManager
from .version_resolver import PostgresVersionResolver
from .error_handler import PostgresErrorHandler

class PostgresManager:
    """Main PostgreSQL management class using modular components."""
    
    def __init__(self, ssh_manager, logger=None):
        self.ssh = ssh_manager
        self.logger = logger or logging.getLogger(__name__)
        self.error_handler = PostgresErrorHandler(self.logger)
        
        # Initialize modular components
        self.system_utils = SystemUtils(ssh_manager, logger)
        self.config_manager = PostgresConfigManager(ssh_manager, self.system_utils, logger)
        self.backup_manager = PostgresBackupManager(ssh_manager, self.system_utils, self.config_manager, logger)
        self.user_manager = PostgresUserManager(ssh_manager, self.system_utils, logger)
        self.version_resolver = PostgresVersionResolver(ssh_manager, self.system_utils, logger)
        
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
    
    def get_latest_patch_version(self, major_version: str = None) -> Tuple[bool, str]:
        """Get the latest patch version for a given major version.
        
        Args:
            major_version: Major version (e.g., '15', '16'). If None, gets latest available major version.
            
        Returns:
            tuple: (success, version_string)
        """
        success, resolved_version, metadata = self.version_resolver.resolve_version(major_version)
        
        if success:
            # Log any warnings from the resolution process
            for warning in metadata.get('warnings', []):
                self.logger.warning(warning)
            
            if metadata.get('fallback_used'):
                self.logger.info(f"Used fallback version: {resolved_version}")
            
            return True, resolved_version
        else:
            return False, resolved_version  # resolved_version contains error message
    
    def install_postgres(self, version: str = None) -> Tuple[bool, str]:
        """Install PostgreSQL with the specified or latest version.
        
        Args:
            version: PostgreSQL major version to install (e.g., '15', '16'). 
                    If None, installs the recommended version.
                    
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Starting PostgreSQL installation (version: {version or 'recommended'})...")
        
        if self.check_postgres_installed():
            return True, "PostgreSQL is already installed"
        
        # Get package manager commands
        pkg_commands = self.system_utils.get_package_manager_commands()
        if not pkg_commands:
            return False, "Unsupported operating system for automatic installation"
        
        # Resolve version using the version resolver
        success, resolved_version, metadata = self.version_resolver.resolve_version(version)
        if not success:
            return False, f"Failed to resolve PostgreSQL version: {resolved_version}"
        
        self.logger.info(f"Installing PostgreSQL version: {resolved_version}")
        
        # Log any warnings from version resolution
        for warning in metadata.get('warnings', []):
            self.logger.warning(warning)
        
        # Update package list first
        if 'update' in pkg_commands:
            self.logger.info("Updating package list...")
            update_result = self.ssh.execute_command(f"sudo {pkg_commands['update']}")
            if update_result['exit_code'] != 0:
                self.error_handler.log_warning_with_context(
                    "Package update failed", "Installation", update_result
                )
        
        # Install PostgreSQL with the resolved version
        success, message = self._install_postgres_version(resolved_version)
        if success:
            return True, f"PostgreSQL {resolved_version} installed and started successfully"
        
        # If specific version installation failed, try fallback approaches
        self.error_handler.log_warning_with_context(
            f"Failed to install PostgreSQL {resolved_version}, trying fallback methods",
            "Installation Fallback"
        )
        
        # Try without version suffix (latest available in repository)
        success, message = self._install_postgres_version(None)
        if success:
            return True, "PostgreSQL (latest available) installed and started successfully"
        
        return False, f"Failed to install PostgreSQL: {message}"
    
    def _install_postgres_version(self, version: str = None) -> Tuple[bool, str]:
        """Internal method to install a specific PostgreSQL version.
        
        Args:
            version: Specific version to install (None for default)
            
        Returns:
            tuple: (success, message)
        """
        try:
            # Update package list first
            self.logger.info("Updating package list...")
            os_type = self.system_utils.detect_os()
            
            if os_type == 'debian':
                update_result = self.ssh.execute_command("sudo apt-get update")
                if update_result['exit_code'] != 0:
                    self.error_handler.log_warning_with_context(
                    "Package update failed, continuing with installation", "Installation"
                )
            elif os_type == 'rhel':
                # Use the detected package manager (dnf or yum)
                pkg_commands = self.system_utils.get_package_manager_commands()
                if 'makecache' in pkg_commands:
                    update_result = self.ssh.execute_command(f"sudo {pkg_commands['makecache']}")
                    if update_result['exit_code'] != 0:
                        self.error_handler.log_warning_with_context(
                    "Package cache update failed, continuing with installation", "Installation"
                )
            
            # Get package names for the version
            package_names = self.system_utils.get_postgres_package_names(version)
            if not package_names:
                return False, f"No package names found for PostgreSQL {version}"
            
            # Get package manager commands
            pkg_commands = self.system_utils.get_package_manager_commands()
            if not pkg_commands or 'install' not in pkg_commands:
                return False, "Package manager not supported"
            
            # Try to install the specific version
            install_cmd = f"sudo {pkg_commands['install']} {' '.join(package_names)}"
            self.logger.info(f"Installing PostgreSQL {version} with command: {install_cmd}")
            
            result = self.ssh.execute_command(install_cmd)
            
            if result['exit_code'] != 0:
                # If specific version installation fails, try with recommended version
                self.error_handler.log_warning_with_context(
                    f"Installation of PostgreSQL {version} failed, trying recommended version",
                    "Installation Fallback"
                )
                recommended_version = PostgresConstants.SUPPORTED_VERSIONS['recommended']
                
                if version != recommended_version:
                    fallback_packages = self.system_utils.get_postgres_package_names(recommended_version)
                    if fallback_packages:
                        fallback_cmd = f"sudo {pkg_commands['install']} {' '.join(fallback_packages)}"
                        self.logger.info(f"Fallback installation command: {fallback_cmd}")
                        fallback_result = self.ssh.execute_command(fallback_cmd)
                        
                        if fallback_result['exit_code'] == 0:
                            version = recommended_version  # Update version for service management
                            self.logger.info(f"Successfully installed PostgreSQL {recommended_version} as fallback")
                        else:
                            return False, f"PostgreSQL installation failed: {fallback_result.get('stderr', 'Unknown error')}"
                    else:
                        return False, f"PostgreSQL installation failed: {result.get('stderr', 'Unknown error')}"
                else:
                    return False, f"PostgreSQL installation failed: {result.get('stderr', 'Unknown error')}"
            
            # Initialize database for RHEL-based systems
            if os_type == 'rhel':
                self.logger.info("Initializing PostgreSQL database...")
                # Try version-specific initialization first, then generic
                init_commands = [
                    f"sudo postgresql-{version}-setup initdb",
                    "sudo postgresql-setup initdb",
                    f"sudo /usr/pgsql-{version}/bin/postgresql-{version}-setup initdb"
                ]
                
                init_success = False
                for init_cmd in init_commands:
                    init_result = self.ssh.execute_command(init_cmd)
                    if init_result['exit_code'] == 0:
                        init_success = True
                        break
                    self.logger.debug(f"Init command failed: {init_cmd}")
                
                if not init_success:
                    self.logger.warning("Database initialization failed, but continuing...")
            
            # Start and enable PostgreSQL service
            service_names = [
                f"postgresql-{version}",
                "postgresql",
                f"postgresql@{version}-main"
            ]
            
            service_started = False
            for service_name in service_names:
                self.logger.info(f"Attempting to start PostgreSQL service: {service_name}")
                success, message = self.system_utils.start_service(service_name)
                if success:
                    service_started = True
                    self.logger.info(f"Successfully started service: {service_name}")
                    
                    # Enable the service
                    enable_cmd = f"sudo systemctl enable {service_name}"
                    enable_result = self.ssh.execute_command(enable_cmd)
                    if enable_result['exit_code'] != 0:
                        self.logger.warning(f"Failed to enable PostgreSQL service: {enable_result.get('stderr', 'Unknown error')}")
                    break
            
            if not service_started:
                self.logger.warning("Could not start PostgreSQL service automatically")
            
            # Clear cache
            self._postgres_installed = None
            self._postgres_version = None
            
            return True, f"PostgreSQL {version} installed successfully"
            
        except Exception as e:
            self.logger.error(f"Error during PostgreSQL installation: {str(e)}")
            return False, f"Installation error: {str(e)}"
    
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
    def check_and_fix_external_connections(self, allowed_ips: List[str] = None, auth_method: str = 'scram-sha-256') -> Tuple[bool, str, Dict]:
        """Configure PostgreSQL to allow external connections with IP whitelisting.
        
        Args:
            allowed_ips: List of allowed IP addresses/CIDR blocks. If None, allows all IPs (0.0.0.0/0)
            auth_method: Authentication method (scram-sha-256, md5, etc.)
        
        Returns:
            tuple: (success, message, changes_made)
        """
        return self.config_manager.configure_external_connections(allowed_ips, auth_method)
    
    def get_postgresql_setting(self, setting: str) -> Optional[str]:
        """Get a PostgreSQL configuration setting."""
        return self.config_manager.get_postgresql_setting(setting)
    
    def get_pg_hba_entries(self) -> List[Dict[str, str]]:
        """Get current pg_hba.conf entries for external access."""
        return self.config_manager.get_pg_hba_entries()
    
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
    
    def get_ssl_status(self) -> Dict[str, Any]:
        """Get current SSL/TLS configuration status.
        
        Returns:
            dict: SSL status information
        """
        return self.config_manager.get_ssl_status()
    
    def update_postgresql_setting(self, setting: str, value: str) -> Tuple[bool, str]:
        """Update a PostgreSQL configuration setting."""
        return self.config_manager.update_postgresql_setting(setting, value)
    
    def fix_postgresql_config(self) -> Tuple[bool, str]:
        """Fix malformed PostgreSQL configuration."""
        return self.config_manager.fix_postgresql_config()
    
    # System utilities
    def get_system_info(self) -> Dict[str, str]:
        """Get system information."""
        return {
            'os_type': self.system_utils.detect_os(),
            'postgres_version': self.get_postgres_version() or 'Not installed',
            'postgres_installed': str(self.check_postgres_installed()),
            'pgbackrest_installed': str(self.backup_manager.is_pgbackrest_installed())
        }