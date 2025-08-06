"""System utilities for PostgreSQL management."""

import logging
import time
from typing import Any, Dict, List, Tuple
from .constants import PostgresConstants

class SystemUtils:
    """Utility class for system-level operations."""
    
    def __init__(self, ssh_manager, logger=None):
        self.ssh = ssh_manager
        self.logger = logger or logging.getLogger(__name__)
        self._os_type = None
        self._os_version = None
    
    def detect_os(self) -> str:
        """Detect the operating system type.
        
        Returns:
            str: 'debian', 'rhel', or 'unknown'
        """
        if self._os_type:
            return self._os_type
            
        try:
            # Check /etc/os-release first (most reliable)
            result = self.ssh.execute_command("cat /etc/os-release")
            if result['exit_code'] == 0:
                os_info = result['stdout'].lower()
                
                for os_type, patterns in PostgresConstants.OS_PATTERNS.items():
                    if any(pattern in os_info for pattern in patterns):
                        self._os_type = os_type
                        self.logger.info(f"Detected OS type: {os_type}")
                        return os_type
            
            # Fallback to lsb_release
            result = self.ssh.execute_command("lsb_release -i")
            if result['exit_code'] == 0:
                os_info = result['stdout'].lower()
                
                for os_type, patterns in PostgresConstants.OS_PATTERNS.items():
                    if any(pattern in os_info for pattern in patterns):
                        self._os_type = os_type
                        self.logger.info(f"Detected OS type: {os_type} (via lsb_release)")
                        return os_type
            
            # Final fallback to uname
            result = self.ssh.execute_command("uname -a")
            if result['exit_code'] == 0:
                os_info = result['stdout'].lower()
                if 'ubuntu' in os_info or 'debian' in os_info:
                    self._os_type = 'debian'
                elif 'centos' in os_info or 'rhel' in os_info or 'red hat' in os_info:
                    self._os_type = 'rhel'
                else:
                    self._os_type = 'unknown'
                    
                self.logger.info(f"Detected OS type: {self._os_type} (via uname)")
                return self._os_type
                
        except Exception as e:
            self.logger.error(f"Error detecting OS: {str(e)}")
            
        self._os_type = 'unknown'
        return self._os_type
    
    def get_package_manager_commands(self) -> Dict[str, str]:
        """Get package manager commands for the detected OS.
        
        Returns:
            dict: Package manager commands
        """
        os_type = self.detect_os()
        
        if os_type == 'debian':
            return {
                'update': 'apt-get update',
                'install': 'apt-get install -y',
                'search': 'apt-cache search',
                'list_installed': 'dpkg -l',
                'madison': 'apt-cache madison',
                'policy': 'apt-cache policy'
            }
        elif os_type == 'rhel':
            # Detect if dnf is available (newer RHEL/Fedora)
            dnf_check = self.ssh.execute_command('which dnf')
            if dnf_check['exit_code'] == 0:
                return {
                    'update': 'dnf update -y',
                    'install': 'dnf install -y',
                    'search': 'dnf search',
                    'list_installed': 'rpm -qa',
                    'list_available': 'dnf list available'
                }
            else:
                return {
                    'update': 'yum update -y',
                    'install': 'yum install -y',
                    'search': 'yum search',
                    'list_installed': 'rpm -qa',
                    'list_available': 'yum list available'
                }
        else:
            return {}
    
    def get_postgres_package_names(self, version: str = None) -> List[str]:
        """Get PostgreSQL package names for the detected OS and version.
        
        Args:
            version: PostgreSQL major version (e.g., '15', '16')
                    If None, returns base package names
        
        Returns:
            list: Package names for the version
        """
        os_type = self.detect_os()
        base_packages = PostgresConstants.PACKAGE_NAMES.get(os_type, {})
        
        if not base_packages:
            return []
        
        if not version:
            # Return base packages without version suffix
            return list(base_packages.values())
        
        packages = []
        
        if os_type == 'debian':
            # For Debian/Ubuntu: postgresql-16, postgresql-contrib-16
            packages.append(f"postgresql-{version}")
            if 'postgresql_contrib' in base_packages:
                packages.append(f"postgresql-contrib-{version}")
        elif os_type == 'rhel':
            # For RHEL/CentOS: postgresql16-server, postgresql16-contrib
            packages.append(f"postgresql{version}-server")
            if 'postgresql_contrib' in base_packages:
                packages.append(f"postgresql{version}-contrib")
        
        return packages
    
    def get_available_postgres_versions(self) -> List[str]:
        """Get list of available PostgreSQL major versions.
        
        Returns:
            list: Available major versions (e.g., ['13', '14', '15', '16'])
        """
        os_type = self.detect_os()
        pkg_commands = self.get_package_manager_commands()
        
        if not pkg_commands:
            return []
        
        versions = []
        
        if os_type == 'debian':
            # Search for postgresql-XX packages
            result = self.ssh.execute_command("apt-cache search '^postgresql-[0-9]+$' | grep -o 'postgresql-[0-9]\+' | sort -V")
            if result['exit_code'] == 0 and result['stdout']:
                for line in result['stdout'].strip().split('\n'):
                    if line.strip():
                        version = line.replace('postgresql-', '')
                        if version.isdigit():
                            versions.append(version)
        
        elif os_type == 'rhel':
            # Search for postgresqlXX-server packages
            result = self.ssh.execute_command("yum search postgresql | grep -o 'postgresql[0-9]\+' | sort -V | uniq")
            if result['exit_code'] == 0 and result['stdout']:
                for line in result['stdout'].strip().split('\n'):
                    if line.strip():
                        version = line.replace('postgresql', '')
                        if version.isdigit():
                            versions.append(version)
        
        return versions
    
    def validate_postgres_version(self, version: str) -> bool:
        """Validate if a PostgreSQL version is available for installation.
        
        Args:
            version: Major version to validate (e.g., '15', '16')
            
        Returns:
            bool: True if version is available
        """
        if not version or not version.isdigit():
            return False
        
        available_versions = self.get_available_postgres_versions()
        return version in available_versions
    
    def get_postgres_paths(self) -> Dict[str, List[str]]:
        """Get PostgreSQL paths for the detected OS.
        
        Returns:
            dict: PostgreSQL paths
        """
        os_type = self.detect_os()
        return PostgresConstants.DEFAULT_POSTGRES_PATHS.get(os_type, {})
    
    def execute_with_retry(self, command: str, max_retries: int = None, 
                          retry_delay: int = None) -> Dict:
        """Execute a command with retry logic.
        
        Args:
            command: Command to execute
            max_retries: Maximum number of retries
            retry_delay: Delay between retries in seconds
            
        Returns:
            dict: Command result
        """
        
        max_retries = max_retries or PostgresConstants.RETRIES['max_retries']
        retry_delay = retry_delay or PostgresConstants.RETRIES['retry_delay']
        
        last_result = None
        
        for attempt in range(max_retries + 1):
            result = self.ssh.execute_command(command)
            
            if result['exit_code'] == 0:
                return result
                
            last_result = result
            
            if attempt < max_retries:
                self.logger.warning(f"Command failed (attempt {attempt + 1}/{max_retries + 1}): {command}")
                self.logger.warning(f"Error: {result.get('stderr', 'Unknown error')}")
                time.sleep(retry_delay)
            else:
                self.logger.error(f"Command failed after {max_retries + 1} attempts: {command}")
                
        return last_result
    
    def execute_as_postgres_user(self, command: str) -> Dict:
        """Execute a command as the postgres user.
        
        Args:
            command: Command to execute
            
        Returns:
            dict: Command result
        """
        postgres_command = f"sudo -u postgres {command}"
        return self.ssh.execute_command(postgres_command)
    
    def execute_postgres_sql(self, sql: str, database: str = 'postgres') -> Dict:
        """Execute a SQL command in PostgreSQL.
        
        Args:
            sql: SQL command to execute
            database: Database to connect to
            
        Returns:
            dict: Command result
        """
        escaped_sql = sql.replace('"', '\\"')
        command = f'psql -d {database} -c "{escaped_sql}"'
        return self.execute_as_postgres_user(command)
    
    def check_service_status(self, service_name: str) -> Tuple[bool, str]:
        """Check if a service is running.
        
        Args:
            service_name: Name of the service
            
        Returns:
            tuple: (is_running, status_message)
        """
        result = self.ssh.execute_command(f"systemctl is-active {service_name}")
        
        if result['exit_code'] == 0 and 'active' in result['stdout']:
            return True, "Service is running"
        else:
            return False, f"Service is not running: {result.get('stdout', 'Unknown status')}"
    
    def start_service(self, service_name: str) -> Tuple[bool, str]:
        """Start a system service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            tuple: (success, message)
        """
        result = self.ssh.execute_command(f"sudo systemctl start {service_name}")
        
        if result['exit_code'] == 0:
            # Wait a moment and check if it's actually running
            time.sleep(2)
            
            is_running, status = self.check_service_status(service_name)
            if is_running:
                return True, f"Service {service_name} started successfully"
            else:
                return False, f"Service {service_name} failed to start: {status}"
        else:
            return False, f"Failed to start {service_name}: {result.get('stderr', 'Unknown error')}"
    
    def stop_service(self, service_name: str) -> Tuple[bool, str]:
        """Stop a system service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            tuple: (success, message)
        """
        result = self.ssh.execute_command(f"sudo systemctl stop {service_name}")
        
        if result['exit_code'] == 0:
            return True, f"Service {service_name} stopped successfully"
        else:
            return False, f"Failed to stop {service_name}: {result.get('stderr', 'Unknown error')}"
    
    def restart_service(self, service_name: str) -> Tuple[bool, str]:
        """Restart a system service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            tuple: (success, message)
        """
        result = self.ssh.execute_command(f"sudo systemctl restart {service_name}")
        
        if result['exit_code'] == 0:
            # Wait a moment and check if it's actually running
            time.sleep(3)
            
            is_running, status = self.check_service_status(service_name)
            if is_running:
                return True, f"Service {service_name} restarted successfully"
            else:
                return False, f"Service {service_name} failed to restart: {status}"
        else:
            return False, f"Failed to restart {service_name}: {result.get('stderr', 'Unknown error')}"
    
    def check_postgresql_service(self) -> Dict[str, Any]:
        """Check PostgreSQL service status.
        
        Returns:
            dict: Service status information
        """
        os_type = self.detect_os()
        service_name = 'postgresql'
        
        # For RHEL-based systems, try to detect the version-specific service
        if os_type == 'rhel':
            # Try to find the PostgreSQL version and use version-specific service name
            version_result = self.ssh.execute_command("rpm -qa | grep postgresql-server | head -1")
            if version_result['exit_code'] == 0 and version_result['stdout']:
                # Extract version from package name
                import re
                version_match = re.search(r'postgresql(\d+)-server', version_result['stdout'])
                if version_match:
                    version = version_match.group(1)
                    service_name = f'postgresql-{version}'
        
        is_running, status = self.check_service_status(service_name)
        
        return {
            'service_name': service_name,
            'is_running': is_running,
            'status': status
        }
    
    def start_postgresql_service(self) -> Tuple[bool, str]:
        """Start PostgreSQL service.
        
        Returns:
            tuple: (success, message)
        """
        service_info = self.check_postgresql_service()
        service_name = service_info['service_name']
        
        if service_info['is_running']:
            return True, f"PostgreSQL service ({service_name}) is already running"
        
        return self.start_service(service_name)
    
    def stop_postgresql_service(self) -> Tuple[bool, str]:
        """Stop PostgreSQL service.
        
        Returns:
            tuple: (success, message)
        """
        service_info = self.check_postgresql_service()
        service_name = service_info['service_name']
        
        if not service_info['is_running']:
            return True, f"PostgreSQL service ({service_name}) is already stopped"
        
        return self.stop_service(service_name)
    
    def restart_postgresql_service(self) -> Tuple[bool, str]:
        """Restart PostgreSQL service.
        
        Returns:
            tuple: (success, message)
        """
        service_info = self.check_postgresql_service()
        service_name = service_info['service_name']
        
        return self.restart_service(service_name)
    
    def create_directory(self, path: str, owner: str = None, permissions: str = None) -> Tuple[bool, str]:
        """Create a directory with specified owner and permissions.
        
        Args:
            path: Directory path to create
            owner: Owner in format 'user:group'
            permissions: Permissions in octal format (e.g., '755')
            
        Returns:
            tuple: (success, message)
        """
        # Create directory
        result = self.ssh.execute_command(f"sudo mkdir -p {path}")
        if result['exit_code'] != 0:
            return False, f"Failed to create directory {path}: {result.get('stderr', 'Unknown error')}"
        
        # Set owner if specified
        if owner:
            result = self.ssh.execute_command(f"sudo chown {owner} {path}")
            if result['exit_code'] != 0:
                return False, f"Failed to set owner for {path}: {result.get('stderr', 'Unknown error')}"
        
        # Set permissions if specified
        if permissions:
            result = self.ssh.execute_command(f"sudo chmod {permissions} {path}")
            if result['exit_code'] != 0:
                return False, f"Failed to set permissions for {path}: {result.get('stderr', 'Unknown error')}"
        
        return True, f"Directory {path} created successfully"
    
    def backup_file(self, file_path: str) -> Tuple[bool, str]:
        """Create a backup of a file with timestamp.
        
        Args:
            file_path: Path to the file to backup
            
        Returns:
            tuple: (success, backup_path)
        """
        import datetime
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        backup_path = f"{file_path}.bak.{timestamp}"
        
        result = self.ssh.execute_command(f"sudo cp {file_path} {backup_path}")
        
        if result['exit_code'] == 0:
            return True, backup_path
        else:
            return False, f"Failed to backup {file_path}: {result.get('stderr', 'Unknown error')}"
    
    def setup_postgres_repository(self) -> Tuple[bool, str]:
        """Setup the official PostgreSQL repository.
        
        Returns:
            tuple: (success, message)
        """
        os_type = self.detect_os()
        
        if os_type == 'debian':
            self.logger.info("Setting up PostgreSQL official repository for Debian/Ubuntu...")
            
            # Install required packages
            install_deps = self.ssh.execute_command("sudo apt-get update && sudo apt-get install -y wget ca-certificates")
            if install_deps['exit_code'] != 0:
                return False, f"Failed to install dependencies: {install_deps.get('stderr', 'Unknown error')}"
            
            # Add PostgreSQL signing key
            key_cmd = "wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -"
            key_result = self.ssh.execute_command(key_cmd)
            if key_result['exit_code'] != 0:
                return False, f"Failed to add PostgreSQL signing key: {key_result.get('stderr', 'Unknown error')}"
            
            # Get OS version for repository URL
            version_result = self.ssh.execute_command("lsb_release -cs")
            if version_result['exit_code'] != 0:
                return False, "Failed to detect OS version"
            
            os_codename = version_result['stdout'].strip()
            
            # Add PostgreSQL repository
            repo_line = f"deb http://apt.postgresql.org/pub/repos/apt/ {os_codename}-pgdg main"
            repo_cmd = f"echo '{repo_line}' | sudo tee /etc/apt/sources.list.d/pgdg.list"
            repo_result = self.ssh.execute_command(repo_cmd)
            if repo_result['exit_code'] != 0:
                return False, f"Failed to add PostgreSQL repository: {repo_result.get('stderr', 'Unknown error')}"
            
            # Update package list
            update_result = self.ssh.execute_command("sudo apt-get update")
            if update_result['exit_code'] != 0:
                return False, f"Failed to update package list: {update_result.get('stderr', 'Unknown error')}"
            
            return True, "PostgreSQL official repository configured successfully"
        
        elif os_type == 'rhel':
            self.logger.info("Setting up PostgreSQL repository for RHEL/CentOS...")
            
            # Install PostgreSQL repository RPM
            repo_cmd = "sudo yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm"
            repo_result = self.ssh.execute_command(repo_cmd)
            if repo_result['exit_code'] != 0:
                return False, f"Failed to install PostgreSQL repository: {repo_result.get('stderr', 'Unknown error')}"
            
            return True, "PostgreSQL repository configured successfully"
        
        return False, "Unsupported operating system for repository setup"
    
    def get_postgres_repository_info(self) -> Tuple[bool, str]:
        """Get information about PostgreSQL repository configuration.
        
        Returns:
            tuple: (success, info_message)
        """
        os_type = self.detect_os()
        
        if os_type == 'debian':
            # Check if PostgreSQL official repository is configured
            result = self.ssh.execute_command("apt-cache policy postgresql | grep -i 'apt.postgresql.org'")
            if result['exit_code'] == 0 and result['stdout'].strip():
                return True, "PostgreSQL official repository is configured"
            else:
                return True, "Using distribution's PostgreSQL packages"
        
        elif os_type == 'rhel':
            # Check for PostgreSQL repository
            result = self.ssh.execute_command("yum repolist | grep -i postgresql")
            if result['exit_code'] == 0 and result['stdout'].strip():
                return True, "PostgreSQL repository is configured"
            else:
                return True, "Using distribution's PostgreSQL packages"
        
        return False, "Unable to determine repository configuration"
    
    def install_postgresql(self, version: str = None) -> Tuple[bool, str]:
        """Install PostgreSQL.
        
        Args:
            version: PostgreSQL major version (e.g., '15', '16')
                    If None, installs the default version
        
        Returns:
            tuple: (success, message)
        """
        try:
            os_type = self.detect_os()
            if os_type == 'unknown':
                return False, "Unsupported operating system for PostgreSQL installation"
            
            # Get package manager commands
            pkg_commands = self.get_package_manager_commands()
            if not pkg_commands:
                return False, "Package manager not supported"
            
            # Setup PostgreSQL repository for better version control
            repo_success, repo_message = self.setup_postgres_repository()
            if not repo_success:
                self.logger.warning(f"Repository setup failed: {repo_message}")
            
            # Get package names
            packages = self.get_postgres_package_names(version)
            if not packages:
                return False, f"No PostgreSQL packages found for version {version or 'default'}"
            
            # Update package list
            if 'update' in pkg_commands:
                self.logger.info("Updating package list...")
                update_result = self.ssh.execute_command(f"sudo {pkg_commands['update']}")
                if update_result['exit_code'] != 0:
                    self.logger.warning(f"Package update failed: {update_result.get('stderr', 'Unknown error')}")
            
            # Install PostgreSQL packages
            packages_str = ' '.join(packages)
            install_cmd = f"sudo {pkg_commands['install']} {packages_str}"
            
            self.logger.info(f"Installing PostgreSQL packages: {packages_str}")
            result = self.ssh.execute_command(install_cmd)
            
            if result['exit_code'] == 0:
                # Initialize database if needed (for RHEL-based systems)
                if os_type == 'rhel' and version:
                    init_cmd = f"sudo /usr/pgsql-{version}/bin/postgresql-{version}-setup initdb"
                    init_result = self.ssh.execute_command(init_cmd)
                    if init_result['exit_code'] != 0:
                        self.logger.warning(f"Database initialization may have failed: {init_result.get('stderr', '')}")
                
                # Start and enable PostgreSQL service
                service_name = 'postgresql'
                if os_type == 'rhel' and version:
                    service_name = f'postgresql-{version}'
                
                start_success, start_message = self.start_service(service_name)
                if not start_success:
                    self.logger.warning(f"Failed to start PostgreSQL service: {start_message}")
                
                # Enable service to start on boot
                enable_result = self.ssh.execute_command(f"sudo systemctl enable {service_name}")
                if enable_result['exit_code'] != 0:
                    self.logger.warning(f"Failed to enable PostgreSQL service: {enable_result.get('stderr', '')}")
                
                return True, f"PostgreSQL {version or 'default'} installed successfully"
            else:
                error_msg = result.get('stderr', 'Unknown error')
                return False, f"Failed to install PostgreSQL: {error_msg}"
                
        except Exception as e:
            self.logger.error(f"PostgreSQL installation failed: {str(e)}")
            return False, f"Installation failed: {str(e)}"