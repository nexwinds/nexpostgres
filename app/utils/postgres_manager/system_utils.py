"""System utilities for PostgreSQL management."""

import logging
from typing import Dict, List, Optional, Tuple
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
                'list_installed': 'dpkg -l'
            }
        elif os_type == 'rhel':
            return {
                'update': 'yum update -y',
                'install': 'yum install -y',
                'search': 'yum search',
                'list_installed': 'rpm -qa'
            }
        else:
            return {}
    
    def get_postgres_package_names(self) -> Dict[str, str]:
        """Get PostgreSQL package names for the detected OS.
        
        Returns:
            dict: Package names
        """
        os_type = self.detect_os()
        return PostgresConstants.PACKAGE_NAMES.get(os_type, {})
    
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
        import time
        
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
            import time
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
            import time
            time.sleep(3)
            
            is_running, status = self.check_service_status(service_name)
            if is_running:
                return True, f"Service {service_name} restarted successfully"
            else:
                return False, f"Service {service_name} failed to restart: {status}"
        else:
            return False, f"Failed to restart {service_name}: {result.get('stderr', 'Unknown error')}"
    
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