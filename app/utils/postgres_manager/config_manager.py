"""Configuration management for PostgreSQL."""

import os
import logging
from typing import Dict, List, Optional, Tuple
from .constants import PostgresConstants
from .system_utils import SystemUtils

class PostgresConfigManager:
    """Manages PostgreSQL configuration files and settings."""
    
    def __init__(self, ssh_manager, system_utils: SystemUtils, logger=None):
        self.ssh = ssh_manager
        self.system_utils = system_utils
        self.logger = logger or logging.getLogger(__name__)
        self._postgresql_conf_path = None
        self._pg_hba_conf_path = None
        self._data_directory = None
    
    def find_postgresql_conf(self) -> Optional[str]:
        """Find the PostgreSQL configuration file.
        
        Returns:
            str: Path to postgresql.conf or None if not found
        """
        if self._postgresql_conf_path:
            return self._postgresql_conf_path
            
        search_paths = []
        
        # Get OS-specific paths
        postgres_paths = self.system_utils.get_postgres_paths()
        
        # Add config directories
        for config_dir in postgres_paths.get('config_dirs', []):
            search_paths.extend([
                f"{config_dir}/*/main/postgresql.conf",
                f"{config_dir}/*/data/postgresql.conf",
                f"{config_dir}/postgresql.conf"
            ])
        
        # Add data directories
        for data_dir in postgres_paths.get('data_dirs', []):
            search_paths.extend([
                f"{data_dir}/*/main/postgresql.conf",
                f"{data_dir}/*/data/postgresql.conf",
                f"{data_dir}/data/postgresql.conf",
                f"{data_dir}/postgresql.conf"
            ])
        
        # Try to find using PostgreSQL query
        try:
            result = self.system_utils.execute_as_postgres_user("psql -t -c 'SHOW config_file;'")
            if result['exit_code'] == 0 and result['stdout'].strip():
                config_file = result['stdout'].strip()
                check_result = self.ssh.execute_command(f"sudo test -f {config_file} && echo 'exists'")
                if 'exists' in check_result['stdout']:
                    self._postgresql_conf_path = config_file
                    self.logger.info(f"Found postgresql.conf via PostgreSQL query: {config_file}")
                    return config_file
        except Exception as e:
            self.logger.debug(f"Could not query PostgreSQL for config file: {str(e)}")
        
        # Search in common paths
        for path_pattern in search_paths:
            result = self.ssh.execute_command(f"sudo find {os.path.dirname(path_pattern)} -name {os.path.basename(path_pattern)} 2>/dev/null | head -1")
            if result['exit_code'] == 0 and result['stdout'].strip():
                config_file = result['stdout'].strip()
                self._postgresql_conf_path = config_file
                self.logger.info(f"Found postgresql.conf: {config_file}")
                return config_file
        
        self.logger.error("Could not locate postgresql.conf")
        return None
    
    def find_pg_hba_conf(self) -> Optional[str]:
        """Find the pg_hba.conf file.
        
        Returns:
            str: Path to pg_hba.conf or None if not found
        """
        if self._pg_hba_conf_path:
            return self._pg_hba_conf_path
            
        # Try to get from PostgreSQL first
        try:
            result = self.system_utils.execute_as_postgres_user("psql -t -c 'SHOW hba_file;'")
            if result['exit_code'] == 0 and result['stdout'].strip():
                hba_file = result['stdout'].strip()
                check_result = self.ssh.execute_command(f"sudo test -f {hba_file} && echo 'exists'")
                if 'exists' in check_result['stdout']:
                    self._pg_hba_conf_path = hba_file
                    self.logger.info(f"Found pg_hba.conf via PostgreSQL query: {hba_file}")
                    return hba_file
        except Exception as e:
            self.logger.debug(f"Could not query PostgreSQL for hba file: {str(e)}")
        
        # Check in same directory as postgresql.conf
        postgresql_conf = self.find_postgresql_conf()
        if postgresql_conf:
            conf_dir = os.path.dirname(postgresql_conf)
            hba_path = os.path.join(conf_dir, "pg_hba.conf")
            check_result = self.ssh.execute_command(f"sudo test -f {hba_path} && echo 'exists'")
            if 'exists' in check_result['stdout']:
                self._pg_hba_conf_path = hba_path
                self.logger.info(f"Found pg_hba.conf in config directory: {hba_path}")
                return hba_path
        
        # Check in data directory
        data_dir = self.get_data_directory()
        if data_dir:
            hba_path = os.path.join(data_dir, "pg_hba.conf")
            check_result = self.ssh.execute_command(f"sudo test -f {hba_path} && echo 'exists'")
            if 'exists' in check_result['stdout']:
                self._pg_hba_conf_path = hba_path
                self.logger.info(f"Found pg_hba.conf in data directory: {hba_path}")
                return hba_path
        
        # Search in common locations
        postgres_paths = self.system_utils.get_postgres_paths()
        search_paths = []
        
        for config_dir in postgres_paths.get('config_dirs', []):
            search_paths.extend([
                f"{config_dir}/*/main/pg_hba.conf",
                f"{config_dir}/*/data/pg_hba.conf",
                f"{config_dir}/pg_hba.conf"
            ])
        
        for path_pattern in search_paths:
            result = self.ssh.execute_command(f"sudo find {os.path.dirname(path_pattern)} -name {os.path.basename(path_pattern)} 2>/dev/null | head -1")
            if result['exit_code'] == 0 and result['stdout'].strip():
                hba_file = result['stdout'].strip()
                self._pg_hba_conf_path = hba_file
                self.logger.info(f"Found pg_hba.conf: {hba_file}")
                return hba_file
        
        self.logger.error("Could not locate pg_hba.conf")
        return None
    
    def get_data_directory(self) -> Optional[str]:
        """Get PostgreSQL data directory.
        
        Returns:
            str: Path to data directory or None if not found
        """
        if self._data_directory:
            return self._data_directory
            
        # Try to get from PostgreSQL
        try:
            result = self.system_utils.execute_as_postgres_user("psql -t -c 'SHOW data_directory;'")
            if result['exit_code'] == 0 and result['stdout'].strip():
                data_dir = result['stdout'].strip()
                check_result = self.ssh.execute_command(f"sudo test -d {data_dir} && echo 'exists'")
                if 'exists' in check_result['stdout']:
                    self._data_directory = data_dir
                    self.logger.info(f"Found data directory via PostgreSQL query: {data_dir}")
                    return data_dir
        except Exception as e:
            self.logger.debug(f"Could not query PostgreSQL for data directory: {str(e)}")
        
        # Search in common locations
        postgres_paths = self.system_utils.get_postgres_paths()
        search_paths = []
        
        for data_dir in postgres_paths.get('data_dirs', []):
            search_paths.extend([
                f"{data_dir}/*/main",
                f"{data_dir}/*/data",
                f"{data_dir}/data"
            ])
        
        for path in search_paths:
            # Check if this looks like a PostgreSQL data directory
            check_result = self.ssh.execute_command(f"sudo test -f {path}/PG_VERSION && echo 'exists'")
            if 'exists' in check_result['stdout']:
                self._data_directory = path
                self.logger.info(f"Found data directory: {path}")
                return path
        
        self.logger.error("Could not locate PostgreSQL data directory")
        return None
    
    def create_default_pg_hba(self, target_path: str) -> bool:
        """Create a default pg_hba.conf file.
        
        Args:
            target_path: Path where pg_hba.conf should be created
            
        Returns:
            bool: True if creation was successful
        """
        self.logger.info(f"Creating default pg_hba.conf at {target_path}")
        
        # Write content to a temporary file
        temp_file = '/tmp/default_pg_hba.conf'
        with open(temp_file, 'w') as f:
            f.write(PostgresConstants.DEFAULT_PG_HBA_CONTENT)
        
        # Upload the file to the server
        upload_result = self.ssh.upload_file(temp_file, temp_file)
        if not upload_result:
            self.logger.error("Failed to upload default pg_hba.conf")
            return False
        
        # Create directory and move file
        dir_path = os.path.dirname(target_path)
        success, message = self.system_utils.create_directory(dir_path)
        if not success:
            self.logger.error(f"Failed to create directory {dir_path}: {message}")
            return False
        
        # Move file to target location
        move_result = self.ssh.execute_command(f"sudo cp {temp_file} {target_path}")
        if move_result['exit_code'] != 0:
            self.logger.error(f"Failed to move pg_hba.conf: {move_result['stderr']}")
            return False
        
        # Set correct ownership and permissions
        self.ssh.execute_command(f"sudo chown postgres:postgres {target_path}")
        self.ssh.execute_command(f"sudo chmod 600 {target_path}")
        
        # Clean up temp file
        self.ssh.execute_command(f"rm -f {temp_file}")
        
        self.logger.info(f"Successfully created default pg_hba.conf at {target_path}")
        return True
    
    def update_postgresql_setting(self, setting: str, value: str) -> Tuple[bool, str]:
        """Update a setting in postgresql.conf.
        
        Args:
            setting: Setting name
            value: Setting value
            
        Returns:
            tuple: (success, message)
        """
        postgresql_conf = self.find_postgresql_conf()
        if not postgresql_conf:
            return False, "Could not locate postgresql.conf"
        
        # Backup the file first
        success, backup_path = self.system_utils.backup_file(postgresql_conf)
        if not success:
            self.logger.warning(f"Could not backup postgresql.conf: {backup_path}")
        
        # Check if setting already exists
        check_cmd = f"sudo grep -E '^[ \\t]*{setting}[ \\t]*=' {postgresql_conf}"
        check_result = self.ssh.execute_command(check_cmd)
        
        if check_result['exit_code'] == 0:
            # Update existing setting - escape quotes properly
            escaped_value = value.replace("'", "'\"'\"'")
            update_cmd = f"sudo sed -i 's|^[ \\t]*{setting}[ \\t]*=.*|{setting} = {escaped_value}|' {postgresql_conf}"
            result = self.ssh.execute_command(update_cmd)
            
            if result['exit_code'] == 0:
                return True, f"Updated {setting} to {value}"
            else:
                return False, f"Failed to update {setting}: {result.get('stderr', 'Unknown error')}"
        else:
            # Add new setting - escape quotes properly
            escaped_value = value.replace("'", "'\"'\"'")
            add_cmd = f"echo '{setting} = {escaped_value}' | sudo tee -a {postgresql_conf}"
            result = self.ssh.execute_command(add_cmd)
            
            if result['exit_code'] == 0:
                return True, f"Added {setting} = {value} to postgresql.conf"
            else:
                return False, f"Failed to add {setting}: {result.get('stderr', 'Unknown error')}"
    
    def get_postgresql_setting(self, setting: str) -> Optional[str]:
        """Get a setting value from postgresql.conf.
        
        Args:
            setting: Setting name
            
        Returns:
            str: Setting value or None if not found
        """
        try:
            result = self.system_utils.execute_as_postgres_user(f"psql -t -c 'SHOW {setting};'")
            if result['exit_code'] == 0 and result['stdout'].strip():
                return result['stdout'].strip()
        except Exception as e:
            self.logger.debug(f"Could not query PostgreSQL for setting {setting}: {str(e)}")
        
        # Fallback to reading from file
        postgresql_conf = self.find_postgresql_conf()
        if postgresql_conf:
            check_cmd = f"sudo grep -E '^[ \\t]*{setting}[ \\t]*=' {postgresql_conf}"
            result = self.ssh.execute_command(check_cmd)
            
            if result['exit_code'] == 0 and result['stdout'].strip():
                # Extract value from line like "setting = value"
                line = result['stdout'].strip()
                if '=' in line:
                    value = line.split('=', 1)[1].strip()
                    # Remove quotes if present
                    if value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    elif value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    return value
        
        return None
    
    def configure_external_connections(self) -> Tuple[bool, str, Dict]:
        """Configure PostgreSQL to allow external connections.
        
        Returns:
            tuple: (success, message, changes_made)
        """
        changes_made = {
            'listen_addresses': False,
            'pg_hba': False,
            'created_files': False
        }
        
        self.logger.info("Configuring PostgreSQL for external connections...")
        
        # Update listen_addresses
        current_listen = self.get_postgresql_setting('listen_addresses')
        if current_listen != '*':
            success, message = self.update_postgresql_setting('listen_addresses', "'*'")
            if success:
                changes_made['listen_addresses'] = True
                self.logger.info("Updated listen_addresses to '*'")
            else:
                return False, f"Failed to update listen_addresses: {message}", changes_made
        else:
            self.logger.info("listen_addresses already set to '*'")
        
        # Configure pg_hba.conf
        pg_hba_path = self.find_pg_hba_conf()
        if not pg_hba_path:
            # Try to create it
            data_dir = self.get_data_directory()
            if data_dir:
                pg_hba_path = os.path.join(data_dir, "pg_hba.conf")
                if self.create_default_pg_hba(pg_hba_path):
                    changes_made['created_files'] = True
                    changes_made['pg_hba'] = True
                else:
                    return False, "Could not create pg_hba.conf", changes_made
            else:
                return False, "Could not locate or create pg_hba.conf", changes_made
        
        if pg_hba_path and not changes_made['created_files']:
            # Backup existing file
            self.system_utils.backup_file(pg_hba_path)
            
            # Check for external IPv4 access
            check_ipv4 = self.ssh.execute_command(f"sudo grep -E '^host[ \\t]+all[ \\t]+all[ \\t]+0\\.0\\.0\\.0/0[ \\t]+' {pg_hba_path}")
            if check_ipv4['exit_code'] != 0:
                add_ipv4 = self.ssh.execute_command(f"echo 'host    all    all    0.0.0.0/0    md5' | sudo tee -a {pg_hba_path}")
                if add_ipv4['exit_code'] == 0:
                    changes_made['pg_hba'] = True
                    self.logger.info("Added IPv4 external access rule")
            
            # Check for external IPv6 access
            check_ipv6 = self.ssh.execute_command(f"sudo grep -E '^host[ \\t]+all[ \\t]+all[ \\t]+::/0[ \\t]+' {pg_hba_path}")
            if check_ipv6['exit_code'] != 0:
                add_ipv6 = self.ssh.execute_command(f"echo 'host    all    all    ::/0    md5' | sudo tee -a {pg_hba_path}")
                if add_ipv6['exit_code'] == 0:
                    changes_made['pg_hba'] = True
                    self.logger.info("Added IPv6 external access rule")
        
        # Restart PostgreSQL if changes were made
        if any(changes_made.values()):
            self.logger.info("Configuration changes made, restarting PostgreSQL...")
            success, message = self.system_utils.restart_service('postgresql')
            if not success:
                return False, f"Configuration updated but PostgreSQL restart failed: {message}", changes_made
        
        return True, "PostgreSQL configured for external connections", changes_made
    
    def fix_postgresql_config(self) -> Tuple[bool, str]:
        """Fix malformed PostgreSQL configuration.
        
        Returns:
            tuple: (success, message)
        """
        postgresql_conf = self.find_postgresql_conf()
        if not postgresql_conf:
            return False, "Could not locate postgresql.conf"
        
        self.logger.info("Fixing malformed PostgreSQL configuration...")
        
        # Backup the file first
        success, backup_path = self.system_utils.backup_file(postgresql_conf)
        if not success:
            self.logger.warning(f"Could not backup postgresql.conf: {backup_path}")
        
        # Remove malformed listen_addresses lines
        remove_cmd = f"sudo sed -i '/^[ \\t]*listen_addresses[ \\t]*=[ \\t]*\*[ \\t]*$/d' {postgresql_conf}"
        result = self.ssh.execute_command(remove_cmd)
        
        if result['exit_code'] != 0:
            return False, f"Failed to remove malformed listen_addresses: {result.get('stderr', 'Unknown error')}"
        
        # Add correct listen_addresses setting
        success, message = self.update_postgresql_setting('listen_addresses', "'*'")
        if not success:
            return False, f"Failed to add correct listen_addresses: {message}"
        
        # Restart PostgreSQL to apply changes
        success, message = self.system_utils.restart_service('postgresql')
        if not success:
            return False, f"Configuration fixed but PostgreSQL restart failed: {message}"
        
        return True, "PostgreSQL configuration fixed successfully"