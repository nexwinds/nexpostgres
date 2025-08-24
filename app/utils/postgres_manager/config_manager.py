"""Configuration management for PostgreSQL."""

import os
import logging
from typing import Any, Dict, List, Optional, Tuple
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
                if self.ssh.check_file_exists(config_file):
                    self._postgresql_conf_path = config_file
                    self.logger.info(f"Found postgresql.conf via PostgreSQL query: {config_file}")
                    return config_file
        except Exception as e:
            self.logger.debug(f"Could not query PostgreSQL for config file: {str(e)}")
        
        # Search in common paths
        for path_pattern in search_paths:
            # Expand wildcards to get actual paths
            if '*' in path_pattern:
                expand_result = self.ssh.execute_command(f"sudo ls {path_pattern} 2>/dev/null || true")
                if expand_result['exit_code'] == 0 and expand_result['stdout'].strip():
                    expanded_paths = expand_result['stdout'].strip().split('\n')
                    for expanded_path in expanded_paths:
                        expanded_path = expanded_path.strip()
                        if expanded_path and os.path.basename(expanded_path) == 'postgresql.conf':
                            if self.ssh.check_file_exists(expanded_path):
                                self._postgresql_conf_path = expanded_path
                                self.logger.info(f"Found postgresql.conf: {expanded_path}")
                                return expanded_path
            else:
                # Direct path without wildcards
                if self.ssh.check_file_exists(path_pattern):
                    self._postgresql_conf_path = path_pattern
                    self.logger.info(f"Found postgresql.conf: {path_pattern}")
                    return path_pattern
        
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
                if self.ssh.check_file_exists(hba_file):
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
            if self.ssh.check_file_exists(hba_path):
                self._pg_hba_conf_path = hba_path
                self.logger.info(f"Found pg_hba.conf in config directory: {hba_path}")
                return hba_path
        
        # Check in data directory
        data_dir = self.get_data_directory()
        if data_dir:
            hba_path = os.path.join(data_dir, "pg_hba.conf")
            if self.ssh.check_file_exists(hba_path):
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
            # Expand wildcards to get actual paths
            if '*' in path_pattern:
                expand_result = self.ssh.execute_command(f"sudo ls {path_pattern} 2>/dev/null || true")
                if expand_result['exit_code'] == 0 and expand_result['stdout'].strip():
                    expanded_paths = expand_result['stdout'].strip().split('\n')
                    for expanded_path in expanded_paths:
                        expanded_path = expanded_path.strip()
                        if expanded_path and os.path.basename(expanded_path) == 'pg_hba.conf':
                            if self.ssh.check_file_exists(expanded_path):
                                self._pg_hba_conf_path = expanded_path
                                self.logger.info(f"Found pg_hba.conf: {expanded_path}")
                                return expanded_path
            else:
                # Direct path without wildcards
                if self.ssh.check_file_exists(path_pattern):
                    self._pg_hba_conf_path = path_pattern
                    self.logger.info(f"Found pg_hba.conf: {path_pattern}")
                    return path_pattern
        
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
                if self.ssh.check_directory_exists(data_dir):
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
        
        for path_pattern in search_paths:
            # Expand wildcards to get actual paths
            if '*' in path_pattern:
                expand_result = self.ssh.execute_command(f"sudo ls -d {path_pattern} 2>/dev/null || true")
                if expand_result['exit_code'] == 0 and expand_result['stdout'].strip():
                    expanded_paths = expand_result['stdout'].strip().split('\n')
                    for expanded_path in expanded_paths:
                        expanded_path = expanded_path.strip()
                        if expanded_path:
                            # Check if this looks like a PostgreSQL data directory
                            if self.ssh.check_file_exists(f"{expanded_path}/PG_VERSION"):
                                self._data_directory = expanded_path
                                self.logger.info(f"Found data directory: {expanded_path}")
                                return expanded_path
            else:
                # Direct path without wildcards
                if self.ssh.check_file_exists(f"{path_pattern}/PG_VERSION"):
                    self._data_directory = path_pattern
                    self.logger.info(f"Found data directory: {path_pattern}")
                    return path_pattern
        
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
            # Add timeout to prevent hanging using threading
            import threading
            
            result_container = {'result': None, 'completed': False}
            
            def query_setting():
                try:
                    result = self.system_utils.execute_as_postgres_user(f"psql -t -c 'SHOW {setting};'")
                    result_container['result'] = result
                    result_container['completed'] = True
                except Exception as e:
                    self.logger.debug(f"Error in PostgreSQL query thread: {str(e)}")
                    result_container['completed'] = True
            
            # Start query in separate thread
            query_thread = threading.Thread(target=query_setting)
            query_thread.daemon = True
            query_thread.start()
            
            # Wait for completion with timeout
            query_thread.join(timeout=30)
            
            if result_container['completed'] and result_container['result']:
                result = result_container['result']
                if result['exit_code'] == 0 and result['stdout'].strip():
                    return result['stdout'].strip()
            elif not result_container['completed']:
                self.logger.warning(f"PostgreSQL query for {setting} timed out after 30 seconds")
                
        except Exception as e:
            self.logger.debug(f"Could not query PostgreSQL for setting {setting}: {str(e)}")
        
        # Fallback to reading from file
        self.logger.debug(f"Falling back to reading {setting} from configuration file")
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
    
    def get_pg_hba_entries(self) -> List[Dict[str, str]]:
        """Get current pg_hba.conf entries for external access.
        
        Returns:
            List of dictionaries with entry details
        """
        pg_hba_path = self.find_pg_hba_conf()
        if not pg_hba_path:
            return []
        
        entries = []
        try:
            # Read pg_hba.conf content
            result = self.ssh.execute_command(f"sudo cat {pg_hba_path}")
            if result['exit_code'] != 0:
                return []
            
            lines = result['stdout'].split('\n')
            for line in lines:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Parse host entries
                if line.startswith('host'):
                    parts = line.split()
                    if len(parts) >= 5:
                        entry = {
                            'type': parts[0],
                            'database': parts[1],
                            'user': parts[2],
                            'address': parts[3],
                            'method': parts[4]
                        }
                        # Only include external access entries (not localhost)
                        if entry['address'] not in ['127.0.0.1/32', '::1/128', 'localhost']:
                            entries.append(entry)
            
        except Exception as e:
            self.logger.error(f"Error reading pg_hba.conf: {str(e)}")
        
        return entries
    
    def _rebuild_pg_hba_with_ips(self, pg_hba_path: str, allowed_ips: List[str], auth_method: str = 'scram-sha-256') -> bool:
        """Rebuild pg_hba.conf with only localhost and specified external IPs.
        
        Args:
            pg_hba_path: Path to pg_hba.conf file
            allowed_ips: List of allowed IP addresses/CIDR blocks
            auth_method: Authentication method for external connections
            
        Returns:
            bool: True if rebuild was successful
        """
        try:
            # Read the current file
            read_result = self.ssh.execute_command(f"sudo cat {pg_hba_path}")
            if read_result['exit_code'] != 0:
                self.logger.error(f"Failed to read pg_hba.conf: {read_result.get('stderr', 'Unknown error')}")
                return False
            
            lines = read_result['stdout'].split('\n')
            new_lines = []
            
            # First pass: keep all non-host lines and localhost host lines
            for line in lines:
                stripped = line.strip()
                
                # Keep all non-host lines (comments, empty lines, local, peer, etc.)
                if not stripped.startswith('host'):
                    new_lines.append(line)
                    continue
                
                # For host lines, only keep localhost entries
                parts = stripped.split()
                if len(parts) >= 4:
                    address = parts[3]  # The address field in pg_hba.conf
                    # Only keep if it's exactly a localhost address
                    if address in ['127.0.0.1/32', '127.0.0.1', '::1/128', '::1', 'localhost']:
                        new_lines.append(line)
                # Skip all other host lines (external access rules)
            
            # Second pass: add new external IP rules
            if allowed_ips:
                new_lines.append("# External access rules")
                for ip in allowed_ips:
                    if self._validate_ip_cidr(ip):
                        new_lines.append(f"host    all             all             {ip}                {auth_method}")
                    else:
                        self.logger.warning(f"Invalid IP/CIDR format: {ip}")
            
            # Write the new content using a here-document to avoid file upload issues
            new_content = '\n'.join(new_lines)
            temp_remote_file = '/tmp/pg_hba_new.conf'
            
            # Use cat with here-document to write content directly on remote server
            escaped_content = new_content.replace('"', '\\"').replace('$', '\\$').replace('`', '\\`')
            write_cmd = f'sudo tee {temp_remote_file} > /dev/null << "EOF"\n{escaped_content}\nEOF'
            
            write_result = self.ssh.execute_command(write_cmd)
            if write_result['exit_code'] != 0:
                self.logger.error(f"Failed to write new pg_hba.conf: {write_result.get('stderr', 'Unknown error')}")
                return False
            
            # Replace the original file
            replace_result = self.ssh.execute_command(f"sudo cp {temp_remote_file} {pg_hba_path}")
            if replace_result['exit_code'] != 0:
                self.logger.error(f"Failed to replace pg_hba.conf: {replace_result.get('stderr', 'Unknown error')}")
                return False
            
            # Set proper permissions
            self.ssh.execute_command(f"sudo chown postgres:postgres {pg_hba_path}")
            self.ssh.execute_command(f"sudo chmod 600 {pg_hba_path}")
            
            # Clean up temp file
            self.ssh.execute_command(f"sudo rm -f {temp_remote_file}")
            
            self.logger.info(f"Successfully rebuilt pg_hba.conf with {len(allowed_ips) if allowed_ips else 0} external IP rules")
            return True
            
        except Exception as e:
            self.logger.error(f"Error rebuilding pg_hba.conf: {str(e)}")
            return False
    
    def configure_external_connections(self, allowed_ips: List[str] = None, auth_method: str = 'scram-sha-256') -> Tuple[bool, str, Dict]:
        """Configure PostgreSQL to allow external connections with IP whitelisting.
        
        Args:
            allowed_ips: List of allowed IP addresses/CIDR blocks. If None, allows all IPs (0.0.0.0/0)
            auth_method: Authentication method (scram-sha-256, md5, etc.)
        
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
            
            # Rebuild pg_hba.conf with only the specified IPs
            ips_to_configure = allowed_ips if allowed_ips else ['0.0.0.0/0', '::/0']
            
            if self._rebuild_pg_hba_with_ips(pg_hba_path, ips_to_configure, auth_method):
                changes_made['pg_hba'] = True
                if allowed_ips:
                    self.logger.info(f"Configured external access for {len(allowed_ips)} specific IP(s) with {auth_method}")
                else:
                    self.logger.info(f"Configured external access for all IPs with {auth_method}")
            else:
                return False, "Failed to configure pg_hba.conf with new IP rules", changes_made
        
        # Restart PostgreSQL if changes were made
        if any(changes_made.values()):
            self.logger.info("Configuration changes made, restarting PostgreSQL...")
            success, message = self.system_utils.restart_service('postgresql')
            if not success:
                return False, f"Configuration updated but PostgreSQL restart failed: {message}", changes_made
        
        return True, "PostgreSQL configured for external connections", changes_made
    
    def _validate_ip_cidr(self, ip_cidr: str) -> bool:
        """Validate IP address or CIDR block format.
        
        Args:
            ip_cidr: IP address or CIDR block to validate
            
        Returns:
            bool: True if valid format
        """
        import re
        
        # IPv4 CIDR pattern
        ipv4_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(?:\/(?:[0-9]|[1-2][0-9]|3[0-2]))?$'
        
        # IPv6 CIDR pattern (simplified)
        ipv6_pattern = r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}(?:\/(?:[0-9]|[1-9][0-9]|1[0-1][0-9]|12[0-8]))?$|^::1(?:\/128)?$|^::(?:\/0)?$'
        
        return bool(re.match(ipv4_pattern, ip_cidr) or re.match(ipv6_pattern, ip_cidr))
    
    def configure_ssl_tls(self, enable_ssl: bool = True, cert_path: str = None, key_path: str = None, auto_generate: bool = True, common_name: str = None, organization: str = None, country: str = None, validity_days: int = 365, key_algorithm: str = "ed25519") -> Tuple[bool, str, Dict]:
        """Configure SSL/TLS for PostgreSQL.
        
        Args:
            enable_ssl: Whether to enable SSL/TLS
            cert_path: Path to SSL certificate file
            key_path: Path to SSL private key file
            auto_generate: Whether to auto-generate self-signed certificates
            common_name: Common name for the certificate (hostname/domain)
            organization: Organization name for the certificate
            country: Two-letter country code
            validity_days: Number of days the certificate will be valid
            key_algorithm: Key algorithm to use (always ed25519 for maximum security)
            
        Returns:
            tuple: (success, message, changes_made)
        """
        changes_made = {
            'ssl_enabled': False,
            'certificates_generated': False,
            'postgresql_conf_updated': False
        }
        
        if not enable_ssl:
            # Disable SSL
            success, message = self.update_postgresql_setting('ssl', 'off')
            if success:
                changes_made['ssl_enabled'] = False
                changes_made['postgresql_conf_updated'] = True
                return True, "SSL/TLS disabled successfully", changes_made
            else:
                return False, f"Failed to disable SSL: {message}", changes_made
        
        # Enable SSL
        data_dir = self.get_data_directory()
        if not data_dir:
            return False, "Could not locate PostgreSQL data directory", changes_made
        
        # Set default certificate paths if not provided
        if not cert_path:
            cert_path = f"{data_dir}/server.crt"
        if not key_path:
            key_path = f"{data_dir}/server.key"
        
        # Generate certificates if auto_generate is True or certificates don't exist
        if auto_generate or not self._check_ssl_certificates(cert_path, key_path):
            success, message = self._generate_ssl_certificates(
                cert_path, key_path, common_name, organization, country, validity_days, key_algorithm
            )
            if success:
                changes_made['certificates_generated'] = True
            else:
                return False, f"Failed to generate SSL certificates: {message}", changes_made
        
        # Update PostgreSQL configuration
        ssl_settings = {
            'ssl': 'on',
            'ssl_cert_file': f"'{cert_path}'",
            'ssl_key_file': f"'{key_path}'",
            'ssl_ca_file': '',
            'ssl_crl_file': ''
        }
        
        for setting, value in ssl_settings.items():
            if value:  # Only set non-empty values
                success, message = self.update_postgresql_setting(setting, value)
                if success:
                    changes_made['postgresql_conf_updated'] = True
                else:
                    return False, f"Failed to update {setting}: {message}", changes_made
        
        changes_made['ssl_enabled'] = True
        
        # Restart PostgreSQL to apply SSL changes
        success, message = self.system_utils.restart_service('postgresql')
        if not success:
            return False, f"SSL configured but PostgreSQL restart failed: {message}", changes_made
        
        # Validate SSL configuration
        ssl_validation_success, ssl_validation_message = self._validate_ssl_configuration(cert_path, key_path)
        if not ssl_validation_success:
            return False, f"SSL configuration validation failed: {ssl_validation_message}", changes_made
        
        changes_made['ssl_validated'] = True
        
        # Check if SSL was automatically enabled during validation
        if "automatically" in ssl_validation_message.lower():
            return True, f"SSL/TLS configured and validated successfully. {ssl_validation_message}", changes_made
        else:
            return True, "SSL/TLS configured and validated successfully", changes_made
    
    def _check_ssl_certificates(self, cert_path: str, key_path: str) -> bool:
        """Check if SSL certificates exist and are valid.
        
        Args:
            cert_path: Path to certificate file
            key_path: Path to private key file
            
        Returns:
            bool: True if certificates exist and are readable
        """
        return (self.ssh.check_file_exists(cert_path) and 
                self.ssh.check_file_exists(key_path))
    
    def _validate_ssl_configuration(self, cert_path: str, key_path: str) -> Tuple[bool, str]:
        """Validate SSL configuration for PostgreSQL.
        
        Args:
            cert_path: Path to certificate file
            key_path: Path to private key file
            
        Returns:
            tuple: (success, message)
        """
        try:
            self.logger.info("Starting SSL configuration validation...")
            
            # Check if certificate files exist and are readable
            self.logger.info(f"Checking if certificate file exists: {cert_path}")
            if not self.ssh.check_file_exists(cert_path):
                return False, f"Certificate file not found: {cert_path}"
            
            self.logger.info(f"Checking if private key file exists: {key_path}")
            if not self.ssh.check_file_exists(key_path):
                return False, f"Private key file not found: {key_path}"
            
            # Check certificate file permissions (should be readable by postgres user)
            self.logger.info("Checking certificate file permissions...")
            cert_perms_cmd = f"ls -la {cert_path}"
            cert_result = self.ssh.execute_command(cert_perms_cmd)
            if cert_result['exit_code'] != 0:
                return False, f"Failed to check certificate file permissions: {cert_result['stderr']}"
            
            # Check key file permissions (should be 600 or similar, readable only by postgres user)
            self.logger.info("Checking private key file permissions...")
            key_perms_cmd = f"ls -la {key_path}"
            key_result = self.ssh.execute_command(key_perms_cmd)
            if key_result['exit_code'] != 0:
                return False, f"Failed to check private key file permissions: {key_result['stderr']}"
            
            # Verify PostgreSQL can read the SSL configuration
            self.logger.info("Checking current PostgreSQL SSL status...")
            ssl_check_cmd = "timeout 30 sudo -u postgres psql -c 'SHOW ssl;' -t"
            self.logger.info(f"Executing SSL status check command: {ssl_check_cmd}")
            result = self.ssh.execute_command(ssl_check_cmd)
            if result['exit_code'] != 0:
                self.logger.warning(f"Failed to check PostgreSQL SSL status: {result.get('stderr', 'Unknown error')}")
                # Try alternative method
                self.logger.info("Trying alternative SSL status check method...")
                alt_cmd = "sudo -u postgres psql -c 'SELECT setting FROM pg_settings WHERE name = \'ssl\';' -t"
                result = self.ssh.execute_command(alt_cmd)
                if result['exit_code'] != 0:
                    return False, f"Failed to check PostgreSQL SSL status: {result.get('stderr', 'Unknown error')}"
            
            ssl_status = result['stdout']
            self.logger.info(f"Current SSL status: {ssl_status.strip()}")
            
            # If SSL is not enabled, try to enable it automatically
            if 'on' not in ssl_status.lower():
                self.logger.info("SSL not enabled, attempting to enable automatically...")
                
                # Check if SSL certificate and key paths are set in configuration
                self.logger.info("Checking current SSL certificate and key file settings...")
                try:
                    self.logger.info("Querying ssl_cert_file setting...")
                    cert_file_setting = self.get_postgresql_setting('ssl_cert_file')
                    self.logger.info("Querying ssl_key_file setting...")
                    key_file_setting = self.get_postgresql_setting('ssl_key_file')
                    
                    self.logger.info(f"Current SSL cert file setting: {cert_file_setting}")
                    self.logger.info(f"Current SSL key file setting: {key_file_setting}")
                except Exception as e:
                    self.logger.warning(f"Could not query SSL settings, proceeding with defaults: {str(e)}")
                    cert_file_setting = None
                    key_file_setting = None
                
                # If SSL cert/key files are not set, set them to the provided paths
                self.logger.info("Checking if SSL certificate path needs to be set...")
                if not cert_file_setting or cert_file_setting.strip("'\"")=="":
                    self.logger.info(f"Setting SSL certificate path to: {cert_path}")
                    cert_success, cert_message = self.update_postgresql_setting('ssl_cert_file', f"'{cert_path}'")
                    if not cert_success:
                        return False, f"Failed to set SSL certificate path: {cert_message}"
                    self.logger.info(f"Successfully set SSL certificate path to: {cert_path}")
                
                self.logger.info("Checking if SSL key path needs to be set...")
                if not key_file_setting or key_file_setting.strip("'\"")=="":
                    self.logger.info(f"Setting SSL key path to: {key_path}")
                    key_success, key_message = self.update_postgresql_setting('ssl_key_file', f"'{key_path}'")
                    if not key_success:
                        return False, f"Failed to set SSL key path: {key_message}"
                    self.logger.info(f"Successfully set SSL key path to: {key_path}")
                
                # Enable SSL in configuration
                self.logger.info("Enabling SSL in PostgreSQL configuration...")
                ssl_enable_success, ssl_enable_message = self.update_postgresql_setting('ssl', 'on')
                if not ssl_enable_success:
                    return False, f"Failed to enable SSL automatically: {ssl_enable_message}"
                
                self.logger.info("SSL enabled in configuration, restarting PostgreSQL...")
                
                # Restart PostgreSQL to apply SSL setting
                self.logger.info("Restarting PostgreSQL service to apply SSL settings...")
                restart_success, restart_message = self.system_utils.restart_service('postgresql')
                if not restart_success:
                    return False, f"Failed to restart PostgreSQL after enabling SSL: {restart_message}"
                
                self.logger.info("PostgreSQL restarted successfully, waiting for service to stabilize...")
                
                # Wait a moment for PostgreSQL to fully start
                import time
                time.sleep(2)
                
                # Verify SSL is now enabled
                self.logger.info("Verifying SSL status after PostgreSQL restart...")
                self.logger.info(f"Executing post-restart SSL check: {ssl_check_cmd}")
                result = self.ssh.execute_command(ssl_check_cmd)
                if result['exit_code'] != 0:
                    self.logger.warning(f"Failed to check SSL status after restart: {result.get('stderr', 'Unknown error')}")
                    # Try alternative method
                    self.logger.info("Trying alternative SSL status check after restart...")
                    alt_cmd = "timeout 30 sudo -u postgres psql -c 'SELECT setting FROM pg_settings WHERE name = \'ssl\';' -t"
                    result = self.ssh.execute_command(alt_cmd)
                    if result['exit_code'] != 0:
                        return False, f"Failed to check SSL status after restart: {result.get('stderr', 'Unknown error')}"
                
                ssl_status = result['stdout']
                self.logger.info(f"SSL status after restart: {ssl_status.strip()}")
                
                if 'on' not in ssl_status.lower():
                    # Get more detailed error information
                    log_cmd = "sudo tail -20 /var/log/postgresql/postgresql-*.log | grep -i ssl"
                    log_result = self.ssh.execute_command(log_cmd)
                    error_details = f"SSL status: {ssl_status.strip()}"
                    if log_result['exit_code'] == 0 and log_result['stdout']:
                        error_details += f"\nPostgreSQL logs: {log_result['stdout']}"
                    return False, f"Failed to automatically enable PostgreSQL SSL. {error_details}"
                
                self.logger.info("SSL automatically enabled and validated successfully")
                return True, "SSL configuration validated successfully (SSL was automatically enabled)"
            
            # Test SSL connection
            self.logger.info("Testing SSL connection...")
            ssl_test_cmd = "timeout 30 sudo -u postgres psql -c 'SELECT version();' 'sslmode=require'"
            self.logger.info(f"Executing SSL connection test: {ssl_test_cmd}")
            result = self.ssh.execute_command(ssl_test_cmd)
            if result['exit_code'] != 0:
                self.logger.warning(f"SSL connection test failed: {result.get('stderr', 'Unknown error')}")
                # Try a simpler SSL test
                self.logger.info("Trying simpler SSL connection test...")
                simple_ssl_cmd = "timeout 30 sudo -u postgres psql -c '\\conninfo' 'sslmode=prefer'"
                self.logger.info(f"Executing simple SSL test: {simple_ssl_cmd}")
                result = self.ssh.execute_command(simple_ssl_cmd)
                if result['exit_code'] != 0:
                    return False, f"SSL connection test failed - PostgreSQL may not be accepting SSL connections: {result.get('stderr', 'Unknown error')}"
            
            self.logger.info("SSL configuration validated successfully")
            return True, "SSL configuration validated successfully"
            
        except Exception as e:
            self.logger.error(f"Exception during SSL validation: {str(e)}")
            return False, f"SSL validation error: {str(e)}"
    
    def _generate_ssl_certificates(self, cert_path: str, key_path: str, common_name: str = None, 
                                 organization: str = None, country: str = None, 
                                 validity_days: int = 365, key_algorithm: str = "ed25519") -> Tuple[bool, str]:
        """Generate self-signed SSL certificates for PostgreSQL.
        
        Args:
            cert_path: Path where certificate should be created
            key_path: Path where private key should be created
            common_name: Common name for the certificate (hostname/domain)
            organization: Organization name for the certificate
            country: Two-letter country code
            validity_days: Number of days the certificate will be valid
            key_algorithm: Key algorithm to use (always ed25519 for maximum security)
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Generating self-signed SSL certificates for PostgreSQL...")
        
        # Set defaults if not provided
        if not common_name:
            hostname_result = self.ssh.execute_command("hostname -f")
            common_name = hostname_result['stdout'].strip() if hostname_result['exit_code'] == 0 else 'localhost'
        
        if not organization:
            organization = "PostgreSQL Server"
        
        if not country:
            country = "US"
        
        # Validate inputs
        if not (30 <= validity_days <= 3650):
            validity_days = 365
        
        if len(country) != 2:
            country = "US"
        
        # Generate Ed25519 private key (safest algorithm)
        key_cmd = f"sudo openssl genpkey -algorithm Ed25519 -out {key_path}"
            
        key_result = self.ssh.execute_command(key_cmd)
        
        if key_result['exit_code'] != 0:
            return False, f"Failed to generate private key: {key_result.get('stderr', 'Unknown error')}"
        
        # Generate certificate with custom parameters
        subject = f"/C={country}/O={organization}/CN={common_name}"
        cert_cmd = f"sudo openssl req -new -x509 -key {key_path} -out {cert_path} -days {validity_days} -subj '{subject}'"
        cert_result = self.ssh.execute_command(cert_cmd)
        
        if cert_result['exit_code'] != 0:
            return False, f"Failed to generate certificate: {cert_result.get('stderr', 'Unknown error')}"
        
        # Validate the generated certificate
        validate_cmd = f"sudo openssl x509 -in {cert_path} -text -noout"
        validate_result = self.ssh.execute_command(validate_cmd)
        
        if validate_result['exit_code'] != 0:
            return False, f"Generated certificate validation failed: {validate_result.get('stderr', 'Unknown error')}"
        
        # Set proper ownership and permissions
        chown_result = self.ssh.execute_command(f"sudo chown postgres:postgres {cert_path} {key_path}")
        chmod_cert = self.ssh.execute_command(f"sudo chmod 644 {cert_path}")
        chmod_key = self.ssh.execute_command(f"sudo chmod 600 {key_path}")
        
        if chown_result['exit_code'] != 0 or chmod_cert['exit_code'] != 0 or chmod_key['exit_code'] != 0:
            self.logger.warning("SSL certificates generated but failed to set proper permissions")
        
        return True, f"SSL certificates generated successfully with Ed25519 (safest algorithm), valid for {validity_days} days"
    
    def get_ssl_status(self) -> Dict[str, Any]:
        """Get current SSL/TLS configuration status.
        
        Returns:
            dict: SSL status information
        """
        status = {
            'ssl_enabled': False,
            'ssl_cert_file': None,
            'ssl_key_file': None,
            'certificates_exist': False,
            'ssl_settings': {}
        }
        
        # Check SSL settings
        ssl_settings = ['ssl', 'ssl_cert_file', 'ssl_key_file', 'ssl_ca_file', 'ssl_crl_file']
        for setting in ssl_settings:
            value = self.get_postgresql_setting(setting)
            status['ssl_settings'][setting] = value
            
            if setting == 'ssl' and value == 'on':
                status['ssl_enabled'] = True
            elif setting == 'ssl_cert_file' and value:
                status['ssl_cert_file'] = value.strip("'\"")
            elif setting == 'ssl_key_file' and value:
                status['ssl_key_file'] = value.strip("'\"")
        
        # Check if certificate files exist
        if status['ssl_cert_file'] and status['ssl_key_file']:
            status['certificates_exist'] = self._check_ssl_certificates(
                status['ssl_cert_file'], 
                status['ssl_key_file']
            )
        
        return status
    
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
        remove_cmd = rf"sudo sed -i '/^[ \t]*listen_addresses[ \t]*=[ \t]*\*[ \t]*$/d' {postgresql_conf}"
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