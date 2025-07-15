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
            success, message = self._generate_ssl_certificates(cert_path, key_path)
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
        
        return True, "SSL/TLS configured successfully", changes_made
    
    def _check_ssl_certificates(self, cert_path: str, key_path: str) -> bool:
        """Check if SSL certificates exist and are valid.
        
        Args:
            cert_path: Path to certificate file
            key_path: Path to private key file
            
        Returns:
            bool: True if certificates exist and are readable
        """
        cert_check = self.ssh.execute_command(f"sudo test -f {cert_path} && sudo test -r {cert_path} && echo 'exists'")
        key_check = self.ssh.execute_command(f"sudo test -f {key_path} && sudo test -r {key_path} && echo 'exists'")
        
        return 'exists' in cert_check['stdout'] and 'exists' in key_check['stdout']
    
    def _generate_ssl_certificates(self, cert_path: str, key_path: str) -> Tuple[bool, str]:
        """Generate self-signed SSL certificates for PostgreSQL.
        
        Args:
            cert_path: Path where certificate should be created
            key_path: Path where private key should be created
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info("Generating self-signed SSL certificates for PostgreSQL...")
        
        # Get server hostname for certificate
        hostname_result = self.ssh.execute_command("hostname -f")
        hostname = hostname_result['stdout'].strip() if hostname_result['exit_code'] == 0 else 'localhost'
        
        # Generate private key
        key_cmd = f"sudo openssl genrsa -out {key_path} 2048"
        key_result = self.ssh.execute_command(key_cmd)
        
        if key_result['exit_code'] != 0:
            return False, f"Failed to generate private key: {key_result.get('stderr', 'Unknown error')}"
        
        # Generate certificate
        subject = f"/C=US/ST=State/L=City/O=Organization/OU=OrgUnit/CN={hostname}"
        cert_cmd = f"sudo openssl req -new -x509 -key {key_path} -out {cert_path} -days 365 -subj '{subject}'"
        cert_result = self.ssh.execute_command(cert_cmd)
        
        if cert_result['exit_code'] != 0:
            return False, f"Failed to generate certificate: {cert_result.get('stderr', 'Unknown error')}"
        
        # Set proper ownership and permissions
        chown_result = self.ssh.execute_command(f"sudo chown postgres:postgres {cert_path} {key_path}")
        chmod_cert = self.ssh.execute_command(f"sudo chmod 644 {cert_path}")
        chmod_key = self.ssh.execute_command(f"sudo chmod 600 {key_path}")
        
        if chown_result['exit_code'] != 0 or chmod_cert['exit_code'] != 0 or chmod_key['exit_code'] != 0:
            self.logger.warning("SSL certificates generated but failed to set proper permissions")
        
        return True, "SSL certificates generated successfully"
    
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