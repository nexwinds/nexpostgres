import logging
import re
from app.utils.ssh_manager import SSHManager
import time
import os

class PostgresManager:
    def __init__(self, ssh_manager):
        self.ssh = ssh_manager
        self.logger = logging.getLogger('nexpostgres.postgres')
    
    def check_postgres_installed(self):
        """Check if PostgreSQL is installed on the remote server"""
        result = self.ssh.execute_command("which psql")
        return result['exit_code'] == 0
    
    def get_postgres_version(self):
        """Get PostgreSQL version installed on the remote server"""
        if not self.check_postgres_installed():
            return None
        
        # Try multiple methods in order of reliability
        methods = [
            # Method 1: Use psql as postgres user
            "sudo -u postgres psql --version",
            # Method 2: Direct version query
            "sudo -u postgres psql -c 'SHOW server_version;' -t",
            # Method 3: Check for pg_lsclusters (Debian/Ubuntu)
            "command -v pg_lsclusters > /dev/null && pg_lsclusters",
            # Method 4: Check data directory structure
            "ls -la /var/lib/postgresql/",
            # Method 5: Check configuration directory
            "ls -la /etc/postgresql/",
            # Method 6: Check package info
            "dpkg -l | grep postgresql | grep -v pgbackrest",
            "rpm -qa | grep postgresql | grep -v pgbackrest"
        ]
        
        for cmd in methods:
            result = self.ssh.execute_command(cmd)
            if result['exit_code'] == 0 and result['stdout'].strip():
                # Extract version from output
                if "psql (PostgreSQL)" in result['stdout']:
                    version_match = re.search(r'(\d+\.\d+)', result['stdout'])
                    if version_match:
                        return version_match.group(1)
                elif "server_version" in cmd:
                    version_match = re.search(r'(\d+\.\d+(?:\.\d+)?)', result['stdout'])
                    if version_match:
                        return version_match.group(1)
                elif "pg_lsclusters" in cmd:
                    cluster_match = re.search(r'(\d+\.\d+)\s+\w+', result['stdout'])
                    if cluster_match:
                        return cluster_match.group(1)
                else:
                    # Generic version pattern extraction
                    version_match = re.search(r'(\d+\.?\d*)', result['stdout'])
                    if version_match:
                        return version_match.group(1)
        
        self.logger.error("Failed to determine PostgreSQL version")
        return None
    
    def check_latest_postgres_version(self):
        """Check if the installed PostgreSQL version is the latest available
        
        Returns:
            tuple: (is_latest, installed_version, latest_version)
        """
        # Get installed version
        installed_version = self.get_postgres_version()
        if not installed_version:
            return False, None, None
            
        # Detect OS type for appropriate package manager
        result = self.ssh.execute_command("cat /etc/os-release")
        is_debian_based = "debian" in result['stdout'].lower() or "ubuntu" in result['stdout'].lower()
        is_rhel_based = "rhel" in result['stdout'].lower() or "centos" in result['stdout'].lower() or "fedora" in result['stdout'].lower() or "rocky" in result['stdout'].lower() or "alma" in result['stdout'].lower()
        
        latest_version = None
        
        if is_debian_based:
            # Update package lists
            self.ssh.execute_command("sudo apt-get update")
            
            # Check for latest PostgreSQL version in apt
            find_latest = self.ssh.execute_command("apt-cache search 'postgresql-[0-9]+$' | grep -oP 'postgresql-\\K[0-9]+' | sort -V | tail -1")
            if find_latest['exit_code'] == 0 and find_latest['stdout'].strip():
                latest_version = find_latest['stdout'].strip()
        elif is_rhel_based:
            # Check for latest PostgreSQL version in dnf
            find_latest = self.ssh.execute_command("sudo dnf list postgresql*-server | grep -v 'client\\|devel\\|docs\\|libs' | grep -oP 'postgresql\\K[0-9]+' | sort -V | tail -1")
            if find_latest['exit_code'] == 0 and find_latest['stdout'].strip():
                latest_version = find_latest['stdout'].strip()
        
        # If we couldn't determine the latest version, assume installed is latest
        if not latest_version:
            return True, installed_version, installed_version
            
        # Compare versions (simple numeric comparison)
        try:
            installed_major = int(installed_version.split('.')[0])
            latest_major = int(latest_version)
            is_latest = installed_major >= latest_major
            
            return is_latest, installed_version, latest_version
        except (ValueError, IndexError):
            # If we can't parse the versions, assume installed is latest
            return True, installed_version, installed_version
    
    def get_data_directory(self):
        """Get PostgreSQL data directory from the remote server"""
        # Try methods in order of reliability
        pg_version = self.get_postgres_version()
        
        # Method 1: Direct query (most reliable)
        result = self.ssh.execute_command("sudo -u postgres psql -t -c 'SHOW data_directory;'")
        if result['exit_code'] == 0 and result['stdout'].strip():
            data_dir = result['stdout'].strip()
            self.logger.info(f"PostgreSQL data directory (from psql): {data_dir}")
            return data_dir
        
        # Method 2: Check using pg_lsclusters (Debian/Ubuntu)
        result = self.ssh.execute_command("command -v pg_lsclusters > /dev/null && pg_lsclusters")
        if result['exit_code'] == 0 and result['stdout'].strip():
            for line in result['stdout'].strip().split('\n'):
                if line and not line.startswith("Ver"):
                    parts = line.split()
                    if len(parts) >= 6:
                        data_dir = parts[5]
                        return data_dir
        
        # Method 3: Check standard locations based on version
        if pg_version:
            possible_dirs = []
            
            if pg_version.startswith('16'):
                possible_dirs = [
                    f"/var/lib/postgresql/{pg_version}/main",
                    f"/var/lib/postgresql/16/main",
                    f"/etc/postgresql/{pg_version}/main",
                    f"/etc/postgresql/16/main"
                ]
            else:
                possible_dirs = [
                    f"/var/lib/postgresql/{pg_version}/main",
                    f"/var/lib/postgresql/{pg_version}/data",
                    f"/var/lib/pgsql/{pg_version}/data"
                ]
                
            for dir_path in possible_dirs:
                check = self.ssh.execute_command(f"sudo test -d {dir_path} && echo 'exists'")
                if check['exit_code'] == 0 and 'exists' in check['stdout']:
                    return dir_path
        
        # Method 4: Check common locations regardless of version
        common_dirs = [
            "/var/lib/postgresql/data",
            "/var/lib/postgresql/*/main",
            "/var/lib/postgresql/*/data",
            "/var/lib/pgsql/data",
            "/var/lib/pgsql/*/data"
        ]
        
        for dir_pattern in common_dirs:
            if "*" in dir_pattern:
                check = self.ssh.execute_command(f"ls -d {dir_pattern} 2>/dev/null | head -1")
                if check['exit_code'] == 0 and check['stdout'].strip():
                    data_dir = check['stdout'].strip()
                    check = self.ssh.execute_command(f"sudo test -d {data_dir} && echo 'exists'")
                    if check['exit_code'] == 0 and 'exists' in check['stdout']:
                        return data_dir
            else:
                check = self.ssh.execute_command(f"sudo test -d {dir_pattern} && echo 'exists'")
                if check['exit_code'] == 0 and 'exists' in check['stdout']:
                    return dir_pattern
        
        # Method 5: Check process information
        result = self.ssh.execute_command("ps aux | grep postgres | grep -- '-D'")
        if result['exit_code'] == 0 and result['stdout'].strip():
            dir_match = re.search(r'-D\s+([^\s]+)', result['stdout'])
            if dir_match:
                return dir_match.group(1)
                
        self.logger.error("Could not determine PostgreSQL data directory")
        return None
    
    def list_databases(self):
        """List all PostgreSQL databases on the remote server"""
        self.logger.info("Listing PostgreSQL databases")
        
        if not self.check_postgres_installed():
            self.logger.error("PostgreSQL is not installed")
            return []
        
        # Get PostgreSQL version for better diagnosis
        pg_version = self.get_postgres_version()
        if pg_version:
            self.logger.info(f"Detected PostgreSQL version: {pg_version}")
        
        # Try methods in order of reliability and complexity
        methods = [
            # Method 1: Direct psql query with size info (preferred)
            "sudo -u postgres psql -t -c \"SELECT datname, pg_size_pretty(pg_database_size(datname)), datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"",
            
            # Method 2: Direct query to postgres database
            "sudo -u postgres psql -d postgres -c 'SELECT datname, usename as owner FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;' -t",
            
            # Method 3: PostgreSQL CLI list databases
            "sudo -u postgres psql -t -c '\\l'",
            
            # Method 4: Socket connection (for PostgreSQL 16.x issues)
            "sudo -u postgres psql -h /var/run/postgresql -d postgres -t -c \"SELECT datname, usename as owner FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;\""
        ]
        
        # Special handling for PostgreSQL 16.x
        if pg_version and pg_version.startswith('16'):
            # Try setting data directory explicitly
            data_dir = self.get_data_directory()
            if data_dir:
                methods.insert(0, f"sudo -u postgres bash -c 'export PGDATA={data_dir}; psql -t -c \"SELECT datname, datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"'")
        
        for cmd in methods:
            result = self.ssh.execute_command(cmd)
            if result['exit_code'] == 0 and result['stdout'].strip():
                # Parse the output based on the command used
                databases = []
                for line in result['stdout'].strip().split('\n'):
                    if not line.strip():
                        continue
                        
                    parts = line.strip().split('|')
                    db_name = parts[0].strip() if len(parts) >= 1 else ""
                    
                    # Skip system databases
                    if db_name in ['postgres', 'template0', 'template1']:
                        continue
                    
                    db_size = parts[1].strip() if len(parts) >= 3 else 'Unknown'
                    db_owner = parts[-1].strip() if len(parts) >= 2 else 'postgres'
                    
                    databases.append({
                        'name': db_name,
                        'size': db_size,
                        'owner': db_owner
                    })
                
                if databases:
                    return databases
        
        # Fallback: If PostgreSQL is installed but we can't list databases
        self.logger.warning("All database listing methods failed. Using placeholder.")
        return [{
            'name': 'postgres',
            'size': 'Unknown',
            'owner': 'postgres'
        }]

    def install_postgres(self):
        """Install PostgreSQL on the remote server"""
        self.logger.info("Installing PostgreSQL")
        
        # Detect OS type
        result = self.ssh.execute_command("cat /etc/os-release")
        is_debian_based = "debian" in result['stdout'].lower() or "ubuntu" in result['stdout'].lower()
        is_rhel_based = "rhel" in result['stdout'].lower() or "centos" in result['stdout'].lower() or "fedora" in result['stdout'].lower() or "rocky" in result['stdout'].lower() or "alma" in result['stdout'].lower()
        
        if is_debian_based:
            # Install PostgreSQL on Debian/Ubuntu
            self.ssh.execute_command("sudo apt-get update")
            self.ssh.execute_command("sudo apt-get install -y curl ca-certificates gnupg lsb-release")
            self.ssh.execute_command("curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg")
            
            # Get OS version
            lsb_result = self.ssh.execute_command("lsb_release -cs")
            codename = lsb_result['stdout'].strip() if lsb_result['exit_code'] == 0 else "focal"
            
            # Add repository for latest PostgreSQL
            repo_cmd = f'echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt/ {codename}-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list'
            self.ssh.execute_command(repo_cmd)
            self.ssh.execute_command("sudo apt-get update")
            
            # Find and install latest version
            find_latest = self.ssh.execute_command("apt-cache search 'postgresql-[0-9]+$' | grep -oP 'postgresql-\\K[0-9]+' | sort -V | tail -1")
            if find_latest['exit_code'] == 0 and find_latest['stdout'].strip():
                pg_version = find_latest['stdout'].strip()
                result = self.ssh.execute_command(f"sudo apt-get install -y postgresql-{pg_version} postgresql-contrib-{pg_version}")
            else:
                result = self.ssh.execute_command("sudo apt-get install -y postgresql postgresql-contrib")
                
        elif is_rhel_based:
            # Install PostgreSQL on RHEL/CentOS
            self.ssh.execute_command("sudo dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-$(rpm -E %rhel)-x86_64/pgdg-redhat-repo-latest.noarch.rpm")
            
            find_latest = self.ssh.execute_command("sudo dnf list postgresql*-server | grep -v 'client\\|devel\\|docs\\|libs' | grep -oP 'postgresql\\K[0-9]+' | sort -V | tail -1")
            if find_latest['exit_code'] == 0 and find_latest['stdout'].strip():
                pg_version = find_latest['stdout'].strip()
                result = self.ssh.execute_command(f"sudo dnf install -y postgresql{pg_version}-server postgresql{pg_version}-contrib")
                self.ssh.execute_command(f"sudo /usr/pgsql-{pg_version}/bin/postgresql-{pg_version}-setup initdb")
            else:
                result = self.ssh.execute_command("sudo dnf install -y postgresql-server postgresql-contrib")
                self.ssh.execute_command("sudo postgresql-setup initdb")
        else:
            # Generic fallback
            result = self.ssh.execute_command("sudo apt-get update && sudo apt-get install -y postgresql postgresql-contrib")
        
        if result['exit_code'] != 0:
            self.logger.error(f"Failed to install PostgreSQL: {result['stderr']}")
            return False
        
        # Enable and start service
        self.ssh.execute_command("sudo systemctl enable postgresql")
        self.ssh.execute_command("sudo systemctl start postgresql")
        
        # Verify installation
        is_installed = self.check_postgres_installed()
        if is_installed:
            pg_version = self.get_postgres_version()
            self.logger.info(f"PostgreSQL {pg_version} installed successfully")
        
        return is_installed
    
    def check_pgbackrest_installed(self):
        """Check if pgBackRest is installed on the remote server"""
        result = self.ssh.execute_command("which pgbackrest")
        return result['exit_code'] == 0
    
    def install_pgbackrest(self):
        """Install pgBackRest on the remote server"""
        self.logger.info("Installing pgBackRest")
        result = self.ssh.execute_command("sudo apt-get update && sudo apt-get install -y pgbackrest")
        return result['exit_code'] == 0 and self.check_pgbackrest_installed()
    
    def setup_pgbackrest_config(self, db_name, s3_bucket, s3_region, s3_access_key, s3_secret_key):
        """Set up pgBackRest configuration for S3 backups"""
        self.logger.info(f"Setting up pgBackRest configuration for database {db_name}")
        
        # Create required directories with proper permissions
        dirs = ["/etc/pgbackrest", "/var/log/pgbackrest", "/var/lib/pgbackrest"]
        for dir_path in dirs:
            self.ssh.execute_command(f"sudo mkdir -p {dir_path}")
            self.ssh.execute_command(f"sudo chmod 750 {dir_path}")
            self.ssh.execute_command(f"sudo chown -R postgres:postgres {dir_path}")
        
        # Explicitly create conf.d directory
        self.ssh.execute_command("sudo mkdir -p /etc/pgbackrest/conf.d")
        self.ssh.execute_command("sudo chmod 750 /etc/pgbackrest/conf.d")
        self.ssh.execute_command("sudo chown -R postgres:postgres /etc/pgbackrest/conf.d")
        
        # Create pgBackRest main configuration
        config_content = f"""[global]
# Path where backups and archives are stored
repo1-path=/var/lib/pgbackrest

# Configuration include path
config-include-path=/etc/pgbackrest/conf.d

# S3 settings
repo1-type=s3
repo1-s3-bucket={s3_bucket}
repo1-s3-endpoint=s3.{s3_region}.amazonaws.com
repo1-s3-region={s3_region}
repo1-s3-key={s3_access_key}
repo1-s3-key-secret={s3_secret_key}

# Backup retention policy
repo1-retention-full=7
repo1-retention-full-type=count

# Memory and process settings
process-max=4

# Log settings
log-level-console=info
log-level-file=debug
log-path=/var/log/pgbackrest

# Performance settings
compress-level=6
compress=y
delta=y
start-fast=y
"""
        
        # Create stanza-specific configuration
        data_dir = self.get_data_directory() or "/var/lib/postgresql/data"
        stanza_content = f"""[{db_name}]
# PostgreSQL connection settings
pg1-path={data_dir}
pg1-port=5432
pg1-socket-path=/var/run/postgresql
pg1-user=postgres
"""
        
        # Write main configuration directly to server
        main_config_file = "/etc/pgbackrest/pgbackrest.conf"
        self.ssh.execute_command(f"echo '{config_content}' | sudo tee {main_config_file} > /dev/null")
        self.ssh.execute_command(f"sudo chmod 640 {main_config_file}")
        self.ssh.execute_command(f"sudo chown postgres:postgres {main_config_file}")
        
        # Write stanza configuration directly to server
        stanza_file = f"/etc/pgbackrest/conf.d/{db_name}.conf"
        self.ssh.execute_command(f"echo '{stanza_content}' | sudo tee {stanza_file} > /dev/null")
        self.ssh.execute_command(f"sudo chmod 640 {stanza_file}")
        self.ssh.execute_command(f"sudo chown postgres:postgres {stanza_file}")
        
        # Update PostgreSQL configuration using ALTER SYSTEM when possible
        self.logger.info("Updating PostgreSQL configuration")
        archive_command = f"pgbackrest --stanza={db_name} archive-push %p"
        
        # Try ALTER SYSTEM first
        alter_system_cmds = [
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET archive_mode = 'on';\"",
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET wal_level = 'replica';\"",
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET archive_command = '{archive_command}';\"",
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET max_wal_senders = '10';\""
        ]
        
        alter_system_failed = False
        for cmd in alter_system_cmds:
            result = self.ssh.execute_command(cmd)
            if result['exit_code'] != 0:
                alter_system_failed = True
                self.logger.warning(f"ALTER SYSTEM command failed: {result['stderr']}")
                break
        
        # Fall back to direct file editing if ALTER SYSTEM fails
        if alter_system_failed:
            self._update_postgres_config(db_name)
        
        # Reload configuration
        self.ssh.execute_command("sudo -u postgres psql -c \"SELECT pg_reload_conf();\"")
        
        # Check if a restart is needed (for archive_mode changes)
        archive_mode_check = self.ssh.execute_command("sudo -u postgres psql -t -c 'SHOW archive_mode;'")
        if archive_mode_check['exit_code'] == 0 and 'on' not in archive_mode_check['stdout'].lower():
            self.logger.info("Restarting PostgreSQL for archive_mode changes")
            self._restart_postgres()
        
        # Create the stanza
        self.logger.info(f"Creating pgBackRest stanza for {db_name}")
        stanza_result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --log-level-console=detail stanza-create --force")
        if stanza_result['exit_code'] != 0:
            self.logger.error(f"Failed to create stanza: {stanza_result['stderr']}")
            return False
        
        # Check configuration
        self.logger.info("Verifying pgBackRest configuration")
        check_result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --log-level-console=detail check")
        if check_result['exit_code'] != 0:
            self.logger.error(f"pgBackRest configuration check failed: {check_result['stderr'] or check_result['stdout']}")
            return False
        
        self.logger.info("pgBackRest configuration completed successfully")
        return True
    
    def update_pgbackrest_config(self, db_name):
        """Update pgBackRest configuration for an existing stanza"""
        self.logger.info(f"Updating pgBackRest configuration for {db_name}")
        
        # Check if main config directory and file exist
        check_config_dir = self.ssh.execute_command("sudo test -d /etc/pgbackrest && echo 'exists'")
        if check_config_dir['exit_code'] != 0 or 'exists' not in check_config_dir['stdout']:
            self.logger.warning("pgBackRest configuration directory not found. Creating it.")
            self.ssh.execute_command("sudo mkdir -p /etc/pgbackrest")
            self.ssh.execute_command("sudo chmod 750 /etc/pgbackrest")
            self.ssh.execute_command("sudo chown postgres:postgres /etc/pgbackrest")
        
        # Create conf.d directory if it doesn't exist
        check_conf_d = self.ssh.execute_command("sudo test -d /etc/pgbackrest/conf.d && echo 'exists'")
        if check_conf_d['exit_code'] != 0 or 'exists' not in check_conf_d['stdout']:
            self.logger.info("Creating pgBackRest conf.d directory")
            self.ssh.execute_command("sudo mkdir -p /etc/pgbackrest/conf.d")
            self.ssh.execute_command("sudo chmod 750 /etc/pgbackrest/conf.d")
            self.ssh.execute_command("sudo chown postgres:postgres /etc/pgbackrest/conf.d")
        
        # Check for main configuration file
        check_main_conf = self.ssh.execute_command("sudo test -f /etc/pgbackrest/pgbackrest.conf && echo 'exists'")
        if check_main_conf['exit_code'] != 0 or 'exists' not in check_main_conf['stdout']:
            self.logger.info("Creating main pgBackRest configuration file")
            
            # Basic configuration without S3 settings
            main_config = """[global]
# Path where backups and archives are stored
repo1-path=/var/lib/pgbackrest

# Configuration include path
config-include-path=/etc/pgbackrest/conf.d

# Backup retention policy
repo1-retention-full=7
repo1-retention-full-type=count

# Log settings
log-level-console=info
log-level-file=debug
log-path=/var/log/pgbackrest

# Performance settings
compress=y
delta=y
"""
            
            # Check if S3 settings exist in database configuration
            s3_check = self.ssh.execute_command("sudo grep -q 'repo1-s3-bucket' /etc/pgbackrest/pgbackrest.conf || echo 'missing'")
            if s3_check['exit_code'] == 0 and 'missing' in s3_check['stdout']:
                # Try to find S3 settings from existing stanza info
                s3_info = self.ssh.execute_command(f"sudo -u postgres pgbackrest info || echo 'no s3 info'")
                if s3_info['exit_code'] == 0 and 'repo1-s3-bucket' in s3_info['stdout']:
                    self.logger.info("Adding S3 settings to main configuration from existing stanza info")
                    
                    # Extract S3 settings from info output
                    s3_bucket = None
                    s3_region = None
                    
                    bucket_match = re.search(r'repo1-s3-bucket: ([^\s]+)', s3_info['stdout'])
                    if bucket_match:
                        s3_bucket = bucket_match.group(1)
                    
                    region_match = re.search(r'repo1-s3-region: ([^\s]+)', s3_info['stdout'])
                    if region_match:
                        s3_region = region_match.group(1)
                    
                    if s3_bucket and s3_region:
                        s3_config = f"""
# S3 settings
repo1-type=s3
repo1-s3-bucket={s3_bucket}
repo1-s3-region={s3_region}
repo1-s3-endpoint=s3.{s3_region}.amazonaws.com
"""
                        main_config += s3_config
            
            # Write main configuration directly to server
            self.ssh.execute_command(f"echo '{main_config}' | sudo tee /etc/pgbackrest/pgbackrest.conf > /dev/null")
            self.ssh.execute_command("sudo chmod 640 /etc/pgbackrest/pgbackrest.conf")
            self.ssh.execute_command("sudo chown postgres:postgres /etc/pgbackrest/pgbackrest.conf")
        else:
            # Update existing main config to include conf.d path if missing
            include_check = self.ssh.execute_command("sudo grep -q 'config-include-path' /etc/pgbackrest/pgbackrest.conf || echo 'missing'")
            if include_check['exit_code'] == 0 and 'missing' in include_check['stdout']:
                self.logger.info("Adding config-include-path to main configuration")
                self.ssh.execute_command("sudo sed -i '/\\[global\\]/a config-include-path=/etc/pgbackrest/conf.d' /etc/pgbackrest/pgbackrest.conf")
        
        # Create or update stanza configuration
        data_dir = self.get_data_directory() or "/var/lib/postgresql/data"
        stanza_file = f"/etc/pgbackrest/conf.d/{db_name}.conf"
        
        check_stanza = self.ssh.execute_command(f"sudo test -f {stanza_file} && echo 'exists'")
        if check_stanza['exit_code'] != 0 or 'exists' not in check_stanza['stdout']:
            self.logger.info(f"Creating stanza configuration for {db_name}")
            stanza_conf = f"""[{db_name}]
# PostgreSQL connection settings
pg1-path={data_dir}
pg1-port=5432
pg1-socket-path=/var/run/postgresql
pg1-user=postgres
"""
            
            # Write stanza configuration directly to server
            self.ssh.execute_command(f"echo '{stanza_conf}' | sudo tee {stanza_file} > /dev/null")
            self.ssh.execute_command(f"sudo chmod 640 {stanza_file}")
            self.ssh.execute_command(f"sudo chown postgres:postgres {stanza_file}")
        
        # Create required log and backup directories
        for dir_path in ["/var/log/pgbackrest", "/var/lib/pgbackrest"]:
            self.ssh.execute_command(f"sudo mkdir -p {dir_path}")
            self.ssh.execute_command(f"sudo chmod 750 {dir_path}")
            self.ssh.execute_command(f"sudo chown -R postgres:postgres {dir_path}")
        
        # Create or update stanza
        self.logger.info(f"Creating stanza {db_name}")
        create_stanza = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --log-level-console=detail stanza-create --force")
        
        # Check if successful
        if create_stanza['exit_code'] != 0:
            self.logger.error(f"Failed to create stanza: {create_stanza['stderr'] or create_stanza['stdout']}")
            return False
            
        # Verify configuration
        check_config = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} check")
        if check_config['exit_code'] != 0:
            self.logger.error(f"pgBackRest configuration check failed: {check_config['stderr']}")
            return False
            
        self.logger.info(f"Successfully updated pgBackRest configuration for {db_name}")
        return True
    
    def _update_postgres_config(self, db_name):
        """Update PostgreSQL configuration for backups"""
        pg_conf = self._find_postgresql_conf()
        if not pg_conf:
            self.logger.error("Could not locate PostgreSQL configuration file")
            return False
        
        # Backup original configuration
        self.ssh.execute_command(f"sudo cp {pg_conf} {pg_conf}.bak")
        
        # Update configuration settings
        settings = {
            'archive_mode': 'on',
            'wal_level': 'replica',
            'archive_command': f"'pgbackrest --stanza={db_name} archive-push %p'"
        }
        
        for setting, value in settings.items():
            # Check if setting exists
            check = self.ssh.execute_command(f"sudo grep -E '^[ \\t]*{setting}[ \\t]*=' {pg_conf}")
            if check['exit_code'] == 0:
                # Update existing setting
                self.ssh.execute_command(f"sudo sed -i 's|^[ \\t]*{setting}[ \\t]*=.*|{setting} = {value}|' {pg_conf}")
            else:
                # Add new setting
                self.ssh.execute_command(f"echo '{setting} = {value}' | sudo tee -a {pg_conf}")
        
        return True
    
    def _find_postgresql_conf(self):
        """Find PostgreSQL configuration file"""
        pg_version = self.get_postgres_version()
        
        # Common configuration paths
        paths = []
        if pg_version:
            paths = [
                f"/etc/postgresql/{pg_version}/main/postgresql.conf",
                f"/var/lib/postgresql/{pg_version}/data/postgresql.conf"
            ]
        
        paths.extend([
            "/etc/postgresql/*/main/postgresql.conf",
            "/var/lib/postgresql/*/data/postgresql.conf",
            "/var/lib/pgsql/*/data/postgresql.conf"
        ])
        
        # Try each path
        for path in paths:
            if "*" in path:
                result = self.ssh.execute_command(f"ls {path} 2>/dev/null | head -1")
                if result['exit_code'] == 0 and result['stdout'].strip():
                    return result['stdout'].strip()
            else:
                result = self.ssh.execute_command(f"sudo test -f {path} && echo '{path}'")
                if result['exit_code'] == 0 and result['stdout'].strip():
                    return result['stdout'].strip()
        
        return None
    
    def _restart_postgres(self):
        """Restart PostgreSQL service"""
        services = ["postgresql", "postgres", "postgresql.service", "postgresql-*"]
        
        for service in services:
            result = self.ssh.execute_command(f"sudo systemctl restart {service}")
            if result['exit_code'] == 0:
                self.logger.info(f"Successfully restarted PostgreSQL using service: {service}")
                return True
        
        # Try pg_ctl as last resort
        data_dir = self.get_data_directory()
        if data_dir:
            result = self.ssh.execute_command(f"sudo -u postgres pg_ctl restart -D {data_dir}")
            if result['exit_code'] == 0:
                return True
        
        self.logger.error("Could not restart PostgreSQL")
        return False
    
    def setup_cron_job(self, db_name, backup_type, cron_expression):
        """Set up cron job for scheduled backups"""
        cron_command = f"pgbackrest --stanza={db_name} --type={backup_type} backup"
        cron_line = f"{cron_expression} postgres {cron_command} > /var/log/pgbackrest/cron-{db_name}-{backup_type}.log 2>&1\n"
        
        with open(f'/tmp/pgbackrest-{db_name}-{backup_type}', 'w') as f:
            f.write(cron_line)
        
        self.ssh.upload_file(f'/tmp/pgbackrest-{db_name}-{backup_type}', f'/tmp/pgbackrest-{db_name}-{backup_type}')
        self.ssh.execute_command(f"sudo mv /tmp/pgbackrest-{db_name}-{backup_type} /etc/cron.d/pgbackrest-{db_name}-{backup_type}")
        self.ssh.execute_command(f"sudo chmod 644 /etc/cron.d/pgbackrest-{db_name}-{backup_type}")
        
        return True
    
    def verify_and_fix_postgres_config(self, db_name):
        """Verify and fix PostgreSQL configuration for backup"""
        # Check if PostgreSQL is running
        pg_running = self.ssh.execute_command("ps aux | grep postgres | grep -v grep")
        if pg_running['exit_code'] != 0 or not pg_running['stdout'].strip():
            self.logger.warning("PostgreSQL is not running. Attempting to start it...")
            
            # Try to start PostgreSQL
            start_success = False
            start_error_msg = ""
            
            # Try multiple methods to start PostgreSQL
            start_methods = [
                "sudo systemctl start postgresql",
                "sudo systemctl start postgres",
                "sudo systemctl start postgresql.service",
                "sudo service postgresql start",
                "sudo service postgres start"
            ]
            
            for start_cmd in start_methods:
                result = self.ssh.execute_command(start_cmd)
                if result['exit_code'] == 0:
                    self.logger.info(f"PostgreSQL started successfully using: {start_cmd}")
                    start_success = True
                    
                    # Enable the service to start on boot
                    service_name = start_cmd.split()[-1]
                    self.ssh.execute_command(f"sudo systemctl enable {service_name}")
                    self.logger.info(f"PostgreSQL service {service_name} enabled to start at boot")
                    break
                else:
                    # Collect error information
                    if result['stderr']:
                        start_error_msg += f"Command '{start_cmd}' failed: {result['stderr']}\n"
            
            # Also try pg_ctl as a last resort
            if not start_success:
                data_dir = self.get_data_directory()
                if data_dir:
                    result = self.ssh.execute_command(f"sudo -u postgres pg_ctl start -D {data_dir}")
                    if result['exit_code'] == 0:
                        self.logger.info("PostgreSQL started successfully using pg_ctl")
                        start_success = True
                    else:
                        if result['stderr']:
                            start_error_msg += f"pg_ctl start failed: {result['stderr']}\n"
            
            # Check again if PostgreSQL is now running
            pg_running = self.ssh.execute_command("ps aux | grep postgres | grep -v grep")
            if pg_running['exit_code'] != 0 or not pg_running['stdout'].strip():
                # Try to repair PostgreSQL if it won't start
                self.logger.warning("PostgreSQL failed to start. Attempting to repair common issues...")
                repair_result, repair_message = self._try_repair_postgresql()
                
                if repair_result:
                    # Try to start PostgreSQL again after repair
                    for start_cmd in start_methods[:2]:  # Try the first two methods again
                        result = self.ssh.execute_command(start_cmd)
                        if result['exit_code'] == 0:
                            self.logger.info(f"PostgreSQL started successfully after repair using: {start_cmd}")
                            start_success = True
                            break
                
                # Final check if PostgreSQL is running
                pg_running = self.ssh.execute_command("ps aux | grep postgres | grep -v grep")
                if pg_running['exit_code'] != 0 or not pg_running['stdout'].strip():
                    # Get diagnostic information
                    diag_info = self._get_postgres_diagnostic_info()
                    error_message = f"PostgreSQL is not running and could not be started automatically.\n{diag_info}"
                    if repair_message:
                        error_message += f"\nRepair attempt: {repair_message}"
                    if start_error_msg:
                        error_message += f"\nStart errors: {start_error_msg}"
                    return False, error_message
            
            self.logger.info("PostgreSQL was successfully started")
        
        changes_made = []
        
        # Update PostgreSQL configuration
        if self._update_postgres_config(db_name):
            changes_made.append("Updated PostgreSQL configuration")
        
        # Update pgBackRest configuration
        if self.update_pgbackrest_config(db_name):
            changes_made.append("Updated pgBackRest configuration")
        
        # Restart if changes were made
        if changes_made:
            self.logger.info(f"Configuration changes made: {', '.join(changes_made)}")
            if self._restart_postgres():
                return True, "Configuration updated and PostgreSQL restarted"
            else:
                return False, "Configuration updated but PostgreSQL restart failed"
        
        return True, "No configuration changes needed"
    
    def _try_repair_postgresql(self):
        """Try to repair common issues with PostgreSQL that prevent it from starting"""
        repair_attempted = False
        repair_message = []
        
        # Check data directory
        data_dir = self.get_data_directory()
        if not data_dir:
            data_dir = "/var/lib/postgresql/data"  # Default fallback
            
        # Check PostgreSQL version to determine potential specific issues
        pg_version = None
        version_dirs = self.ssh.execute_command("ls -d /var/lib/postgresql/*/main 2>/dev/null || ls -d /var/lib/pgsql/*/data 2>/dev/null")
        if version_dirs['exit_code'] == 0 and version_dirs['stdout'].strip():
            for dir_path in version_dirs['stdout'].strip().split('\n'):
                version_match = re.search(r'/postgresql/(\d+)/main', dir_path)
                if version_match:
                    pg_version = version_match.group(1)
                    data_dir = dir_path
                    break
                    
        self.logger.info(f"Detected PostgreSQL version: {pg_version}, data directory: {data_dir}")
        
        # Check for systemd failure type
        systemd_status = self.ssh.execute_command("sudo systemctl status postgresql* || sudo systemctl status postgres")
        systemd_failure = None
        if "failed" in systemd_status['stdout'].lower():
            # Look for specific failure reasons
            if "result: protocol" in systemd_status['stdout'].lower():
                systemd_failure = "protocol"
                repair_message.append("Detected systemd 'protocol' failure - PostgreSQL may have configuration issues")
            elif "result: exit-code" in systemd_status['stdout'].lower():
                systemd_failure = "exit-code"
                repair_message.append("Detected systemd 'exit-code' failure - PostgreSQL process terminated abnormally")
            elif "result: timeout" in systemd_status['stdout'].lower():
                systemd_failure = "timeout"
                repair_message.append("Detected systemd 'timeout' failure - PostgreSQL start operation timed out")
        
        # Check for lock files
        self.logger.info("Checking for stale lock files...")
        lock_files = [
            f"{data_dir}/postmaster.pid",
            f"{data_dir}/.s.PGSQL.5432.lock",
            "/var/run/postgresql/.s.PGSQL.5432.lock",
            "/tmp/.s.PGSQL.5432.lock"
        ]
        
        for lock_file in lock_files:
            result = self.ssh.execute_command(f"sudo test -f {lock_file} && echo 'exists'")
            if 'exists' in result['stdout']:
                self.logger.info(f"Found stale lock file: {lock_file}, removing...")
                self.ssh.execute_command(f"sudo rm -f {lock_file}")
                repair_attempted = True
                repair_message.append(f"Removed stale lock file {lock_file}")
        
        # Check for socket issues (very common with protocol failures)
        socket_files = [
            "/var/run/postgresql/.s.PGSQL.5432",
            "/tmp/.s.PGSQL.5432"
        ]
        
        for socket_file in socket_files:
            result = self.ssh.execute_command(f"sudo test -S {socket_file} && echo 'exists'")
            if 'exists' in result['stdout']:
                self.logger.info(f"Found potentially stale socket: {socket_file}, removing...")
                self.ssh.execute_command(f"sudo rm -f {socket_file}")
                repair_attempted = True
                repair_message.append(f"Removed stale socket {socket_file}")
        
        # Check disk space
        df_result = self.ssh.execute_command("df -h")
        if df_result['exit_code'] == 0:
            lines = df_result['stdout'].strip().split('\n')
            for line in lines[1:]:  # Skip header
                parts = line.split()
                if len(parts) >= 5:
                    usage = parts[4].strip('%')
                    try:
                        if int(usage) >= 95:  # 95% or more
                            self.logger.warning(f"Disk usage critical: {parts[4]} on {parts[5]}")
                            repair_message.append(f"Warning: Disk nearly full ({parts[4]}) on {parts[5]}")
                    except (ValueError, IndexError):
                        pass
        
        # Check permissions on data directory
        if data_dir:
            # Ensure proper ownership
            perm_result = self.ssh.execute_command(f"sudo ls -la {data_dir}")
            if perm_result['exit_code'] == 0:
                if "postgres" not in perm_result['stdout']:
                    self.logger.info("Fixing PostgreSQL data directory permissions...")
                    self.ssh.execute_command(f"sudo chown -R postgres:postgres {data_dir}")
                    self.ssh.execute_command(f"sudo chmod -R 700 {data_dir}")
                    repair_attempted = True
                    repair_message.append(f"Fixed permissions on {data_dir}")
                    
            # Verify consistent ownership of all files
            self.ssh.execute_command(f"sudo find {data_dir} ! -user postgres -exec chown postgres:postgres {{}} ';'")
            self.ssh.execute_command(f"sudo find {data_dir} -type d ! -perm 700 -exec chmod 700 {{}} ';'")
        
        # Fix possible corrupted configs
        postgres_conf_path = self._find_postgresql_conf()
        if postgres_conf_path:
            # Check if postgresql.conf has proper permissions
            self.ssh.execute_command(f"sudo chown postgres:postgres {postgres_conf_path}")
            repair_message.append(f"Ensured proper ownership of {postgres_conf_path}")
            
            # PostgreSQL 17 specific fixes for protocol failures
            if pg_version == "17" and systemd_failure == "protocol":
                self.logger.info("Applying PostgreSQL 17 specific protocol failure fixes...")
                
                # Check for problematic settings in postgresql.conf
                for setting in ["unix_socket_directories", "listen_addresses", "port"]:
                    setting_check = self.ssh.execute_command(f"sudo grep -E '^{setting}\\s*=' {postgres_conf_path}")
                    if setting_check['exit_code'] == 0:
                        # Reset to default
                        self.ssh.execute_command(f"sudo sed -i 's/^{setting}\\s*=.*$/# {setting} = default/' {postgres_conf_path}")
                        repair_attempted = True
                        repair_message.append(f"Reset {setting} in postgresql.conf to default")
                
                # Specifically check for socket directories issue
                socket_check = self.ssh.execute_command("sudo test -d /var/run/postgresql -a -w /var/run/postgresql || echo 'issue'")
                if 'issue' in socket_check['stdout']:
                    self.ssh.execute_command("sudo mkdir -p /var/run/postgresql")
                    self.ssh.execute_command("sudo chown postgres:postgres /var/run/postgresql")
                    self.ssh.execute_command("sudo chmod 775 /var/run/postgresql")
                    repair_attempted = True
                    repair_message.append("Fixed socket directory permissions")
                
                # Fix SSL settings that might cause protocol failures
                ssl_settings_check = self.ssh.execute_command(f"sudo grep -E '^ssl\\s*=' {postgres_conf_path}")
                if ssl_settings_check['exit_code'] == 0:
                    self.ssh.execute_command(f"sudo sed -i 's/^ssl\\s*=.*$/ssl = off/' {postgres_conf_path}")
                    repair_attempted = True
                    repair_message.append("Disabled SSL temporarily to troubleshoot protocol failures")
        
        # Check systemd service status for errors
        if systemd_failure:
            # Try resetting failed state
            self.ssh.execute_command("sudo systemctl reset-failed postgresql* || true")
            self.ssh.execute_command("sudo systemctl daemon-reload")
            repair_attempted = True
            repair_message.append("Reset failed systemd service status and reloaded daemon")
            
            # For protocol failures, try more aggressive fixes
            if systemd_failure == "protocol":
                # Check if port 5432 is already in use by another process
                port_check = self.ssh.execute_command("sudo ss -tulpn | grep ':5432'")
                if port_check['exit_code'] == 0 and port_check['stdout'].strip():
                    # Port is in use, try to identify and stop the process
                    pid_match = re.search(r'pid=(\d+)', port_check['stdout'])
                    if pid_match:
                        pid = pid_match.group(1)
                        self.ssh.execute_command(f"sudo kill {pid}")
                        repair_attempted = True
                        repair_message.append(f"Killed process {pid} using port 5432")
                
                # Try to clean potential corrupt systemd unit files
                self.ssh.execute_command("sudo systemctl stop postgresql* || true")
                self.ssh.execute_command("sudo rm -f /run/systemd/units/invocation:postgresql* || true")
                self.ssh.execute_command("sudo systemctl daemon-reload")
                repair_attempted = True
                repair_message.append("Cleaned systemd unit invocation files")
        
        # Try deeper repair for persistent issues (especially for PostgreSQL 17)
        if pg_version and systemd_failure:
            # Check if we need deep repair
            recent_failures = self.ssh.execute_command("sudo systemctl show postgresql -p Result")
            deep_repair_needed = (
                systemd_failure == "protocol" or 
                "Result=protocol" in recent_failures['stdout'] or
                len(repair_message) >= 3  # If we've already tried multiple fixes
            )
            
            if deep_repair_needed:
                self.logger.warning("Attempting deep repair for PostgreSQL...")
                
                # Collect diagnostic info before attempting repair
                self.ssh.execute_command("sudo journalctl -u postgresql --no-pager -n 50 > /tmp/pg_journalctl.log")
                
                # Backup pg_hba.conf and postgresql.conf 
                if postgres_conf_path:
                    conf_dir = os.path.dirname(postgres_conf_path)
                    self.ssh.execute_command(f"sudo cp {conf_dir}/pg_hba.conf {conf_dir}/pg_hba.conf.backup 2>/dev/null || true")
                    self.ssh.execute_command(f"sudo cp {conf_dir}/postgresql.conf {conf_dir}/postgresql.conf.backup 2>/dev/null || true")
                    repair_message.append("Created backup of PostgreSQL configuration files")
                
                # Try resetting shared memory segments (common issue with protocol failures)
                self.ssh.execute_command("sudo ipcrm -a 2>/dev/null || true")
                repair_attempted = True
                repair_message.append("Reset system shared memory segments")
                
                # For PostgreSQL 17, common issue is socket directory or configuration issues
                if pg_version == "17":
                    # Create minimal working postgresql.conf if needed
                    if postgres_conf_path:
                        # Create a more complete but minimal configuration for PostgreSQL 17
                        self.ssh.execute_command(f'''sudo bash -c "cat > {postgres_conf_path}.minimal << EOF
# Minimal PostgreSQL configuration
listen_addresses = 'localhost'
port = 5432
unix_socket_directories = '/var/run/postgresql'
shared_buffers = 128MB
dynamic_shared_memory_type = posix
max_connections = 100
ssl = off
# The following entries are auto-detected but included for completeness
data_directory = '{data_dir}'
hba_file = '{conf_dir}/pg_hba.conf'
ident_file = '{conf_dir}/pg_ident.conf'
EOF"''')
                        
                        # Try with minimal config
                        self.ssh.execute_command(f"sudo cp {postgres_conf_path}.minimal {postgres_conf_path}")
                        self.ssh.execute_command(f"sudo chown postgres:postgres {postgres_conf_path}")
                        repair_attempted = True
                        repair_message.append("Created minimal PostgreSQL configuration to ensure startup")
                    
                    # Also check for specific PostgreSQL 17 service unit issues
                    pg17_unit_check = self.ssh.execute_command("sudo systemctl list-unit-files | grep postgresql")
                    if 'postgresql@17' in pg17_unit_check['stdout']:
                        # Try the explicit instance name for PostgreSQL 17
                        self.ssh.execute_command("sudo systemctl stop postgresql postgresql@17-main || true")
                        self.ssh.execute_command("sudo systemctl reset-failed postgresql postgresql@17-main || true")
                        self.ssh.execute_command("sudo systemctl daemon-reload")
                        repair_attempted = True
                        repair_message.append("Reset PostgreSQL 17 specific systemd service units")
        
        if repair_attempted:
            return True, "; ".join(repair_message)
        else:
            return False, "No repair actions were needed or possible"
    
    def _get_postgres_diagnostic_info(self):
        """Collect diagnostic information about PostgreSQL status"""
        diagnostic_info = []
        
        # Check if PostgreSQL packages are installed
        pkg_check = self.ssh.execute_command("dpkg -l | grep postgresql || rpm -qa | grep postgresql")
        if pkg_check['exit_code'] == 0 and pkg_check['stdout'].strip():
            diagnostic_info.append("PostgreSQL packages are installed")
        else:
            diagnostic_info.append("No PostgreSQL packages detected")
            
        # Check service status
        status_check = self.ssh.execute_command("sudo systemctl status postgresql* || sudo service postgresql status")
        if status_check['stdout']:
            # Extract status from systemctl output
            status_lines = status_check['stdout'].strip().split('\n')
            for line in status_lines[:10]:  # Limit to first 10 lines
                if "Active:" in line:
                    diagnostic_info.append(f"Service status: {line.strip()}")
                    break
        
        # Check logs
        log_check = self.ssh.execute_command("sudo tail -n 20 /var/log/postgresql/*.log 2>/dev/null || sudo journalctl -u postgresql -n 20")
        if log_check['stdout']:
            diagnostic_info.append("Recent log entries:")
            log_lines = log_check['stdout'].strip().split('\n')
            for line in log_lines[-5:]:  # Last 5 lines
                diagnostic_info.append(f"  {line.strip()}")
        
        # Check data directory
        data_dir = self.get_data_directory()
        if data_dir:
            diagnostic_info.append(f"Data directory: {data_dir}")
            # Check permissions
            perm_check = self.ssh.execute_command(f"ls -la {data_dir}")
            if perm_check['exit_code'] == 0:
                diagnostic_info.append("Data directory permissions:")
                perm_lines = perm_check['stdout'].strip().split('\n')
                for line in perm_lines[:3]:  # First few lines
                    diagnostic_info.append(f"  {line.strip()}")
        else:
            diagnostic_info.append("Data directory not found")
        
        return "\n".join(diagnostic_info)
    
    def fix_incremental_backup_config(self, db_name):
        """Fix configuration for incremental backups"""
        # Check if a full backup exists
        check_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
        if check_backup['exit_code'] != 0 or ('backup/incr' in check_backup['stdout'] and 'backup/full' not in check_backup['stdout']):
            # Force a full backup if needed
            full_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --type=full backup")
            if full_backup['exit_code'] != 0:
                return False, full_backup['stderr']
            return True, "Full backup created successfully. Incremental backups can now be performed."
        
        return True, "Backup configuration is correct."
    
    def check_backup_system_health(self, db_name):
        """Perform a comprehensive check of the entire backup system
        
        Returns:
            tuple: (overall_status, dict_with_detailed_status)
        """
        health_status = {
            'postgres_running': False,
            'postgres_service_status': None,
            'postgres_service_enabled': None,
            'postgres_service_name': None,
            'pgbackrest_installed': False,
            'pg_config_correct': False,
            'backup_config_correct': False,
            'full_backup_exists': False,
            'issues': []
        }
        
        # Check if PostgreSQL is running
        pg_running = self.ssh.execute_command("ps aux | grep postgres | grep -v grep")
        health_status['postgres_running'] = pg_running['exit_code'] == 0 and pg_running['stdout'].strip()
        
        # Check PostgreSQL service status
        pg_services = ["postgresql", "postgres", "postgresql.service"]
        for service in pg_services:
            # Check service status
            status_result = self.ssh.execute_command(f"sudo systemctl is-active {service}")
            if status_result['exit_code'] == 0:
                health_status['postgres_service_status'] = status_result['stdout'].strip()
                health_status['postgres_service_name'] = service
                
                # Check if service is enabled
                enabled_result = self.ssh.execute_command(f"sudo systemctl is-enabled {service}")
                health_status['postgres_service_enabled'] = enabled_result['exit_code'] == 0 and enabled_result['stdout'].strip() == "enabled"
                break
        
        if not health_status['postgres_running']:
            health_status['issues'].append("PostgreSQL is not running")
            
            # Add more detailed service status if available
            if health_status['postgres_service_status']:
                health_status['issues'].append(f"PostgreSQL service ({health_status['postgres_service_name']}) is {health_status['postgres_service_status']}")
                if not health_status['postgres_service_enabled']:
                    health_status['issues'].append(f"PostgreSQL service ({health_status['postgres_service_name']}) is not enabled to start automatically")
                
            return False, health_status
        
        # Check if pgbackrest is installed
        health_status['pgbackrest_installed'] = self.check_pgbackrest_installed()
        if not health_status['pgbackrest_installed']:
            health_status['issues'].append("pgbackrest is not installed")
            return False, health_status
            
        # Check PostgreSQL configuration - only check, don't modify
        pg_config = self._find_postgresql_conf()
        if not pg_config:
            health_status['issues'].append("Could not find PostgreSQL configuration file")
        else:
            # Check archive mode and command settings
            check_cmd = f"sudo grep -E 'archive_mode|archive_command' {pg_config}"
            result = self.ssh.execute_command(check_cmd)
            
            archive_mode_ok = 'archive_mode = on' in result['stdout'].lower()
            archive_command_ok = 'pgbackrest' in result['stdout'].lower() and 'archive-push' in result['stdout'].lower()
            
            health_status['pg_config_correct'] = archive_mode_ok and archive_command_ok
            
            if not archive_mode_ok:
                health_status['issues'].append("archive_mode is not set to 'on'")
            if not archive_command_ok:
                health_status['issues'].append("archive_command is not properly configured for pgBackRest")
                
        # Check backup configuration - only check, don't modify
        check_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
        health_status['backup_config_correct'] = check_backup['exit_code'] == 0
        
        if not health_status['backup_config_correct']:
            health_status['issues'].append("pgBackRest configuration is incorrect")
            
            # Check for specific S3 endpoint issue
            if "requires option: repo1-s3-endpoint" in check_backup['stderr']:
                health_status['issues'].append("Missing S3 endpoint configuration")
        
        # Check if full backup exists
        if health_status['backup_config_correct']:
            health_status['full_backup_exists'] = 'backup/full' in check_backup['stdout']
            
            if not health_status['full_backup_exists']:
                health_status['issues'].append("No full backup exists")
                
        # Overall status is good if all key checks pass
        overall_status = (health_status['postgres_running'] and 
                         health_status['pgbackrest_installed'] and 
                         health_status['pg_config_correct'] and 
                         health_status['backup_config_correct'] and 
                         (health_status['full_backup_exists'] or True))  # Don't fail just because no backup exists yet
        
        return overall_status, health_status
    
    def execute_backup(self, db_name, backup_type):
        """Execute a backup on demand"""
        # Check if configuration is valid without modifying it
        check_cmd = f"sudo -u postgres pgbackrest --stanza={db_name} check"
        check_result = self.ssh.execute_command(check_cmd)
        
        # Only fix configuration if there's an issue
        if check_result['exit_code'] != 0:
            self.logger.warning(f"Backup configuration check failed: {check_result['stderr']}. Attempting to fix.")
            
            # Check for specific S3 endpoint issue
            if "requires option: repo1-s3-endpoint" in check_result['stderr']:
                self.logger.info("Fixing missing S3 endpoint configuration")
                s3_region = None
                region_check = self.ssh.execute_command("sudo grep 'repo1-s3-region' /etc/pgbackrest/pgbackrest.conf")
                if region_check['exit_code'] == 0:
                    region_match = re.search(r'repo1-s3-region=([^\s]+)', region_check['stdout'])
                    if region_match:
                        s3_region = region_match.group(1)
                
                if s3_region:
                    endpoint_cmd = f"sudo sed -i '/repo1-s3-region/a repo1-s3-endpoint=s3.{s3_region}.amazonaws.com' /etc/pgbackrest/pgbackrest.conf"
                    self.ssh.execute_command(endpoint_cmd)
            else:
                # Only perform full configuration fix if not a simple issue
                self.verify_and_fix_postgres_config(db_name)
        
        # For incremental backup, check if full backup exists
        if backup_type == 'incr':
            check_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
            if check_backup['exit_code'] != 0 or 'backup/full' not in check_backup['stdout']:
                self.logger.warning("No prior backup exists, changing to full backup")
                backup_type = 'full'
        
        # Execute backup without reconfiguring
        self.logger.info(f"Executing {backup_type} backup for {db_name}")
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --type={backup_type} --repo1-retention-full=7 --repo1-retention-full-type=count backup")
        
        if result['exit_code'] != 0:
            return False, result['stderr']
        
        return True, result['stdout']
    
    def list_backups(self, db_name):
        """List available backups"""
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
        
        if result['exit_code'] != 0:
            return []
        
        # Parse backup info
        backups = []
        output = result['stdout'].strip()
        if not output:
            return []
        
        try:
            # Group by backup entries
            backup_sections = []
            current_section = []
            
            for line in output.split('\n'):
                line = line.strip()
                if not line or line.startswith('stanza'):
                    continue
                
                # Check if this line starts a new backup entry
                if 'backup' in line and ':' in line:
                    if current_section:
                        backup_sections.append(current_section)
                    current_section = [line]
                elif current_section:
                    current_section.append(line)
            
            # Add the last section
            if current_section:
                backup_sections.append(current_section)
            
            # Process each backup section
            for section in backup_sections:
                backup_info = {'info': {}}
                
                # First line contains the backup name
                if section and ':' in section[0]:
                    name = section[0].split(':')[0].strip()
                    backup_info['name'] = name
                    backup_info['type'] = 'full' if 'full' in name else 'incr'
                    
                    # Process additional info
                    for i in range(1, len(section)):
                        if '=' in section[i]:
                            key, value = section[i].split('=', 1)
                            backup_info['info'][key.strip()] = value.strip()
                    
                    backups.append(backup_info)
        
        except Exception as e:
            self.logger.error(f"Error parsing backup list: {str(e)}")
        
        return backups
    
    def restore_backup(self, db_name, backup_name=None, restore_time=None):
        """Restore database from backup"""
        # Stop PostgreSQL
        self.ssh.execute_command("sudo systemctl stop postgresql")
        
        # Build restore command
        restore_cmd = f"sudo -u postgres pgbackrest --stanza={db_name} restore"
        
        if backup_name:
            restore_cmd += f" --set={backup_name}"
        
        if restore_time:
            restore_cmd += f" --type=time --target='{restore_time}'"
        
        # Execute restore
        result = self.ssh.execute_command(restore_cmd)
        
        # Start PostgreSQL
        self.ssh.execute_command("sudo systemctl start postgresql")
        
        if result['exit_code'] != 0:
            return False, result['stderr']
        
        return True, result['stdout']
    
    def upgrade_postgres_to_latest(self):
        """Upgrade PostgreSQL to the latest available version
        
        Returns:
            tuple: (success, old_version, new_version)
        """
        # Get current version
        old_version = self.get_postgres_version()
        if not old_version:
            return False, None, None
            
        # Check if we're already on the latest version
        is_latest, _, latest_version = self.check_latest_postgres_version()
        if is_latest:
            return True, old_version, old_version
            
        # Detect OS type for appropriate upgrade method
        result = self.ssh.execute_command("cat /etc/os-release")
        is_debian_based = "debian" in result['stdout'].lower() or "ubuntu" in result['stdout'].lower()
        is_rhel_based = "rhel" in result['stdout'].lower() or "centos" in result['stdout'].lower() or "fedora" in result['stdout'].lower() or "rocky" in result['stdout'].lower() or "alma" in result['stdout'].lower()
        
        success = False
        
        if is_debian_based:
            # Debian/Ubuntu upgrade
            self.logger.info(f"Upgrading PostgreSQL from {old_version} to {latest_version} on Debian/Ubuntu")
            
            # Install the new version
            self.ssh.execute_command("sudo apt-get update")
            result = self.ssh.execute_command(f"sudo apt-get install -y postgresql-{latest_version} postgresql-contrib-{latest_version}")
            
            if result['exit_code'] != 0:
                self.logger.error(f"Failed to install PostgreSQL {latest_version}: {result['stderr']}")
                return False, old_version, None
                
            # Use pg_upgradecluster if available (Debian/Ubuntu)
            pg_upgrade_check = self.ssh.execute_command("which pg_upgradecluster")
            if pg_upgrade_check['exit_code'] == 0:
                # Get the cluster name (usually "main")
                cluster_check = self.ssh.execute_command("pg_lsclusters")
                cluster_name = "main"  # Default
                
                if cluster_check['exit_code'] == 0:
                    # Parse output to find cluster name
                    for line in cluster_check['stdout'].strip().split('\n'):
                        if line and old_version in line:
                            parts = line.split()
                            if len(parts) >= 2:
                                cluster_name = parts[1]
                                break
                
                # Run upgrade
                result = self.ssh.execute_command(f"sudo pg_upgradecluster {old_version} {cluster_name}")
                success = result['exit_code'] == 0
            else:
                # Manual upgrade might be needed
                self.logger.warning("pg_upgradecluster not available, manual upgrade might be required")
                success = False
                
        elif is_rhel_based:
            # RHEL/CentOS upgrade
            self.logger.info(f"Upgrading PostgreSQL from {old_version} to {latest_version} on RHEL/CentOS")
            
            # Install the new version
            result = self.ssh.execute_command(f"sudo dnf install -y postgresql{latest_version}-server postgresql{latest_version}-contrib")
            
            if result['exit_code'] != 0:
                self.logger.error(f"Failed to install PostgreSQL {latest_version}: {result['stderr']}")
                return False, old_version, None
                
            # Initialize the new database
            init_result = self.ssh.execute_command(f"sudo /usr/pgsql-{latest_version}/bin/postgresql-{latest_version}-setup initdb")
            
            if init_result['exit_code'] != 0:
                self.logger.error(f"Failed to initialize PostgreSQL {latest_version}: {init_result['stderr']}")
                return False, old_version, None
                
            # Manual upgrade might be needed
            self.logger.warning("Manual data migration might be required")
            success = True  # We installed the new version, but data migration might be needed
        else:
            # Generic upgrade attempt
            self.logger.warning("Unsupported OS for automatic PostgreSQL upgrade")
            success = False
            
        # Verify the upgrade
        new_version = self.get_postgres_version()
        
        # If version didn't change, the upgrade might have failed
        if new_version == old_version:
            self.logger.warning(f"PostgreSQL version remains at {old_version} after upgrade attempt")
            success = False
            
        return success, old_version, new_version
    
    def initialize_server(self):
        """Initialize a new server: update system, install PostgreSQL and pgBackRest, wait, and restart"""
        self.logger.info("Initializing server with PostgreSQL and pgBackRest")
        
        try:
            # Update server packages
            self.logger.info("Updating server packages")
            self.ssh.execute_command("sudo apt-get update")
            update_result = self.ssh.execute_command("sudo apt-get upgrade -y")
            
            # Install PostgreSQL - continue even if update failed
            self.logger.info("Installing PostgreSQL")
            postgres_installed = self.install_postgres()
            if not postgres_installed:
                self.logger.error("Failed to install PostgreSQL")
                return False, "Failed to install PostgreSQL"
            
            # Get PostgreSQL version for confirmation
            pg_version = self.get_postgres_version()
            if not pg_version:
                self.logger.error("PostgreSQL installation verification failed")
                return False, "PostgreSQL installation verification failed"
            
            self.logger.info(f"PostgreSQL {pg_version} installed successfully")
            
            # Install pgBackRest
            self.logger.info("Installing pgBackRest")
            pgbackrest_installed = self.install_pgbackrest()
            if not pgbackrest_installed:
                self.logger.warning("Failed to install pgBackRest, but PostgreSQL is working")
                return True, f"Server initialized with PostgreSQL {pg_version}, but pgBackRest installation failed"
            
            self.logger.info("pgBackRest installed successfully")
            
            # Wait 10 seconds
            self.logger.info("Waiting 10 seconds before restarting services")
            time.sleep(10)
            
            # Restart PostgreSQL
            self.logger.info("Restarting PostgreSQL")
            restart_success = self._restart_postgres()
            if not restart_success:
                self.logger.warning("Failed to restart PostgreSQL, but installation was successful")
                return True, f"Server initialized with PostgreSQL {pg_version} and pgBackRest, but restart failed"
            
            self.logger.info("PostgreSQL restarted successfully")
            return True, f"Server initialized successfully with PostgreSQL {pg_version} and pgBackRest"
            
        except Exception as e:
            self.logger.error(f"Error initializing server: {str(e)}")
            return False, f"Error initializing server: {str(e)}"
    
    def create_database(self, db_name, username, password):
        """Create a new PostgreSQL database and user
        
        Args:
            db_name (str): The name of the database to create
            username (str): The database username to create
            password (str): The password for the user
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating database {db_name} with user {username}")
        
        try:
            # Check if PostgreSQL is installed
            if not self.check_postgres_installed():
                return False, "PostgreSQL is not installed on the server"
            
            # Check if database already exists
            check_db_cmd = f"sudo -u postgres psql -t -c \"SELECT 1 FROM pg_database WHERE datname = '{db_name}';\""
            check_result = self.ssh.execute_command(check_db_cmd)
            
            if check_result['exit_code'] == 0 and check_result['stdout'].strip():
                return False, f"Database '{db_name}' already exists"
            
            # Check if user already exists
            check_user_cmd = f"sudo -u postgres psql -t -c \"SELECT 1 FROM pg_roles WHERE rolname = '{username}';\""
            check_user_result = self.ssh.execute_command(check_user_cmd)
            
            user_exists = check_user_result['exit_code'] == 0 and check_user_result['stdout'].strip()
            
            # Create user if it doesn't exist
            if not user_exists:
                create_user_cmd = f"sudo -u postgres psql -c \"CREATE USER {username} WITH ENCRYPTED PASSWORD '{password}';\""
                user_result = self.ssh.execute_command(create_user_cmd)
                
                if user_result['exit_code'] != 0:
                    return False, f"Failed to create user '{username}': {user_result['stderr']}"
            
            # Create database
            create_db_cmd = f"sudo -u postgres psql -c \"CREATE DATABASE {db_name} OWNER {username};\""
            db_result = self.ssh.execute_command(create_db_cmd)
            
            if db_result['exit_code'] != 0:
                return False, f"Failed to create database '{db_name}': {db_result['stderr']}"
            
            # Grant privileges
            grant_cmd = f"sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {username};\""
            self.ssh.execute_command(grant_cmd)
            
            return True, f"Database '{db_name}' with owner '{username}' created successfully"
            
        except Exception as e:
            self.logger.error(f"Error creating database: {str(e)}")
            return False, f"Error creating database: {str(e)}"
    
    def update_database_user(self, username, password):
        """Update a PostgreSQL user's password
        
        Args:
            username (str): The database username to update
            password (str): The new password for the user
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Updating password for user {username}")
        
        try:
            # Check if PostgreSQL is installed
            if not self.check_postgres_installed():
                return False, "PostgreSQL is not installed on the server"
            
            # Check if user exists
            check_user_cmd = f"sudo -u postgres psql -t -c \"SELECT 1 FROM pg_roles WHERE rolname = '{username}';\""
            check_user_result = self.ssh.execute_command(check_user_cmd)
            
            if not (check_user_result['exit_code'] == 0 and check_user_result['stdout'].strip()):
                return False, f"User '{username}' does not exist"
            
            # Update user password
            update_cmd = f"sudo -u postgres psql -c \"ALTER USER {username} WITH ENCRYPTED PASSWORD '{password}';\""
            result = self.ssh.execute_command(update_cmd)
            
            if result['exit_code'] != 0:
                return False, f"Failed to update password for user '{username}': {result['stderr']}"
            
            return True, f"Password updated for user '{username}'"
            
        except Exception as e:
            self.logger.error(f"Error updating user password: {str(e)}")
            return False, f"Error updating user password: {str(e)}"