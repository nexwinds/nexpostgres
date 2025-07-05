import logging
import re
from app.utils.ssh_manager import SSHManager

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
        
        # Create required directories
        dirs = ["/etc/pgbackrest", "/var/log/pgbackrest", "/var/lib/pgbackrest"]
        for dir_path in dirs:
            self.ssh.execute_command(f"sudo mkdir -p {dir_path}")
            self.ssh.execute_command(f"sudo chmod 750 {dir_path}")
        
        # Create pgBackRest configuration
        config_content = f"""
[global]
repo1-type=s3
repo1-s3-bucket={s3_bucket}
repo1-s3-endpoint=s3.{s3_region}.amazonaws.com
repo1-s3-region={s3_region}
repo1-s3-key={s3_access_key}
repo1-s3-key-secret={s3_secret_key}
repo1-path=/pgbackrest
repo1-retention-full=7
repo1-retention-full-type=count
process-max=4
log-level-console=info
log-level-file=debug

[{db_name}]
pg1-path={self.get_data_directory() or "/var/lib/postgresql/data"}
"""
        
        # Write configuration to file
        with open('/tmp/pgbackrest.conf', 'w') as f:
            f.write(config_content)
        
        self.ssh.upload_file('/tmp/pgbackrest.conf', '/tmp/pgbackrest.conf')
        self.ssh.execute_command("sudo mv /tmp/pgbackrest.conf /etc/pgbackrest/pgbackrest.conf")
        self.ssh.execute_command("sudo chmod 640 /etc/pgbackrest/pgbackrest.conf")
        self.ssh.execute_command("sudo chown postgres:postgres /etc/pgbackrest/pgbackrest.conf")
        
        # Update PostgreSQL configuration
        self._update_postgres_config(db_name)
        
        # Create the stanza
        self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} stanza-create")
        
        # Check configuration
        check_result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} check")
        if check_result['exit_code'] != 0:
            self.logger.error(f"pgBackRest configuration check failed: {check_result['stderr']}")
            return False
        
        # Restart PostgreSQL
        self._restart_postgres()
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
            return False, "PostgreSQL is not running"
        
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
    
    def update_pgbackrest_config(self, db_name):
        """Update pgBackRest configuration for an existing stanza"""
        # Check if config exists
        check_config = self.ssh.execute_command("sudo test -f /etc/pgbackrest/pgbackrest.conf && echo 'exists'")
        if check_config['exit_code'] != 0 or 'exists' not in check_config['stdout']:
            self.logger.warning("pgBackRest configuration not found")
            return False
        
        # Check if stanza exists
        check_stanza = self.ssh.execute_command(f"sudo grep '\\[{db_name}\\]' /etc/pgbackrest/pgbackrest.conf")
        if check_stanza['exit_code'] != 0:
            # Add stanza if it doesn't exist
            data_dir = self.get_data_directory() or "/var/lib/postgresql/data"
            self.ssh.execute_command(f"""echo "
[{db_name}]
pg1-path={data_dir}" | sudo tee -a /etc/pgbackrest/pgbackrest.conf""")
        
        # Create stanza
        create_stanza = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} stanza-create")
        return create_stanza['exit_code'] == 0
    
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
    
    def execute_backup(self, db_name, backup_type):
        """Execute a backup on demand"""
        # Verify and fix configuration issues
        self.verify_and_fix_postgres_config(db_name)
        
        # For incremental backup, check if full backup exists
        if backup_type == 'incr':
            check_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
            if check_backup['exit_code'] != 0 or 'backup/full' not in check_backup['stdout']:
                self.logger.warning("No prior backup exists, changing to full backup")
                backup_type = 'full'
        
        # Execute backup
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