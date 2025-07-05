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
        
        # Try multiple methods to get PostgreSQL version
        
        # Method 1: Use psql as postgres user
        result = self.ssh.execute_command("sudo -u postgres psql --version")
        if result['exit_code'] == 0:
            # Extract version from output like "psql (PostgreSQL) 13.4"
            version_match = re.search(r'(\d+\.\d+)', result['stdout'])
            if version_match:
                return version_match.group(1)
        
        # Method 2: Try to get version directly from PostgreSQL
        result = self.ssh.execute_command("sudo -u postgres psql -c 'SHOW server_version;' -t")
        if result['exit_code'] == 0:
            version_str = result['stdout'].strip()
            version_match = re.search(r'(\d+\.\d+)', version_str)
            if version_match:
                return version_match.group(1)
        
        # Method 3: Check postgres data directory structure
        result = self.ssh.execute_command("ls -la /var/lib/postgresql/")
        if result['exit_code'] == 0:
            # Look for version directories like "9.6", "10", "11", "12", "13", "14", "15", etc.
            dir_match = re.search(r'(\d+\.?\d*)', result['stdout'])
            if dir_match:
                return dir_match.group(1)
                
        # Method 4: Check configuration directory structure
        result = self.ssh.execute_command("ls -la /etc/postgresql/")
        if result['exit_code'] == 0:
            # Look for version directories like "9.6", "10", "11", "12", "13", "14", "15", etc.
            dir_match = re.search(r'(\d+\.?\d*)', result['stdout'])
            if dir_match:
                return dir_match.group(1)
        
        # Method 5: Try dpkg (for Debian/Ubuntu) or rpm (for RHEL/CentOS)
        result = self.ssh.execute_command("dpkg -l | grep postgresql | grep -v pgbackrest")
        if result['exit_code'] == 0:
            version_match = re.search(r'postgresql-(\d+\.?\d*)', result['stdout'])
            if version_match:
                return version_match.group(1)
        
        result = self.ssh.execute_command("rpm -qa | grep postgresql | grep -v pgbackrest")
        if result['exit_code'] == 0:
            version_match = re.search(r'postgresql-(\d+\.?\d*)', result['stdout'])
            if version_match:
                return version_match.group(1)
        
        # Log available PostgreSQL info for debugging
        self.logger.error("Failed to determine PostgreSQL version, collecting debug info:")
        debug_cmds = [
            "which psql",
            "find /etc -name 'postgres*' -type d",
            "find /var/lib -name 'postgres*' -type d",
            "ps aux | grep postgres",
            "pg_lsclusters" # This is available on Debian/Ubuntu systems
        ]
        
        for cmd in debug_cmds:
            try:
                result = self.ssh.execute_command(cmd)
                self.logger.info(f"Debug command '{cmd}': exit_code={result['exit_code']}, stdout={result['stdout']}")
            except Exception as e:
                self.logger.info(f"Debug command '{cmd}' failed: {str(e)}")
        
        return None
    
    def list_databases(self):
        """List all PostgreSQL databases on the remote server"""
        self.logger.info("Listing PostgreSQL databases")
        
        if not self.check_postgres_installed():
            self.logger.error("PostgreSQL is not installed")
            return []
        
        # Run psql command to list databases
        result = self.ssh.execute_command("sudo -u postgres psql -t -c \"SELECT datname, pg_size_pretty(pg_database_size(datname)), datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"")
        
        if result['exit_code'] != 0:
            self.logger.error(f"Failed to list databases: {result['stderr']}")
            return []
        
        # Parse the output
        databases = []
        for line in result['stdout'].strip().split('\n'):
            if line.strip():
                parts = line.strip().split('|')
                if len(parts) >= 3:
                    db_name = parts[0].strip()
                    db_size = parts[1].strip()
                    db_owner = parts[2].strip()
                    
                    # Skip system databases
                    if db_name not in ['postgres', 'template0', 'template1']:
                        databases.append({
                            'name': db_name,
                            'size': db_size,
                            'owner': db_owner
                        })
        
        return databases
    
    def install_postgres(self):
        """Install PostgreSQL on the remote server"""
        self.logger.info("Installing PostgreSQL")
        
        # Update package lists
        self.ssh.execute_command("sudo apt-get update")
        
        # Install PostgreSQL
        result = self.ssh.execute_command("sudo apt-get install -y postgresql postgresql-contrib")
        
        if result['exit_code'] != 0:
            self.logger.error(f"Failed to install PostgreSQL: {result['stderr']}")
            return False
        
        # Enable and start PostgreSQL service
        self.ssh.execute_command("sudo systemctl enable postgresql")
        self.ssh.execute_command("sudo systemctl start postgresql")
        
        return self.check_postgres_installed()
    
    def check_pgbackrest_installed(self):
        """Check if pgBackRest is installed on the remote server"""
        result = self.ssh.execute_command("which pgbackrest")
        return result['exit_code'] == 0
    
    def install_pgbackrest(self):
        """Install pgBackRest on the remote server"""
        self.logger.info("Installing pgBackRest")
        
        # Update package lists
        self.ssh.execute_command("sudo apt-get update")
        
        # Install pgBackRest
        result = self.ssh.execute_command("sudo apt-get install -y pgbackrest")
        
        if result['exit_code'] != 0:
            self.logger.error(f"Failed to install pgBackRest: {result['stderr']}")
            return False
        
        return self.check_pgbackrest_installed()
    
    def setup_pgbackrest_config(self, db_name, s3_bucket, s3_region, s3_access_key, s3_secret_key):
        """Set up pgBackRest configuration for S3 backups"""
        self.logger.info(f"Setting up pgBackRest configuration for database {db_name}")
        
        # Create pgBackRest configuration directory
        self.ssh.execute_command("sudo mkdir -p /etc/pgbackrest")
        self.ssh.execute_command("sudo mkdir -p /var/log/pgbackrest")
        self.ssh.execute_command("sudo mkdir -p /var/lib/pgbackrest")
        self.ssh.execute_command("sudo chmod 750 /var/log/pgbackrest")
        self.ssh.execute_command("sudo chmod 750 /var/lib/pgbackrest")
        
        # Create pgBackRest configuration file with proper retention settings
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
pg1-path=/var/lib/postgresql/data
"""
        
        # Write configuration to file
        with open('/tmp/pgbackrest.conf', 'w') as f:
            f.write(config_content)
        
        self.ssh.upload_file('/tmp/pgbackrest.conf', '/tmp/pgbackrest.conf')
        self.ssh.execute_command("sudo mv /tmp/pgbackrest.conf /etc/pgbackrest/pgbackrest.conf")
        self.ssh.execute_command("sudo chmod 640 /etc/pgbackrest/pgbackrest.conf")
        self.ssh.execute_command("sudo chown postgres:postgres /etc/pgbackrest/pgbackrest.conf")
        
        # Find PostgreSQL configuration file
        postgresql_conf_path = None
        
        # Get PostgreSQL version
        pg_version = self.get_postgres_version()
        
        if pg_version:
            conf_paths = [
                f"/etc/postgresql/{pg_version}/main/postgresql.conf",
                f"/var/lib/postgresql/{pg_version}/data/postgresql.conf"
            ]
        else:
            # If version can't be determined, try common paths
            conf_paths = [
                "/etc/postgresql/*/main/postgresql.conf",
                "/var/lib/postgresql/*/data/postgresql.conf",
                "/var/lib/pgsql/*/data/postgresql.conf"
            ]
            
            # Try to find by listing directories
            find_result = self.ssh.execute_command("find /etc/postgresql -name postgresql.conf")
            if find_result['exit_code'] == 0 and find_result['stdout'].strip():
                # Use the first found configuration file
                conf_paths.insert(0, find_result['stdout'].strip().split("\n")[0])
        
        # Try each path until we find the config file
        for path in conf_paths:
            check_path = self.ssh.execute_command(f"sudo test -f {path} && echo 'exists'")
            if check_path['exit_code'] == 0 and 'exists' in check_path['stdout']:
                postgresql_conf_path = path
                self.logger.info(f"Found PostgreSQL configuration at: {postgresql_conf_path}")
                break
            
            # If it's a wildcard path, try to expand it
            if "*" in path:
                expand_path = self.ssh.execute_command(f"sudo ls {path} 2>/dev/null | head -1")
                if expand_path['exit_code'] == 0 and expand_path['stdout'].strip():
                    postgresql_conf_path = expand_path['stdout'].strip()
                    self.logger.info(f"Found PostgreSQL configuration at: {postgresql_conf_path}")
                    break
        
        if not postgresql_conf_path:
            self.logger.error("Could not locate PostgreSQL configuration file")
            return False
            
        # Make a backup of the conf file
        self.ssh.execute_command(f"sudo cp {postgresql_conf_path} {postgresql_conf_path}.bak")
        
        # Update PostgreSQL configuration with proper settings
        # Instead of just appending, update the configuration properly
        # First, check if archive_mode is already set
        check_archive = self.ssh.execute_command(f"sudo grep -E '^[ \\t]*archive_mode[ \\t]*=' {postgresql_conf_path}")
        check_wal = self.ssh.execute_command(f"sudo grep -E '^[ \\t]*wal_level[ \\t]*=' {postgresql_conf_path}")
        
        # If archive_mode is already set, update it
        if check_archive['exit_code'] == 0:
            self.ssh.execute_command(f"sudo sed -i 's/^[ \\t]*archive_mode[ \\t]*=.*/archive_mode = on/' {postgresql_conf_path}")
        else:
            # Otherwise, append it
            self.ssh.execute_command(f"echo 'archive_mode = on' | sudo tee -a {postgresql_conf_path}")
        
        # If wal_level is already set, update it
        if check_wal['exit_code'] == 0:
            self.ssh.execute_command(f"sudo sed -i 's/^[ \\t]*wal_level[ \\t]*=.*/wal_level = replica/' {postgresql_conf_path}")
        else:
            # Otherwise, append it
            self.ssh.execute_command(f"echo 'wal_level = replica' | sudo tee -a {postgresql_conf_path}")
        
        # Set the archive command
        check_archive_cmd = self.ssh.execute_command(f"sudo grep -E '^[ \\t]*archive_command[ \\t]*=' {postgresql_conf_path}")
        if check_archive_cmd['exit_code'] == 0:
            self.ssh.execute_command(f"sudo sed -i 's|^[ \\t]*archive_command[ \\t]*=.*|archive_command = \\'pgbackrest --stanza={db_name} archive-push %p\\'|' {postgresql_conf_path}")
        else:
            self.ssh.execute_command(f"echo 'archive_command = \\'pgbackrest --stanza={db_name} archive-push %p\\'' | sudo tee -a {postgresql_conf_path}")
        
        # Create pgBackRest stanza
        self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} stanza-create")
        
        # Check configuration
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} check")
        
        if result['exit_code'] != 0:
            self.logger.error(f"pgBackRest configuration check failed: {result['stderr']}")
            return False
            
        # Try to restart PostgreSQL with different service names
        service_names = ["postgresql", "postgres", "postgresql.service", "postgresql-*"]
        restarted = False
        
        for service in service_names:
            restart_cmd = f"sudo systemctl restart {service}"
            result = self.ssh.execute_command(restart_cmd)
            if result['exit_code'] == 0:
                self.logger.info(f"Successfully restarted PostgreSQL using service: {service}")
                restarted = True
                break
        
        if not restarted:
            self.logger.warning("Could not restart PostgreSQL using systemctl, trying pg_ctl")
            # Try pg_ctl as a last resort
            data_dir = self.ssh.execute_command("sudo -u postgres psql -c 'SHOW data_directory;' -t")
            if data_dir['exit_code'] == 0 and data_dir['stdout'].strip():
                pg_data_dir = data_dir['stdout'].strip()
                self.ssh.execute_command(f"sudo -u postgres pg_ctl restart -D {pg_data_dir}")
            else:
                self.logger.error("Could not restart PostgreSQL")
                # Continue anyway, as the configuration may be picked up on next manual restart
        
        return True
    
    def setup_cron_job(self, db_name, backup_type, cron_expression):
        """Set up cron job for scheduled backups"""
        self.logger.info(f"Setting up {backup_type} backup cron job for database {db_name}")
        
        cron_command = f"pgbackrest --stanza={db_name} --type={backup_type} backup"
        
        # Create temporary cron file
        cron_line = f"{cron_expression} postgres {cron_command} > /var/log/pgbackrest/cron-{db_name}-{backup_type}.log 2>&1\n"
        
        with open(f'/tmp/pgbackrest-{db_name}-{backup_type}', 'w') as f:
            f.write(cron_line)
        
        self.ssh.upload_file(f'/tmp/pgbackrest-{db_name}-{backup_type}', f'/tmp/pgbackrest-{db_name}-{backup_type}')
        self.ssh.execute_command(f"sudo mv /tmp/pgbackrest-{db_name}-{backup_type} /etc/cron.d/pgbackrest-{db_name}-{backup_type}")
        self.ssh.execute_command(f"sudo chmod 644 /etc/cron.d/pgbackrest-{db_name}-{backup_type}")
        
        return True
    
    def verify_and_fix_postgres_config(self, db_name):
        """Verify and fix PostgreSQL configuration for backup"""
        self.logger.info(f"Verifying PostgreSQL configuration for database {db_name}")
        
        # Check if PostgreSQL is running
        pg_running = self.ssh.execute_command("ps aux | grep postgres | grep -v grep")
        if pg_running['exit_code'] != 0 or not pg_running['stdout'].strip():
            self.logger.error("PostgreSQL does not appear to be running")
            return False, "PostgreSQL is not running"
        
        # Get PostgreSQL version
        pg_version = self.get_postgres_version()
        
        # Try to find PostgreSQL configuration file
        postgresql_conf_path = None
        
        if pg_version:
            # Standard path for Debian/Ubuntu
            conf_paths = [
                f"/etc/postgresql/{pg_version}/main/postgresql.conf",
                f"/var/lib/postgresql/{pg_version}/data/postgresql.conf"  # For Red Hat/CentOS
            ]
        else:
            # If version can't be determined, try common paths
            conf_paths = [
                "/etc/postgresql/*/main/postgresql.conf",  # For Debian/Ubuntu
                "/var/lib/postgresql/*/data/postgresql.conf",  # For Red Hat/CentOS
                "/var/lib/pgsql/*/data/postgresql.conf"  # Alternative Red Hat path
            ]
            
            # Try to find by listing directories
            find_result = self.ssh.execute_command("find /etc /var/lib -name postgresql.conf 2>/dev/null")
            if find_result['exit_code'] == 0 and find_result['stdout'].strip():
                # Use the first found configuration file
                conf_paths.insert(0, find_result['stdout'].strip().split("\n")[0])
        
        # Try each path until we find the config file
        for path in conf_paths:
            check_path = self.ssh.execute_command(f"sudo test -f {path} && echo 'exists'")
            if check_path['exit_code'] == 0 and 'exists' in check_path['stdout']:
                postgresql_conf_path = path
                self.logger.info(f"Found PostgreSQL configuration at: {postgresql_conf_path}")
                break
            
            # If it's a wildcard path, try to expand it
            if "*" in path:
                expand_path = self.ssh.execute_command(f"sudo ls {path} 2>/dev/null | head -1")
                if expand_path['exit_code'] == 0 and expand_path['stdout'].strip():
                    postgresql_conf_path = expand_path['stdout'].strip()
                    self.logger.info(f"Found PostgreSQL configuration at: {postgresql_conf_path}")
                    break
        
        if not postgresql_conf_path:
            self.logger.error("Could not locate PostgreSQL configuration file")
            return False, "Could not locate PostgreSQL configuration file"
        
        # Get data directory for finding HBA and recovery files if needed
        data_dir = None
        data_dir_cmd = self.ssh.execute_command("sudo -u postgres psql -t -c 'SHOW data_directory;'")
        if data_dir_cmd['exit_code'] == 0 and data_dir_cmd['stdout'].strip():
            data_dir = data_dir_cmd['stdout'].strip()
            self.logger.info(f"PostgreSQL data directory: {data_dir}")
        
        # Check current settings directly from PostgreSQL if possible
        settings_check = {}
        try:
            # Try to get settings from PostgreSQL
            for setting in ['wal_level', 'archive_mode', 'archive_command']:
                result = self.ssh.execute_command(f"sudo -u postgres psql -c 'SHOW {setting};' -t")
                if result['exit_code'] == 0:
                    settings_check[setting] = result['stdout'].strip()
                    self.logger.info(f"Current {setting}: {settings_check[setting]}")
        except Exception as e:
            self.logger.warning(f"Error checking PostgreSQL settings: {str(e)}")
        
        changes_made = []
        
        # Fix WAL level if needed
        if settings_check.get('wal_level', '').lower() != 'replica':
            self.ssh.execute_command(f"sudo sed -i 's/^[ \\t]*wal_level[ \\t]*=.*/wal_level = replica/' {postgresql_conf_path}")
            if not self.ssh.execute_command(f"sudo grep -E '^[ \\t]*wal_level[ \\t]*=' {postgresql_conf_path}")['exit_code'] == 0:
                self.ssh.execute_command(f"echo 'wal_level = replica' | sudo tee -a {postgresql_conf_path}")
            changes_made.append("wal_level set to replica")
        
        # Fix archive_mode if needed
        if settings_check.get('archive_mode', '').lower() != 'on':
            self.ssh.execute_command(f"sudo sed -i 's/^[ \\t]*archive_mode[ \\t]*=.*/archive_mode = on/' {postgresql_conf_path}")
            if not self.ssh.execute_command(f"sudo grep -E '^[ \\t]*archive_mode[ \\t]*=' {postgresql_conf_path}")['exit_code'] == 0:
                self.ssh.execute_command(f"echo 'archive_mode = on' | sudo tee -a {postgresql_conf_path}")
            changes_made.append("archive_mode set to on")
        
        # Fix archive_command if needed
        archive_cmd = f"pgbackrest --stanza={db_name} archive-push %p"
        if archive_cmd not in settings_check.get('archive_command', ''):
            # First, try to update it if it exists
            self.ssh.execute_command(f"sudo sed -i 's|^[ \\t]*archive_command[ \\t]*=.*|archive_command = \\'pgbackrest --stanza={db_name} archive-push %p\\'|' {postgresql_conf_path}")
            
            # Check if the setting was applied
            if not self.ssh.execute_command(f"sudo grep -E '^[ \\t]*archive_command[ \\t]*=' {postgresql_conf_path}")['exit_code'] == 0:
                # If not found, add it
                self.ssh.execute_command(f"echo \"archive_command = 'pgbackrest --stanza={db_name} archive-push %p'\" | sudo tee -a {postgresql_conf_path}")
                
            changes_made.append("archive_command set for pgbackrest")
        
        # Explicitly check that archive_command is set correctly
        check_cmd = self.ssh.execute_command(f"sudo grep 'archive_command' {postgresql_conf_path}")
        if check_cmd['exit_code'] != 0 or 'pgbackrest' not in check_cmd['stdout']:
            # Force set it to ensure it's applied
            self.logger.info("Forcing archive_command setting to be applied")
            self.ssh.execute_command(f"echo \"archive_command = 'pgbackrest --stanza={db_name} archive-push %p'\" | sudo tee -a {postgresql_conf_path}")
            changes_made.append("archive_command explicitly added")
        
        # If we have the data directory, create/update recovery config if needed (for PostgreSQL < 12)
        if data_dir:
            pg_recovery_path = f"{data_dir}/recovery.conf"
            check_recovery = self.ssh.execute_command(f"sudo test -f {pg_recovery_path} && echo 'exists'")
            
            if check_recovery['exit_code'] == 0 and 'exists' in check_recovery['stdout']:
                # Update existing recovery.conf if it exists
                self.ssh.execute_command(f"sudo sed -i 's|^[ \\t]*restore_command[ \\t]*=.*|restore_command = \\'pgbackrest --stanza={db_name} archive-get %f %p\\'|' {pg_recovery_path}")
                changes_made.append("recovery.conf updated")
        
        # Also update pgBackRest configuration to ensure it's correct
        self.update_pgbackrest_config(db_name)
        
        # Restart PostgreSQL if changes were made
        if changes_made:
            self.logger.info(f"PostgreSQL configuration changes made: {', '.join(changes_made)}")
            restart_result = self.ssh.execute_command("sudo systemctl restart postgresql")
            if restart_result['exit_code'] == 0:
                # Verify settings after restart
                try:
                    verify_cmd = self.ssh.execute_command("sudo -u postgres psql -c 'SHOW archive_command;' -t")
                    if verify_cmd['exit_code'] == 0:
                        if 'pgbackrest' in verify_cmd['stdout']:
                            self.logger.info("Archive command successfully applied")
                        else:
                            self.logger.warning(f"Archive command not correctly applied: {verify_cmd['stdout']}")
                except Exception as e:
                    self.logger.warning(f"Could not verify settings after restart: {str(e)}")
                    
                return True, "Configuration updated and PostgreSQL restarted"
            else:
                # Try alternate service names if standard doesn't work
                alt_services = ["postgresql", "postgres", "postgresql.service", "postgresql-*"]
                restarted = False
                
                for service in alt_services:
                    restart_result = self.ssh.execute_command(f"sudo systemctl restart {service}")
                    if restart_result['exit_code'] == 0:
                        restarted = True
                        break
                
                if restarted:
                    return True, "Configuration updated and PostgreSQL restarted"
                else:
                    return False, "Configuration updated but PostgreSQL restart failed"
        else:
            return True, "No configuration changes needed"
            
    def update_pgbackrest_config(self, db_name):
        """Update pgBackRest configuration for an existing stanza"""
        self.logger.info(f"Updating pgBackRest configuration for database {db_name}")
        
        # Check if pgBackRest config exists
        check_config = self.ssh.execute_command("sudo test -f /etc/pgbackrest/pgbackrest.conf && echo 'exists'")
        
        if check_config['exit_code'] != 0 or 'exists' not in check_config['stdout']:
            self.logger.warning("pgBackRest configuration not found")
            return False
            
        # Check if stanza exists in config
        check_stanza = self.ssh.execute_command(f"sudo grep '\\[{db_name}\\]' /etc/pgbackrest/pgbackrest.conf")
        
        if check_stanza['exit_code'] != 0:
            # Stanza doesn't exist, add it
            self.logger.info(f"Adding stanza {db_name} to pgBackRest config")
            
            # Get data directory
            data_dir_cmd = self.ssh.execute_command("sudo -u postgres psql -t -c 'SHOW data_directory;'")
            data_dir = "/var/lib/postgresql/data"  # Default
            
            if data_dir_cmd['exit_code'] == 0 and data_dir_cmd['stdout'].strip():
                data_dir = data_dir_cmd['stdout'].strip()
            
            # Add stanza section
            self.ssh.execute_command(f"""echo "
[{db_name}]
pg1-path={data_dir}" | sudo tee -a /etc/pgbackrest/pgbackrest.conf""")
        
        # Create the stanza if it doesn't exist
        create_stanza = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} stanza-create")
        
        if create_stanza['exit_code'] != 0:
            self.logger.warning(f"Error creating stanza: {create_stanza['stderr']}")
            return False
            
        return True
    
    def fix_incremental_backup_config(self, db_name):
        """Fix configuration for incremental backups"""
        self.logger.info(f"Fixing incremental backup configuration for database {db_name}")
        
        # First, check if a full backup exists
        check_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
        
        if check_backup['exit_code'] != 0 or 'backup/incr' in check_backup['stdout']:
            # If no backup exists or only incremental backups exist, force a full backup
            self.logger.info("No full backup found, performing a full backup first")
            full_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --type=full backup")
            
            if full_backup['exit_code'] != 0:
                self.logger.error(f"Failed to create full backup: {full_backup['stderr']}")
                return False, full_backup['stderr']
                
            return True, "Full backup created successfully. Incremental backups can now be performed."
            
        return True, "Backup configuration is correct."
    
    def execute_backup(self, db_name, backup_type):
        """Execute a backup on demand"""
        self.logger.info(f"Executing {backup_type} backup for database {db_name}")
        
        # First check if archive_mode is enabled and working correctly
        archive_check = self.ssh.execute_command("sudo -u postgres psql -c \"SHOW archive_mode;\"")
        if "on" not in archive_check.get('stdout', ''):
            self.logger.warning("PostgreSQL archive_mode may not be enabled properly")
            
            # Try to fix configuration issues
            self.verify_and_fix_postgres_config(db_name)
        
        # Check archive_command
        archive_cmd_check = self.ssh.execute_command("sudo -u postgres psql -c \"SHOW archive_command;\"")
        if "pgbackrest" not in archive_cmd_check.get('stdout', ''):
            self.logger.warning("PostgreSQL archive_command is not properly configured for pgBackRest")
            
            # Fix the archive command specifically
            self.update_pgbackrest_config(db_name)
            
            # Force update the archive command
            self.ssh.execute_command(f"sudo -u postgres psql -c \"ALTER SYSTEM SET archive_command = 'pgbackrest --stanza={db_name} archive-push %p';\"")
            self.ssh.execute_command("sudo -u postgres psql -c \"SELECT pg_reload_conf();\"")
        
        # If this is an incremental backup, check if a full backup exists first
        if backup_type == 'incr':
            check_backup = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
            if check_backup['exit_code'] != 0 or 'backup' not in check_backup['stdout']:
                # No backups exist, so force a full backup instead
                self.logger.warning("No prior backup exists, changing to full backup")
                backup_type = 'full'
        
        # Execute backup with proper retention settings
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --type={backup_type} --repo1-retention-full=7 --repo1-retention-full-type=count backup")
        
        if result['exit_code'] != 0:
            self.logger.error(f"Backup failed: {result['stderr']}")
            return False, result['stderr']
        
        return True, result['stdout']
    
    def list_backups(self, db_name):
        """List available backups"""
        self.logger.info(f"Listing backups for database {db_name}")
        
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
        
        if result['exit_code'] != 0:
            self.logger.error(f"Failed to list backups: {result['stderr']}")
            return []
        
        # Parse backup info
        backups = []
        lines = result['stdout'].strip().split('\n')
        current_backup = None
        
        for line in lines:
            if line.startswith('stanza'):
                continue
                
            if 'backup' in line and ':' in line:
                if current_backup is not None:
                    backups.append(current_backup)
                    
                backup_name = line.split(':')[0].strip()
                current_backup = {
                    'name': backup_name,
                    'type': 'full' if 'full' in backup_name else 'incr',
                    'info': {}
                }
            
            elif current_backup is not None and '=' in line:
                key, value = line.strip().split('=')
                current_backup['info'][key.strip()] = value.strip()
        
        if current_backup is not None:
            backups.append(current_backup)
            
        return backups
    
    def restore_backup(self, db_name, backup_name=None, restore_time=None):
        """Restore database from backup"""
        self.logger.info(f"Restoring database {db_name} from backup")
        
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
            self.logger.error(f"Restore failed: {result['stderr']}")
            return False, result['stderr']
        
        return True, result['stdout'] 