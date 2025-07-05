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
        
        result = self.ssh.execute_command("psql --version")
        if result['exit_code'] == 0:
            # Extract version from output like "psql (PostgreSQL) 13.4"
            version_match = re.search(r'(\d+\.\d+)', result['stdout'])
            if version_match:
                return version_match.group(1)
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
        
        # Create pgBackRest configuration file
        config_content = f"""
[global]
repo1-type=s3
repo1-s3-bucket={s3_bucket}
repo1-s3-endpoint=s3.{s3_region}.amazonaws.com
repo1-s3-region={s3_region}
repo1-s3-key={s3_access_key}
repo1-s3-key-secret={s3_secret_key}
repo1-path=/pgbackrest
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
        
        # Update PostgreSQL configuration
        pg_version = self.get_postgres_version()
        if not pg_version:
            self.logger.error("Could not determine PostgreSQL version")
            return False
        
        # Create a backup of the postgresql.conf file
        postgresql_conf_path = f"/etc/postgresql/{pg_version}/main/postgresql.conf"
        self.ssh.execute_command(f"sudo cp {postgresql_conf_path} {postgresql_conf_path}.bak")
        
        # Add archive settings to postgresql.conf
        archive_settings = """
# pgBackRest archive settings
archive_mode = on
archive_command = 'pgbackrest --stanza={0} archive-push %p'
"""
        
        # Append archive settings to postgresql.conf
        self.ssh.execute_command(f"echo '{archive_settings.format(db_name)}' | sudo tee -a {postgresql_conf_path}")
        
        # Create pgBackRest stanza
        self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} stanza-create")
        
        # Check configuration
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} check")
        
        if result['exit_code'] != 0:
            self.logger.error(f"pgBackRest configuration check failed: {result['stderr']}")
            return False
            
        # Restart PostgreSQL
        self.ssh.execute_command("sudo systemctl restart postgresql")
        
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
    
    def execute_backup(self, db_name, backup_type):
        """Execute a backup on demand"""
        self.logger.info(f"Executing {backup_type} backup for database {db_name}")
        
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} --type={backup_type} backup")
        
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