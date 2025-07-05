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
            # Extract version from output like "psql (PostgreSQL) 13.4" or "psql (PostgreSQL) 16.9"
            # Include minor version for PostgreSQL 16+ since data directories might use specific format
            version_match = re.search(r'(\d+\.\d+)', result['stdout'])
            if version_match:
                return version_match.group(1)
        
        # Method 2: Try to get version directly from PostgreSQL
        result = self.ssh.execute_command("sudo -u postgres psql -c 'SHOW server_version;' -t")
        if result['exit_code'] == 0:
            version_str = result['stdout'].strip()
            # Get full version string for better accuracy
            version_match = re.search(r'(\d+\.\d+(?:\.\d+)?)', version_str)
            if version_match:
                return version_match.group(1)
        
        # Method 3: Check PostgreSQL clusters with pg_lsclusters (Debian/Ubuntu)
        result = self.ssh.execute_command("command -v pg_lsclusters > /dev/null && pg_lsclusters")
        if result['exit_code'] == 0 and result['stdout'].strip():
            # Parse pg_lsclusters output which provides accurate cluster version information
            # Format: Ver Cluster Port Status Owner    Data directory              Log file
            cluster_match = re.search(r'(\d+\.\d+)\s+\w+', result['stdout'])
            if cluster_match:
                return cluster_match.group(1)
        
        # Method 4: Check postgres data directory structure
        result = self.ssh.execute_command("ls -la /var/lib/postgresql/")
        if result['exit_code'] == 0:
            # Look for version directories like "9.6", "10", "11", "12", "13", "14", "15", "16", etc.
            dir_match = re.search(r'(\d+\.?\d*)', result['stdout'])
            if dir_match:
                return dir_match.group(1)
                
        # Method 5: Check configuration directory structure
        result = self.ssh.execute_command("ls -la /etc/postgresql/")
        if result['exit_code'] == 0:
            # Look for version directories like "9.6", "10", "11", "12", "13", "14", "15", "16", etc.
            dir_match = re.search(r'(\d+\.?\d*)', result['stdout'])
            if dir_match:
                return dir_match.group(1)
        
        # Method 6: Try dpkg (for Debian/Ubuntu) or rpm (for RHEL/CentOS)
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
    
    def get_data_directory(self):
        """Get PostgreSQL data directory from the remote server"""
        # Method 1: Direct query to PostgreSQL (most reliable)
        result = self.ssh.execute_command("sudo -u postgres psql -t -c 'SHOW data_directory;'")
        if result['exit_code'] == 0 and result['stdout'].strip():
            data_dir = result['stdout'].strip()
            self.logger.info(f"PostgreSQL data directory (from psql): {data_dir}")
            return data_dir
        
        # Method 2: Check using pg_lsclusters (Debian/Ubuntu)
        result = self.ssh.execute_command("command -v pg_lsclusters > /dev/null && pg_lsclusters")
        if result['exit_code'] == 0 and result['stdout'].strip():
            # Parse the output to extract the data directory
            # Format: Ver Cluster Port Status Owner    Data directory              Log file
            for line in result['stdout'].strip().split('\n'):
                if line and not line.startswith("Ver"):
                    parts = line.split()
                    if len(parts) >= 6:
                        version = parts[0]
                        cluster = parts[1]
                        data_dir = parts[5]
                        self.logger.info(f"PostgreSQL data directory (from pg_lsclusters): {data_dir}")
                        return data_dir
        
        # Method 3: Try to find the data directory by examining configuration
        pg_version = self.get_postgres_version()
        if pg_version:
            # Special handling for PostgreSQL 16.x
            if pg_version.startswith('16'):
                # PostgreSQL 16.x may use more specific paths
                possible_dirs = [
                    f"/var/lib/postgresql/{pg_version}/main",
                    f"/var/lib/postgresql/16/main",  # Common for 16.x versions
                    f"/var/lib/postgresql/16.9/main",  # Specific for 16.9
                    f"/etc/postgresql/{pg_version}/main",
                    f"/etc/postgresql/16/main", 
                    f"/etc/postgresql/16.9/main"
                ]
                
                for dir_path in possible_dirs:
                    check = self.ssh.execute_command(f"sudo test -d {dir_path} && echo 'exists'")
                    if check['exit_code'] == 0 and 'exists' in check['stdout']:
                        self.logger.info(f"PostgreSQL 16.x data directory (from standard location): {dir_path}")
                        return dir_path
                
                # Try looking for postgres clusters specifically for version 16
                pg_clusters = self.ssh.execute_command("command -v pg_lsclusters > /dev/null && pg_lsclusters | grep '16'")
                if pg_clusters['exit_code'] == 0 and pg_clusters['stdout'].strip():
                    for line in pg_clusters['stdout'].strip().split('\n'):
                        if "16" in line:  # Any 16.x version
                            parts = line.split()
                            if len(parts) >= 6:
                                data_dir = parts[5]
                                self.logger.info(f"PostgreSQL 16.x data directory (from pg_lsclusters): {data_dir}")
                                # Verify directory exists and is valid
                                check = self.ssh.execute_command(f"sudo test -d {data_dir} && echo 'exists'")
                                if check['exit_code'] == 0 and 'exists' in check['stdout']:
                                    return data_dir
            else:
                # Check standard locations based on version
                possible_dirs = [
                    f"/var/lib/postgresql/{pg_version}/main",
                    f"/var/lib/postgresql/{pg_version}/data",
                    f"/var/lib/pgsql/{pg_version}/data"
                ]
                
                for dir_path in possible_dirs:
                    check = self.ssh.execute_command(f"sudo test -d {dir_path} && echo 'exists'")
                    if check['exit_code'] == 0 and 'exists' in check['stdout']:
                        self.logger.info(f"PostgreSQL data directory (from standard location): {dir_path}")
                        return dir_path
        
        # Method 4: Check common data directory paths regardless of version
        common_dirs = [
            "/var/lib/postgresql/data",
            "/var/lib/postgresql/*/main",
            "/var/lib/postgresql/*/data",
            "/var/lib/pgsql/data",
            "/var/lib/pgsql/*/data"
        ]
        
        for dir_pattern in common_dirs:
            if "*" in dir_pattern:
                # Handle wildcard paths
                check = self.ssh.execute_command(f"ls -d {dir_pattern} 2>/dev/null | head -1")
                if check['exit_code'] == 0 and check['stdout'].strip():
                    data_dir = check['stdout'].strip()
                    dir_check = self.ssh.execute_command(f"sudo test -d {data_dir} && echo 'exists'")
                    if dir_check['exit_code'] == 0 and 'exists' in dir_check['stdout']:
                        self.logger.info(f"PostgreSQL data directory (from common path): {data_dir}")
                        return data_dir
            else:
                # Direct path check
                check = self.ssh.execute_command(f"sudo test -d {dir_pattern} && echo 'exists'")
                if check['exit_code'] == 0 and 'exists' in check['stdout']:
                    self.logger.info(f"PostgreSQL data directory (from common path): {dir_pattern}")
                    return dir_pattern
        
        # Method 5: Check process information
        result = self.ssh.execute_command("ps aux | grep postgres | grep -- '-D'")
        if result['exit_code'] == 0 and result['stdout'].strip():
            # Try to extract data directory from process command line
            dir_match = re.search(r'-D\s+([^\s]+)', result['stdout'])
            if dir_match:
                data_dir = dir_match.group(1)
                self.logger.info(f"PostgreSQL data directory (from process): {data_dir}")
                return data_dir
        
        # Method 6: Last resort, check postgres user's home directory
        result = self.ssh.execute_command("sudo -u postgres echo $HOME")
        if result['exit_code'] == 0 and result['stdout'].strip():
            home_dir = result['stdout'].strip()
            postgres_data = f"{home_dir}/data"
            check = self.ssh.execute_command(f"sudo test -d {postgres_data} && echo 'exists'")
            if check['exit_code'] == 0 and 'exists' in check['stdout']:
                self.logger.info(f"PostgreSQL data directory (from postgres user home): {postgres_data}")
                return postgres_data
                
        self.logger.error("Could not determine PostgreSQL data directory")
        return None
    
    def find_running_postgres_instance(self):
        """Find a running PostgreSQL instance by locating the postmaster.pid file"""
        self.logger.info("Attempting to find running PostgreSQL instance through postmaster.pid")
        
        # Common locations for postmaster.pid
        pid_locations = [
            "/var/lib/postgresql/16/main/postmaster.pid",
            "/var/lib/postgresql/16.9/main/postmaster.pid",
            "/var/run/postgresql/16-main.pid",  # Debian/Ubuntu style
            "/var/run/postgresql/16.9-main.pid"  # Debian/Ubuntu style for 16.9
        ]
        
        # Search for postmaster.pid files
        find_cmd = "sudo find /var/lib/postgresql /var/run/postgresql -name postmaster.pid 2>/dev/null || echo 'No postmaster.pid found'"
        find_result = self.ssh.execute_command(find_cmd)
        
        if find_result['exit_code'] == 0 and 'No postmaster.pid found' not in find_result['stdout']:
            self.logger.info(f"Found postmaster.pid files: {find_result['stdout']}")
            
            # Extract locations from the find result
            found_pids = find_result['stdout'].strip().split("\n")
            if found_pids:
                pid_locations = found_pids + pid_locations  # Add found locations to the beginning
        
        # Check each potential postmaster.pid location
        for pid_file in pid_locations:
            check_cmd = f"sudo test -f {pid_file} && echo 'exists'"
            check_result = self.ssh.execute_command(check_cmd)
            
            if check_result['exit_code'] == 0 and 'exists' in check_result['stdout']:
                self.logger.info(f"Found postmaster.pid at: {pid_file}")
                
                # Read the pid file to get the data directory
                read_cmd = f"sudo cat {pid_file}"
                read_result = self.ssh.execute_command(read_cmd)
                
                if read_result['exit_code'] == 0:
                    lines = read_result['stdout'].strip().split("\n")
                    if len(lines) >= 3:
                        # Line 1: PID
                        # Line 2: Data directory
                        # Line 3: Start time
                        data_dir = lines[1]
                        self.logger.info(f"PostgreSQL data directory from postmaster.pid: {data_dir}")
                        
                        # Try to connect using this data directory
                        conn_cmd = f"sudo -u postgres bash -c 'export PGDATA={data_dir}; psql -t -c \"SELECT datname, usename as owner FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;\"'"
                        conn_result = self.ssh.execute_command(conn_cmd)
                        
                        if conn_result['exit_code'] == 0:
                            # Parse database list
                            databases = []
                            for line in conn_result['stdout'].strip().split("\n"):
                                if line.strip():
                                    parts = line.strip().split("|")
                                    if len(parts) >= 2:
                                        db_name = parts[0].strip()
                                        db_owner = parts[1].strip()
                                        
                                        # Skip system databases
                                        if db_name not in ['postgres', 'template0', 'template1']:
                                            # Try to get size
                                            size_cmd = f"sudo -u postgres bash -c 'export PGDATA={data_dir}; psql -t -c \"SELECT pg_size_pretty(pg_database_size('\\'{db_name}\\'')),\"'"
                                            size_result = self.ssh.execute_command(size_cmd)
                                            db_size = "Unknown"
                                            
                                            if size_result['exit_code'] == 0 and size_result['stdout'].strip():
                                                db_size = size_result['stdout'].strip()
                                            
                                            databases.append({
                                                'name': db_name,
                                                'size': db_size,
                                                'owner': db_owner
                                            })
                            
                            if databases:
                                self.logger.info(f"Successfully listed {len(databases)} databases using postmaster.pid method")
                                return databases
                        
                        # Try with pg_ctl status
                        pg_ctl_cmd = f"sudo -u postgres pg_ctl -D {data_dir} status"
                        pg_ctl_result = self.ssh.execute_command(pg_ctl_cmd)
                        
                        if pg_ctl_result['exit_code'] == 0 and "server is running" in pg_ctl_result['stdout']:
                            self.logger.info(f"PostgreSQL server is running with data directory: {data_dir}")
                            
                            # Try a simplified connection with this directory
                            simple_cmd = f"sudo -u postgres bash -c 'export PGDATA={data_dir}; psql -l'"
                            simple_result = self.ssh.execute_command(simple_cmd)
                            
                            if simple_result['exit_code'] == 0:
                                self.logger.info("Successfully connected using postmaster.pid data directory")
                                
                                # Parse the output of psql -l
                                databases = []
                                lines = simple_result['stdout'].strip().split("\n")
                                for i in range(3, len(lines) - 2):  # Skip header and footer rows
                                    line = lines[i]
                                    if "|" in line:
                                        parts = line.split("|")
                                        if len(parts) >= 3:
                                            db_name = parts[0].strip()
                                            db_owner = parts[2].strip()
                                            
                                            # Skip system databases
                                            if db_name not in ['postgres', 'template0', 'template1']:
                                                databases.append({
                                                    'name': db_name,
                                                    'size': "Unknown",  # We don't have size info
                                                    'owner': db_owner
                                                })
                                
                                if databases:
                                    self.logger.info(f"Successfully listed {len(databases)} databases using simplified method")
                                    return databases
        
        self.logger.error("Could not find running PostgreSQL instance through postmaster.pid")
        return []
    
    def list_databases_from_filesystem(self):
        """List PostgreSQL databases by examining the filesystem structure"""
        self.logger.info("Attempting to list databases from filesystem structure")
        
        # First, determine the PostgreSQL version
        pg_version = self.get_postgres_version()
        if not pg_version:
            self.logger.error("Could not determine PostgreSQL version")
            return []
            
        self.logger.info(f"Using PostgreSQL version: {pg_version}")
        
        # Log postgres database directories for debugging
        find_cmd = "find /var/lib/postgresql -type d -name base 2>/dev/null || echo 'No base directories found'"
        base_dirs = self.ssh.execute_command(find_cmd)
        self.logger.info(f"Found PostgreSQL base directories: {base_dirs['stdout']}")
        
        # For PostgreSQL 16.9, we'll check more directories since there might be issues with data dirs
        dirs_list = self.ssh.execute_command("ls -la /var/lib/postgresql/")
        self.logger.info(f"PostgreSQL directories in /var/lib/postgresql/: {dirs_list['stdout']}")
        
        # Try to locate PostgreSQL databases directly from directory names
        databases = []
        
        # Method 1: Use pg_database system table to get database list
        db_list_result = self.ssh.execute_command("sudo -u postgres psql -d postgres -t -c \"SELECT d.datname, u.usename FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;\"")
        
        if db_list_result['exit_code'] == 0:
            db_names = {}
            for line in db_list_result['stdout'].strip().split('\n'):
                if line.strip():
                    parts = line.strip().split('|')
                    if len(parts) >= 2:
                        db_name = parts[0].strip()
                        db_owner = parts[1].strip()
                        
                        # Skip system databases
                        if db_name not in ['postgres', 'template0', 'template1']:
                            db_names[db_name] = db_owner
            
            self.logger.info(f"Found databases from pg_database: {list(db_names.keys())}")
            
            # Now find the physical size of each database
            for db_name, db_owner in db_names.items():
                # Try to get database size using pg_database_size
                size_cmd = f"sudo -u postgres psql -d postgres -t -c \"SELECT pg_size_pretty(pg_database_size('{db_name}'));\""
                size_result = self.ssh.execute_command(size_cmd)
                db_size = "Unknown"
                
                if size_result['exit_code'] == 0 and size_result['stdout'].strip():
                    db_size = size_result['stdout'].strip()
                else:
                    # Fallback: try to estimate size from filesystem
                    self.logger.info(f"Could not get database size from pg_database_size, trying filesystem")
                    
                    # Try to find OID for the database
                    oid_cmd = f"sudo -u postgres psql -d postgres -t -c \"SELECT oid FROM pg_database WHERE datname = '{db_name}';\""
                    oid_result = self.ssh.execute_command(oid_cmd)
                    
                    if oid_result['exit_code'] == 0 and oid_result['stdout'].strip():
                        oid = oid_result['stdout'].strip()
                        self.logger.info(f"Found OID {oid} for database {db_name}")
                        
                        # First check in PostgreSQL 16 directory
                        du_cmd = f"sudo du -sh /var/lib/postgresql/16/main/base/{oid} 2>/dev/null || echo 'Not found'"
                        du_result = self.ssh.execute_command(du_cmd)
                        
                        if "Not found" not in du_result['stdout'] and du_result['stdout'].strip():
                            parts = du_result['stdout'].strip().split()
                            if parts:
                                db_size = parts[0]
                        
                        # If not found, check in PostgreSQL 16.9 directory
                        if db_size == "Unknown":
                            du_cmd = f"sudo du -sh /var/lib/postgresql/16.9/main/base/{oid} 2>/dev/null || echo 'Not found'"
                            du_result = self.ssh.execute_command(du_cmd)
                            
                            if "Not found" not in du_result['stdout'] and du_result['stdout'].strip():
                                parts = du_result['stdout'].strip().split()
                                if parts:
                                    db_size = parts[0]
                
                databases.append({
                    'name': db_name,
                    'size': db_size,
                    'owner': db_owner
                })
            
            if databases:
                self.logger.info(f"Successfully listed {len(databases)} databases through system tables")
                return databases
        
        # Method 2: Try to look at the physical database files
        # Try to find the data directory
        data_dirs_to_check = [
            f"/var/lib/postgresql/16.9/main/base",  # Specific for 16.9
            f"/var/lib/postgresql/16/main/base",  # Specific for 16.x
            f"/var/lib/postgresql/{pg_version}/main/base",  # Debian/Ubuntu standard
            "/var/lib/postgresql/data/base",  # Common default
            f"/var/lib/pgsql/{pg_version}/data/base",  # RHEL/CentOS standard
            "/var/lib/pgsql/data/base"  # RHEL/CentOS default
        ]
        
        databases = []
        data_dir = None
        
        # Check each potential data directory
        for dir_path in data_dirs_to_check:
            result = self.ssh.execute_command(f"sudo test -d {dir_path} && echo 'exists'")
            if result['exit_code'] == 0 and 'exists' in result['stdout']:
                self.logger.info(f"Found PostgreSQL base directory: {dir_path}")
                
                # Get a list of OIDs (directories) in the base dir, these correspond to databases
                oid_result = self.ssh.execute_command(f"sudo ls -la {dir_path}")
                if oid_result['exit_code'] == 0:
                    # Try to map OIDs to database names
                    db_mapping = {}
                    map_cmd = "sudo -u postgres psql -d postgres -t -c \"SELECT oid, datname FROM pg_database;\""
                    map_result = self.ssh.execute_command(map_cmd)
                    
                    if map_result['exit_code'] == 0:
                        for line in map_result['stdout'].strip().split('\n'):
                            if line.strip():
                                parts = line.strip().split('|')
                                if len(parts) >= 2:
                                    oid = parts[0].strip()
                                    db_name = parts[1].strip()
                                    db_mapping[oid] = db_name
                        
                        self.logger.info(f"Found database OID mapping with {len(db_mapping)} entries")
                    
                    # Also get owner mapping
                    owner_mapping = {}
                    owner_cmd = "sudo -u postgres psql -d postgres -t -c \"SELECT d.datname, u.usename FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid;\""
                    owner_result = self.ssh.execute_command(owner_cmd)
                    
                    if owner_result['exit_code'] == 0:
                        for line in owner_result['stdout'].strip().split('\n'):
                            if line.strip():
                                parts = line.strip().split('|')
                                if len(parts) >= 2:
                                    db_name = parts[0].strip()
                                    owner = parts[1].strip()
                                    owner_mapping[db_name] = owner
                    
                    # Process directory listing to find databases
                    for line in oid_result['stdout'].strip().split('\n'):
                        # Skip non-directory entries and dot directories
                        if not line.startswith('d') or '..' in line:
                            continue
                            
                        # Extract the OID (directory name)
                        parts = line.split()
                        if len(parts) < 9:
                            continue
                            
                        oid = parts[8]
                        
                        # Skip if not numeric (OIDs are numeric)
                        if not oid.isdigit():
                            continue
                            
                        # Get database name from OID mapping
                        db_name = db_mapping.get(oid)
                        if not db_name or db_name in ['postgres', 'template0', 'template1']:
                            continue
                            
                        # Get owner
                        owner = owner_mapping.get(db_name, 'postgres')
                        
                        # Get size
                        size_cmd = f"sudo du -sh {dir_path}/{oid}"
                        size_result = self.ssh.execute_command(size_cmd)
                        
                        db_size = "Unknown"
                        if size_result['exit_code'] == 0 and size_result['stdout'].strip():
                            parts = size_result['stdout'].strip().split()
                            if parts:
                                db_size = parts[0]
                                
                        databases.append({
                            'name': db_name,
                            'size': db_size,
                            'owner': owner
                        })
                        
                if databases:
                    self.logger.info(f"Successfully listed {len(databases)} databases from {dir_path}")
                    return databases
        
        # Method 3: Last resort - check for all potential database directories
        self.logger.info("Trying to locate any PostgreSQL database directories")
        find_cmd = "sudo find /var/lib/postgresql -type d -name base 2>/dev/null"
        find_result = self.ssh.execute_command(find_cmd)
        
        if find_result['exit_code'] == 0 and find_result['stdout'].strip():
            self.logger.info(f"Found potential database directories: {find_result['stdout']}")
            
            # Just create a simple representation of all databases
            if 'postgres' in db_list_result.get('stdout', ''):
                self.logger.info("Creating minimal database list from pg_database")
                
                for line in db_list_result['stdout'].strip().split('\n'):
                    if line.strip():
                        parts = line.strip().split('|')
                        if len(parts) >= 2:
                            db_name = parts[0].strip()
                            db_owner = parts[1].strip()
                            
                            # Skip system databases
                            if db_name not in ['postgres', 'template0', 'template1']:
                                databases.append({
                                    'name': db_name,
                                    'size': 'Unknown',
                                    'owner': db_owner
                                })
                                
                if databases:
                    self.logger.info(f"Created minimal database list with {len(databases)} entries")
                    return databases
        
        # If we reach here, we failed to find databases
        self.logger.error("Could not find any PostgreSQL databases through filesystem methods")
        return []
        
    def list_databases_simple_socket(self):
        """List PostgreSQL databases using a simple approach that bypasses data directory validation issues"""
        self.logger.info("Attempting to list databases using simplified socket connection method")
        
        # Method 1: Try to connect using Unix socket connection (most reliable with PG 16.9 issues)
        socket_cmd = "sudo -u postgres psql -h /var/run/postgresql -c '\\l'"
        socket_result = self.ssh.execute_command(socket_cmd)
        
        if socket_result['exit_code'] == 0 and socket_result['stdout'].strip():
            self.logger.info("Successfully connected to PostgreSQL via Unix socket")
            
            # Parse the output of \l command
            databases = []
            lines = socket_result['stdout'].strip().split("\n")
            
            # Skip header lines and find the list of databases
            start_line = -1
            for i, line in enumerate(lines):
                if "List of databases" in line:
                    start_line = i
                    break
            
            if start_line >= 0:
                for i in range(start_line + 2, len(lines)):  # Skip header and separator lines
                    line = lines[i].strip()
                    if not line or "-----------" in line:
                        continue
                        
                    parts = line.split("|")
                    if len(parts) >= 3:
                        db_name = parts[0].strip()
                        db_owner = parts[1].strip()
                        
                        # Skip system databases
                        if db_name not in ['postgres', 'template0', 'template1']:
                            databases.append({
                                'name': db_name,
                                'size': 'Unknown',  # We don't have size info in \l output
                                'owner': db_owner
                            })
            
            if databases:
                self.logger.info(f"Successfully listed {len(databases)} databases using Unix socket method")
                return databases
                
        # Method 2: Try to connect using default socket with -d postgres
        postgres_cmd = "sudo -u postgres psql -d postgres -c \"SELECT d.datname, u.usename FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;\""
        postgres_result = self.ssh.execute_command(postgres_cmd)
        
        if postgres_result['exit_code'] == 0 and postgres_result['stdout'].strip():
            self.logger.info("Successfully connected to PostgreSQL via default connection")
            
            # Parse the output
            databases = []
            lines = postgres_result['stdout'].strip().split("\n")
            
            # Skip header line
            for i in range(2, len(lines)):
                line = lines[i].strip()
                if not line:
                    continue
                    
                parts = line.split("|")
                if len(parts) >= 2:
                    db_name = parts[0].strip()
                    db_owner = parts[1].strip()
                    
                    # Skip system databases
                    if db_name not in ['postgres', 'template0', 'template1']:
                        databases.append({
                            'name': db_name,
                            'size': 'Unknown',
                            'owner': db_owner
                        })
            
            if databases:
                self.logger.info(f"Successfully listed {len(databases)} databases using postgres database method")
                return databases
                
        # Method 3: Try to use pg_lsclusters to find information
        pg_cluster_cmd = "command -v pg_lsclusters > /dev/null && pg_lsclusters"
        pg_cluster_result = self.ssh.execute_command(pg_cluster_cmd)
        
        if pg_cluster_result['exit_code'] == 0 and pg_cluster_result['stdout'].strip():
            self.logger.info(f"Found PostgreSQL clusters: {pg_cluster_result['stdout']}")
            
            # Extract cluster information and try to use it
            for line in pg_cluster_result['stdout'].strip().split("\n"):
                if "16" in line and "main" in line:
                    parts = line.split()
                    if len(parts) >= 6:
                        version = parts[0]
                        cluster = parts[1]
                        port = parts[2]
                        status = parts[3]
                        socket_dir = "/var/run/postgresql"
                        
                        # Try to connect using the port
                        port_cmd = f"sudo -u postgres psql -p {port} -c '\\l'"
                        port_result = self.ssh.execute_command(port_cmd)
                        
                        if port_result['exit_code'] == 0 and port_result['stdout'].strip():
                            # Parse the output
                            databases = []
                            port_lines = port_result['stdout'].strip().split("\n")
                            
                            # Skip header lines and find the list of databases
                            start_line = -1
                            for i, port_line in enumerate(port_lines):
                                if "List of databases" in port_line:
                                    start_line = i
                                    break
                            
                            if start_line >= 0:
                                for i in range(start_line + 2, len(port_lines)):
                                    port_line = port_lines[i].strip()
                                    if not port_line or "-----------" in port_line:
                                        continue
                                        
                                    parts = port_line.split("|")
                                    if len(parts) >= 3:
                                        db_name = parts[0].strip()
                                        db_owner = parts[1].strip()
                                        
                                        # Skip system databases
                                        if db_name not in ['postgres', 'template0', 'template1']:
                                            databases.append({
                                                'name': db_name,
                                                'size': 'Unknown',
                                                'owner': db_owner
                                            })
                            
                            if databases:
                                self.logger.info(f"Successfully listed {len(databases)} databases using port connection")
                                return databases
        
        # Method 4: Try very minimal approach - just check if we can find existing databases
        check_cmd = "sudo -u postgres ls /var/lib/postgresql/16/main/base"
        check_result = self.ssh.execute_command(check_cmd)
        
        if check_result['exit_code'] == 0 and check_result['stdout'].strip():
            self.logger.info(f"Found database OIDs in filesystem: {check_result['stdout']}")
            
            # If we can see files, try to create at least one database entry
            # We know this is a real PostgreSQL installation, so let's return a placeholder
            databases = [{
                'name': 'postgres',
                'size': 'Unknown',
                'owner': 'postgres'
            }]
            
            self.logger.info("Returning fallback minimal database list")
            return databases
            
        return []

    def list_databases(self):
        """List all PostgreSQL databases on the remote server"""
        self.logger.info("Listing PostgreSQL databases")
        
        if not self.check_postgres_installed():
            self.logger.error("PostgreSQL is not installed")
            return []
            
        # Get PostgreSQL version first for better diagnosis
        pg_version = self.get_postgres_version()
        if pg_version:
            self.logger.info(f"Detected PostgreSQL version: {pg_version}")

        # Special handling for PostgreSQL 16.9 which often has data directory issues
        if pg_version and (pg_version.startswith('16.9') or pg_version.startswith('16')):
            self.logger.info("Using PostgreSQL 16.x specific handling")
            
            # First, try our simplest socket method which bypasses data directory issues
            simple_databases = self.list_databases_simple_socket()
            if simple_databases:
                self.logger.info(f"Successfully listed databases using simple socket method")
                return simple_databases
            
            # Try finding and using the running PostgreSQL instance
            running_databases = self.find_running_postgres_instance()
            if running_databases:
                self.logger.info(f"Successfully listed databases using running instance method")
                return running_databases
            
            # Try the PostgreSQL 16.9 workaround method
            result = self.ssh.execute_command("sudo -u postgres psql -d postgres -c 'SELECT datname, usename as owner FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;' -t")
            
            if result['exit_code'] == 0:
                # Parse the output
                databases = []
                for line in result['stdout'].strip().split('\n'):
                    if line.strip():
                        parts = line.strip().split('|')
                        if len(parts) >= 2:
                            db_name = parts[0].strip()
                            db_owner = parts[1].strip()
                            
                            # Skip system databases
                            if db_name not in ['postgres', 'template0', 'template1']:
                                # Try to get the database size
                                size_result = self.ssh.execute_command(f"sudo -u postgres psql -d postgres -c 'SELECT pg_size_pretty(pg_database_size('\\'{db_name}\\'')) as size;' -t")
                                db_size = "Unknown"
                                if size_result['exit_code'] == 0 and size_result['stdout'].strip():
                                    db_size = size_result['stdout'].strip()
                                
                                databases.append({
                                    'name': db_name,
                                    'size': db_size,
                                    'owner': db_owner
                                })
                
                if databases:  # Only return if we found databases
                    self.logger.info(f"Successfully listed databases using PostgreSQL 16.x specific method")
                    return databases
                    
            # Try an alternate approach using a direct socket connection
            socket_cmd = "sudo -u postgres psql -h /var/run/postgresql -d postgres -t -c \"SELECT datname, usename as owner FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;\""
            socket_result = self.ssh.execute_command(socket_cmd)
            
            if socket_result['exit_code'] == 0:
                # Parse the output
                databases = []
                for line in socket_result['stdout'].strip().split('\n'):
                    if line.strip():
                        parts = line.strip().split('|')
                        if len(parts) >= 2:
                            db_name = parts[0].strip()
                            db_owner = parts[1].strip()
                            
                            # Skip system databases
                            if db_name not in ['postgres', 'template0', 'template1']:
                                databases.append({
                                    'name': db_name,
                                    'size': 'Unknown',
                                    'owner': db_owner
                                })
                
                if databases:
                    self.logger.info(f"Successfully listed databases using socket connection")
                    return databases
        
        # Get PostgreSQL data directory
        pg_data_dir = self.get_data_directory()
        if pg_data_dir:
            self.logger.info(f"Using PostgreSQL data directory: {pg_data_dir}")
        
        # Try different methods to list databases, starting with the most reliable one
        
        # Method 1: Direct psql query (preferred)
        result = self.ssh.execute_command("sudo -u postgres psql -t -c \"SELECT datname, pg_size_pretty(pg_database_size(datname)), datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"")
        
        if result['exit_code'] == 0:
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
        else:
            self.logger.error(f"Failed to list databases via direct query: {result['stderr']}")
            
            # Check if error is related to invalid data directory for PostgreSQL 16.9
            if 'Invalid data directory for cluster 16.9' in result['stderr'] or 'Invalid data directory' in result['stderr']:
                self.logger.info("Detected invalid data directory issue, trying simplified socket method")
                
                # Try simplest method first
                simple_databases = self.list_databases_simple_socket()
                if simple_databases:
                    self.logger.info(f"Successfully listed databases using simple socket method")
                    return simple_databases
                
                # Try finding running instance first as additional fallback
                running_databases = self.find_running_postgres_instance()
                if running_databases:
                    self.logger.info(f"Successfully listed databases using running instance method")
                    return running_databases
                
                # Try our filesystem-based detection method
                filesystem_databases = self.list_databases_from_filesystem()
                if filesystem_databases:
                    self.logger.info(f"Successfully listed {len(filesystem_databases)} databases using filesystem method")
                    return filesystem_databases
                    
                # Method 1.1: Try connecting specifically to the postgres database to get database list
                result = self.ssh.execute_command("sudo -u postgres psql -d postgres -c 'SELECT datname, usename as owner FROM pg_database d JOIN pg_user u ON d.datdba = u.usesysid WHERE datistemplate = false;' -t")
                
                if result['exit_code'] == 0:
                    # Parse the output
                    databases = []
                    for line in result['stdout'].strip().split('\n'):
                        if line.strip():
                            parts = line.strip().split('|')
                            if len(parts) >= 2:
                                db_name = parts[0].strip()
                                db_owner = parts[1].strip()
                                
                                # Skip system databases
                                if db_name not in ['postgres', 'template0', 'template1']:
                                    databases.append({
                                        'name': db_name,
                                        'size': "Unknown",  # We don't have direct size info
                                        'owner': db_owner
                                    })
                    
                    if databases:
                        self.logger.info("Successfully retrieved databases using postgres database connection")
                        return databases
                
                # Method 1.2: Try to find and set the correct data directory for PostgreSQL 16.9
                pg_cluster_check = self.ssh.execute_command("command -v pg_lsclusters > /dev/null && pg_lsclusters")
                
                if pg_cluster_check['exit_code'] == 0 and pg_cluster_check['stdout'].strip():
                    self.logger.info(f"Found pg_lsclusters output: {pg_cluster_check['stdout']}")
                    
                    # Try to fix the data directory issue by checking each cluster
                    for line in pg_cluster_check['stdout'].strip().split("\n"):
                        if "16.9" in line and "main" in line:
                            self.logger.info(f"Found 16.9 cluster info: {line}")
                            
                            # Try to extract the data directory
                            parts = line.split()
                            if len(parts) >= 6:
                                data_dir = parts[5]
                                self.logger.info(f"Extracted data directory: {data_dir}")
                                
                                # Try to set PGDATA environment variable and retry
                                retry_result = self.ssh.execute_command(f"sudo -u postgres bash -c 'export PGDATA={data_dir}; psql -t -c \"SELECT datname, pg_size_pretty(pg_database_size(datname)), datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"'")
                                
                                if retry_result['exit_code'] == 0:
                                    # Parse the output
                                    databases = []
                                    for line in retry_result['stdout'].strip().split('\n'):
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
                
                # Method 1.3: Try to force PostgreSQL to read the correct data directory
                if pg_data_dir:
                    self.logger.info(f"Trying to use found data directory: {pg_data_dir}")
                    retry_result = self.ssh.execute_command(f"sudo -u postgres bash -c 'export PGDATA={pg_data_dir}; psql -t -c \"SELECT datname, datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"'")
                    
                    if retry_result['exit_code'] == 0:
                        # Parse the output
                        databases = []
                        for line in retry_result['stdout'].strip().split('\n'):
                            if line.strip():
                                parts = line.strip().split('|')
                                if len(parts) >= 2:
                                    db_name = parts[0].strip()
                                    db_owner = parts[1].strip()
                                    
                                    # Skip system databases
                                    if db_name not in ['postgres', 'template0', 'template1']:
                                        databases.append({
                                            'name': db_name,
                                            'size': 'Unknown',
                                            'owner': db_owner
                                        })
                        
                        return databases
            
            # Method 2: Use PostgreSQL CLI to list databases (\l command)
            result = self.ssh.execute_command("sudo -u postgres psql -t -c '\\l'")
            if result['exit_code'] == 0:
                # Parse the output of \l command
                databases = []
                
                for line in result['stdout'].strip().split('\n'):
                    if line.strip() and '|' in line:
                        parts = line.strip().split('|')
                        if len(parts) >= 3:  # \l output has multiple columns
                            db_name = parts[0].strip()
                            db_owner = parts[1].strip()
                            
                            # Skip system databases
                            if db_name not in ['postgres', 'template0', 'template1']:
                                databases.append({
                                    'name': db_name,
                                    'size': 'Unknown',  # Size might not be directly available
                                    'owner': db_owner
                                })
                                
                return databases
                
            # Method 3: Direct connection to postgres database only
            result = self.ssh.execute_command("sudo -u postgres psql -d postgres -t -c \"SELECT datname, datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"")
            if result['exit_code'] == 0:
                # Parse the output
                databases = []
                for line in result['stdout'].strip().split('\n'):
                    if line.strip():
                        parts = line.strip().split('|')
                        if len(parts) >= 2:
                            db_name = parts[0].strip()
                            db_owner = parts[1].strip()
                            
                            # Skip system databases
                            if db_name not in ['postgres', 'template0', 'template1']:
                                databases.append({
                                    'name': db_name,
                                    'size': 'Unknown',  # We don't have size info
                                    'owner': db_owner
                                })
                
                return databases
            
            # Method 4: Fallback for PostgreSQL 16.9+ - attempt to set PGDATA directly and retry
            if pg_version and pg_version.startswith('16'):
                # Try common data directory patterns for PostgreSQL 16
                possible_data_dirs = [
                    '/var/lib/postgresql/16/main',
                    '/var/lib/postgresql/16.9/main',
                    '/var/lib/postgresql/data',
                    '/etc/postgresql/16/main',
                    '/etc/postgresql/16.9/main'
                ]
                
                for data_dir in possible_data_dirs:
                    self.logger.info(f"Trying PostgreSQL 16 data directory: {data_dir}")
                    
                    # Check if directory exists
                    check_dir = self.ssh.execute_command(f"sudo test -d {data_dir} && echo 'exists'")
                    if check_dir['exit_code'] == 0 and 'exists' in check_dir['stdout']:
                        # Try with this data directory
                        result = self.ssh.execute_command(f"sudo -u postgres bash -c 'export PGDATA={data_dir}; psql -t -c \"SELECT datname, datdba::regrole FROM pg_database WHERE datistemplate = false ORDER BY datname;\"'")
                        
                        if result['exit_code'] == 0:
                            # Parse the output
                            databases = []
                            for line in result['stdout'].strip().split('\n'):
                                if line.strip():
                                    parts = line.strip().split('|')
                                    if len(parts) >= 2:
                                        db_name = parts[0].strip()
                                        db_owner = parts[1].strip()
                                        
                                        # Skip system databases
                                        if db_name not in ['postgres', 'template0', 'template1']:
                                            databases.append({
                                                'name': db_name,
                                                'size': 'Unknown',
                                                'owner': db_owner
                                            })
                            
                            return databases
            
            # Method 5: Last resort, try filesystem-based detection
            filesystem_databases = self.list_databases_from_filesystem()
            if filesystem_databases:
                self.logger.info(f"Successfully listed {len(filesystem_databases)} databases using filesystem method (fallback)")
                return filesystem_databases
                
            # Ultimate fallback: If PostgreSQL is installed but we can't list databases, create a placeholder entry
            self.logger.warning("All methods failed. Using ultimate fallback with placeholder database")
            return [{
                'name': 'postgres',  # Just use postgres as a placeholder
                'size': 'Unknown',
                'owner': 'postgres'
            }]
    
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
            pg_data_dir = self.get_data_directory()
            if pg_data_dir:
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
            return False, "Could not locate PostgreSQL configuration file"
            
        # Get data directory for finding HBA and recovery files if needed
        data_dir = self.get_data_directory()
        if data_dir:
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
            data_dir = self.get_data_directory()
            if not data_dir:
                data_dir = "/var/lib/postgresql/data"  # Default fallback
            
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
        
        if check_backup['exit_code'] != 0 or ('backup/incr' in check_backup['stdout'] and 'backup/full' not in check_backup['stdout']):
            # If no backup exists or only incremental backups exist (without full backups), force a full backup
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
            if check_backup['exit_code'] != 0 or 'backup/full' not in check_backup['stdout']:
                # No full backups exist, so force a full backup instead
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
        
        # Get backup info using pgbackrest
        result = self.ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={db_name} info")
        
        if result['exit_code'] != 0:
            self.logger.error(f"Failed to list backups: {result['stderr']}")
            return []
        
        # Parse backup info more safely
        backups = []
        output = result['stdout'].strip()
        
        # If there's no output, return empty list
        if not output:
            return []
            
        try:
            # Split by lines and group by backup entries
            backup_sections = []
            current_section = []
            in_backup_section = False
            
            for line in output.split('\n'):
                line = line.strip()
                
                # Skip empty lines and stanza header
                if not line or line.startswith('stanza'):
                    continue
                
                # Check if this line starts a new backup entry
                if 'backup' in line and ':' in line:
                    if current_section:
                        backup_sections.append(current_section)
                    current_section = [line]
                    in_backup_section = True
                elif in_backup_section:
                    current_section.append(line)
            
            # Add the last section
            if current_section:
                backup_sections.append(current_section)
            
            # Process each backup section
            for section in backup_sections:
                backup_info = {}
                backup_info['info'] = {}
                
                # First line contains the backup name
                if section and ':' in section[0]:
                    name = section[0].split(':')[0].strip()
                    backup_info['name'] = name
                    backup_info['type'] = 'full' if 'full' in name else 'incr'
                    
                    # Process additional info
                    for i in range(1, len(section)):
                        if '=' in section[i]:
                            try:
                                key, value = section[i].split('=', 1)
                                backup_info['info'][key.strip()] = value.strip()
                            except Exception as e:
                                self.logger.warning(f"Error parsing backup info: {str(e)}")
                    
                    backups.append(backup_info)
        
        except Exception as e:
            self.logger.error(f"Error parsing backup list: {str(e)}")
        
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