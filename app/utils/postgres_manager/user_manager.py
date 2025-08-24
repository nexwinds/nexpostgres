"""User management for PostgreSQL."""

import logging
from typing import Dict, List, Tuple
from .system_utils import SystemUtils

class PostgresUserManager:
    """Manages PostgreSQL users and permissions."""
    
    def __init__(self, ssh_manager, system_utils: SystemUtils, logger=None):
        self.ssh = ssh_manager
        self.system_utils = system_utils
        self.logger = logger or logging.getLogger(__name__)
    
    def user_exists(self, username: str) -> bool:
        """Check if a PostgreSQL user exists.
        
        Args:
            username: Username to check
            
        Returns:
            bool: True if user exists
        """
        result = self.system_utils.execute_postgres_sql(
            f"SELECT 1 FROM pg_roles WHERE rolname = '{username}';"
        )
        # Check if query succeeded and returned the value '1' (indicating user exists)
        return result['exit_code'] == 0 and result['stdout'] and '1' in result['stdout'] and '(1 row)' in result['stdout']
    
    def create_user(self, username: str, password: str) -> Tuple[bool, str]:
        """Create a PostgreSQL user.
        
        Args:
            username: Username to create
            password: Password for the user
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating PostgreSQL user: {username}")
        
        if self.user_exists(username):
            return False, f"User '{username}' already exists"
        
        result = self.system_utils.execute_postgres_sql(
            f"CREATE USER {username} WITH ENCRYPTED PASSWORD '{password}';"
        )
        
        if result['exit_code'] == 0:
            return True, f"User '{username}' created successfully"
        else:
            return False, f"Failed to create user '{username}': {result.get('stderr', 'Unknown error')}"
    
    def update_user_password(self, username: str, password: str) -> Tuple[bool, str]:
        """Update a PostgreSQL user's password.
        
        Args:
            username: Username to update
            password: New password
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Updating password for user: {username}")
        
        if not self.user_exists(username):
            return False, f"User '{username}' does not exist"
        
        result = self.system_utils.execute_postgres_sql(
            f"ALTER USER {username} WITH ENCRYPTED PASSWORD '{password}';"
        )
        
        if result['exit_code'] == 0:
            return True, f"Password updated for user '{username}'"
        else:
            return False, f"Failed to update password for user '{username}': {result.get('stderr', 'Unknown error')}"
    
    def delete_user(self, username: str) -> Tuple[bool, str]:
        """Delete a PostgreSQL user.
        
        Args:
            username: Username to delete
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Deleting PostgreSQL user: {username}")
        
        if not self.user_exists(username):
            return False, f"User '{username}' does not exist"
        
        result = self.system_utils.execute_postgres_sql(
            f"DROP USER IF EXISTS {username};"
        )
        
        if result['exit_code'] == 0:
            return True, f"User '{username}' deleted successfully"
        else:
            return False, f"Failed to delete user '{username}': {result.get('stderr', 'Unknown error')}"
    
    def grant_database_permissions(self, username: str, db_name: str, 
                                 permission_level: str) -> Tuple[bool, str]:
        """Grant database permissions to a user.
        
        Args:
            username: Username to grant permissions to
            db_name: Database name
            permission_level: 'no_access', 'read_only', or 'read_write'
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Granting {permission_level} permissions to {username} on {db_name}")
        
        if not self.user_exists(username):
            return False, f"User '{username}' does not exist"
        
        if permission_level == 'no_access':
            return self._revoke_all_permissions(username, db_name)
        elif permission_level == 'read_only':
            return self._grant_read_only_permissions(username, db_name)
        elif permission_level == 'read_write':
            return self._grant_read_write_permissions(username, db_name)
        elif permission_level == 'all_permissions':
            return self._grant_all_permissions(username, db_name)
        else:
            return False, f"Invalid permission level: {permission_level}"
    
    def grant_individual_permissions(self, username: str, db_name: str, permissions: dict) -> Tuple[bool, str]:
        """Grant individual permissions to a user on a database.
        
        Args:
            username: Username to grant permissions to
            db_name: Database name
            permissions: Dict with keys: connect, select, insert, update, delete, create
        
        Returns:
            Tuple of (success, message)
        """

        
        if not self.user_exists(username):
            self.logger.error(f"DEBUG: User '{username}' does not exist")
            return False, f"User '{username}' does not exist"
        
        # First revoke all permissions to start clean

        success, message = self._revoke_all_permissions(username, db_name)
        if not success:
            self.logger.error(f"DEBUG: Failed to revoke permissions: {message}")
            return success, message
        
        # If no permissions are granted, just return (user has no access)
        if not any(permissions.values()):

            return True, f"All permissions revoked for user '{username}' on database '{db_name}'"
        
        commands = []
        db_commands = []
        
        # Grant CONNECT permission if requested
        if permissions.get('connect', False):

            commands.append(f"GRANT CONNECT ON DATABASE {db_name} TO {username};")
            db_commands.append(f"GRANT USAGE ON SCHEMA public TO {username};")
        
        # Grant CREATE permission if requested
        if permissions.get('create', False):

            commands.append(f"GRANT CREATE ON DATABASE {db_name} TO {username};")
            db_commands.append(f"GRANT CREATE ON SCHEMA public TO {username};")
        
        # Grant table-level permissions
        table_permissions = []
        if permissions.get('select', False):
            table_permissions.append('SELECT')
        if permissions.get('insert', False):
            table_permissions.append('INSERT')
        if permissions.get('update', False):
            table_permissions.append('UPDATE')
        if permissions.get('delete', False):
            table_permissions.append('DELETE')
        

        
        if table_permissions:
            perm_str = ', '.join(table_permissions)
            db_commands.extend([
                f"GRANT {perm_str} ON ALL TABLES IN SCHEMA public TO {username};",
                f"GRANT {perm_str} ON ALL SEQUENCES IN SCHEMA public TO {username};",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT {perm_str} ON TABLES TO {username};",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT {perm_str} ON SEQUENCES TO {username};"
            ])
        
        # Execute general commands
        for cmd in commands:
            result = self.system_utils.execute_postgres_sql(cmd)
            if result['exit_code'] != 0:
                return False, f"Failed to execute: {cmd}"
        
        # Execute database-specific commands
        for cmd in db_commands:
            result = self.system_utils.execute_postgres_sql(cmd, db_name)
            if result['exit_code'] != 0:
                # For CREATE permissions, treat failures as errors since they're critical
                if 'CREATE' in cmd:
                    return False, f"Failed to grant CREATE permission: {result.get('stderr', 'Unknown error')}"
                else:
                    self.logger.warning(f"Database command failed (continuing): {cmd} - Error: {result.get('stderr', 'Unknown error')}")
        
        granted_perms = [k for k, v in permissions.items() if v]
        return True, f"Granted permissions ({', '.join(granted_perms)}) to user '{username}' on database '{db_name}'"
    
    def _revoke_all_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Revoke all permissions from a user on a database."""
        commands = [
            f"REVOKE ALL PRIVILEGES ON DATABASE {db_name} FROM {username};",
            f"REVOKE CONNECT ON DATABASE {db_name} FROM {username};"
        ]
        
        # Execute commands on the specific database
        db_commands = [
            f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {username};",
            f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {username};",
            f"REVOKE ALL PRIVILEGES ON SCHEMA public FROM {username};"
        ]
        
        # Execute general commands
        for cmd in commands:
            result = self.system_utils.execute_postgres_sql(cmd)
            if result['exit_code'] != 0:
                self.logger.warning(f"Command failed (continuing): {cmd}")
        
        # Execute database-specific commands
        for cmd in db_commands:
            result = self.system_utils.execute_postgres_sql(cmd, db_name)
            if result['exit_code'] != 0:
                self.logger.warning(f"Command failed (continuing): {cmd}")
        
        return True, f"Revoked all permissions for user '{username}' on database '{db_name}'"
    
    def _grant_read_only_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Grant read-only permissions to a user on a database."""
        commands = [
            f"REVOKE ALL ON DATABASE {db_name} FROM {username};",
            f"GRANT CONNECT ON DATABASE {db_name} TO {username};"
        ]
        
        db_commands = [
            f"REVOKE ALL ON SCHEMA public FROM {username};",
            f"GRANT USAGE ON SCHEMA public TO {username};",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {username};",
            f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {username};",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {username};",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO {username};"
        ]
        
        # Execute general commands
        for cmd in commands:
            result = self.system_utils.execute_postgres_sql(cmd)
            if result['exit_code'] != 0:
                return False, f"Failed to execute: {cmd}"
        
        # Execute database-specific commands
        for cmd in db_commands:
            result = self.system_utils.execute_postgres_sql(cmd, db_name)
            if result['exit_code'] != 0:
                self.logger.warning(f"Command failed (continuing): {cmd}")
        
        return True, f"Granted read-only permissions to user '{username}' on database '{db_name}'"
    
    def _grant_read_write_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Grant read-write permissions to a user on a database."""
        commands = [
            f"REVOKE ALL ON DATABASE {db_name} FROM {username};",
            f"GRANT CONNECT ON DATABASE {db_name} TO {username};"
        ]
        
        db_commands = [
            f"REVOKE ALL ON SCHEMA public FROM {username};",
            f"GRANT USAGE ON SCHEMA public TO {username};",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {username};",
            f"GRANT SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {username};",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {username};",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, UPDATE ON SEQUENCES TO {username};"
        ]
        
        # Execute general commands
        for cmd in commands:
            result = self.system_utils.execute_postgres_sql(cmd)
            if result['exit_code'] != 0:
                return False, f"Failed to execute: {cmd}"
        
        # Execute database-specific commands
        for cmd in db_commands:
            result = self.system_utils.execute_postgres_sql(cmd, db_name)
            if result['exit_code'] != 0:
                self.logger.warning(f"Command failed (continuing): {cmd}")
        
        return True, f"Granted read-write permissions to user '{username}' on database '{db_name}'"

    def _grant_all_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Grant all permissions to a user on a database."""
        commands = [
            f"REVOKE ALL ON DATABASE {db_name} FROM {username};",
            f"GRANT CONNECT ON DATABASE {db_name} TO {username};",
            f"GRANT CREATE ON DATABASE {db_name} TO {username};"
        ]
        
        db_commands = [
            f"REVOKE ALL ON SCHEMA public FROM {username};",
            f"GRANT ALL PRIVILEGES ON SCHEMA public TO {username};",
            f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {username};",
            f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {username};",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO {username};",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO {username};"
        ]
        
        # Execute general commands
        for cmd in commands:
            result = self.system_utils.execute_postgres_sql(cmd)
            if result['exit_code'] != 0:
                return False, f"Failed to execute: {cmd}"
        
        # Execute database-specific commands
        for cmd in db_commands:
            result = self.system_utils.execute_postgres_sql(cmd, db_name)
            if result['exit_code'] != 0:
                self.logger.warning(f"Command failed (continuing): {cmd}")
        
        return True, f"Granted all permissions to user '{username}' on database '{db_name}'"
    
    def create_database_user(self, username: str, password: str, db_name: str, 
                           permission_level: str = 'read_write') -> Tuple[bool, str]:
        """Create a PostgreSQL user with specified permissions on a database.
        
        Args:
            username: Username to create
            password: Password for the user
            db_name: Database name to grant permissions on
            permission_level: 'no_access', 'read_only', or 'read_write'
            
        Returns:
            tuple: (success, message)
        """
        self.logger.info(f"Creating database user {username} for database {db_name} with {permission_level} permissions")
        
        # Create or update user
        user_existed = self.user_exists(username)
        if user_existed:
            self.logger.info(f"User {username} already exists, updating password")
            success, message = self.update_user_password(username, password)
            if not success:
                return False, message
        else:
            self.logger.info(f"Creating new user {username}")
            success, message = self.create_user(username, password)
            if not success:
                return False, message
            
            # Verify user was created successfully
            if not self.user_exists(username):
                return False, f"User '{username}' was not created successfully"
        
        # Grant permissions
        success, perm_message = self.grant_database_permissions(username, db_name, permission_level)
        if not success:
            return False, perm_message
        
        return True, f"User '{username}' configured with {permission_level} access to database '{db_name}'"
    
    def list_database_users(self, db_name: str) -> List[Dict[str, str]]:
        """List all PostgreSQL users with access to a specific database.
        
        Args:
            db_name: Database name to check users for
            
        Returns:
            list: List of users with their roles/permissions
        """
        self.logger.info(f"Listing users for database {db_name}")
        
        # Check privileges to determine permission level
        # Updated to properly detect CREATE permissions for new permission combinations
        query = f"""
        SELECT r.rolname as username,
               CASE WHEN r.rolsuper THEN 'all_permissions'
                    WHEN has_database_privilege(r.rolname, '{db_name}', 'CREATE') AND EXISTS (
                        SELECT 1 FROM pg_tables pt
                        WHERE pt.schemaname = 'public'
                        AND has_table_privilege(r.rolname, pt.schemaname||'.'||pt.tablename, 'INSERT')
                        LIMIT 1
                    ) THEN 'all_permissions'
                    WHEN EXISTS (
                        SELECT 1 FROM pg_tables pt
                        WHERE pt.schemaname = 'public'
                        AND has_table_privilege(r.rolname, pt.schemaname||'.'||pt.tablename, 'INSERT')
                        LIMIT 1
                    ) THEN 'read_write'
                    WHEN has_database_privilege(r.rolname, '{db_name}', 'CONNECT') THEN 'read_only'
                    ELSE 'no_access'
               END as permission_level
        FROM pg_roles r
        WHERE r.rolcanlogin = true
          AND r.rolname != 'postgres'
        ORDER BY r.rolname;
        """
        
        result = self.system_utils.execute_postgres_sql(query, db_name)
        
        users = []
        if result['exit_code'] == 0 and result['stdout'].strip():
            for line in result['stdout'].strip().split('\n'):
                if line.strip():
                    parts = line.strip().split('|')
                    if len(parts) >= 2:
                        username = parts[0].strip()
                        permission = parts[1].strip()
                        
                        users.append({
                            'username': username,
                            'permission_level': permission
                        })
        
        return users
    
    def get_user_permissions(self, username: str, db_name: str) -> str:
        """Get the permission level of a user on a specific database.
        
        Args:
            username: Username to check
            db_name: Database name
            
        Returns:
            str: Permission level ('superuser', 'read_write', 'read_only', 'no_access')
        """
        if not self.user_exists(username):
            return 'no_access'
    
    def get_user_individual_permissions(self, username: str, db_name: str) -> Dict[str, bool]:
        """Get detailed individual permissions for a user on a specific database.
        
        Args:
            username: Username to check
            db_name: Database name
            
        Returns:
            dict: Dictionary with individual permission flags
        """
        if not self.user_exists(username):
            return {
                'connect': False,
                'select': False,
                'insert': False,
                'update': False,
                'delete': False,
                'create': False
            }
        

        
        # Improved permission detection using more reliable methods
        # Check database-level permissions first
        db_query = f"""
        SELECT 
            has_database_privilege('{username}', '{db_name}', 'CONNECT') as connect_priv,
            has_database_privilege('{username}', '{db_name}', 'CREATE') as create_priv;
        """
        
        db_result = self.system_utils.execute_postgres_sql(db_query, db_name)
        connect_perm = False
        create_perm = False
        
        if db_result['exit_code'] == 0 and db_result['stdout'].strip():

            lines = db_result['stdout'].strip().split('\n')
            for line in lines:
                line = line.strip()
                if '|' in line and ('t' in line or 'f' in line) and not line.startswith('-') and 'priv' not in line:
                    parts = line.split('|')
                    if len(parts) >= 2:
                        connect_perm = parts[0].strip().lower() == 't'
                        create_perm = parts[1].strip().lower() == 't'
                        break
        
        # Check table-level permissions using a more accurate approach
        # First check if there are any tables in the public schema
        table_check_query = "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';"
        table_result = self.system_utils.execute_postgres_sql(table_check_query, db_name)
        
        has_tables = False
        if table_result['exit_code'] == 0 and table_result['stdout'].strip():
            try:
                table_count = int(table_result['stdout'].strip().split('\n')[-2].strip())
                has_tables = table_count > 0
            except (ValueError, IndexError):
                has_tables = False
        

        
        # Initialize table permissions
        select_perm = False
        insert_perm = False
        update_perm = False
        delete_perm = False
        
        if has_tables:
            # Check permissions on existing tables
            table_perms_query = f"""
            SELECT 
                bool_or(has_table_privilege('{username}', schemaname||'.'||tablename, 'SELECT')) as select_priv,
                bool_or(has_table_privilege('{username}', schemaname||'.'||tablename, 'INSERT')) as insert_priv,
                bool_or(has_table_privilege('{username}', schemaname||'.'||tablename, 'UPDATE')) as update_priv,
                bool_or(has_table_privilege('{username}', schemaname||'.'||tablename, 'DELETE')) as delete_priv
            FROM pg_tables 
            WHERE schemaname = 'public';
            """
        else:
            # Check default privileges for future tables
            table_perms_query = f"""
            SELECT 
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    JOIN pg_roles r ON da.defaclrole = r.oid
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND ('{username}' = ANY(string_to_array(array_to_string(da.defaclacl, ','), ','))
                         AND array_to_string(da.defaclacl, ',') LIKE '%{username}=%r%')
                ) THEN 't' ELSE 'f' END as select_priv,
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    JOIN pg_roles r ON da.defaclrole = r.oid
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND ('{username}' = ANY(string_to_array(array_to_string(da.defaclacl, ','), ','))
                         AND array_to_string(da.defaclacl, ',') LIKE '%{username}=%a%')
                ) THEN 't' ELSE 'f' END as insert_priv,
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    JOIN pg_roles r ON da.defaclrole = r.oid
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND ('{username}' = ANY(string_to_array(array_to_string(da.defaclacl, ','), ','))
                         AND array_to_string(da.defaclacl, ',') LIKE '%{username}=%w%')
                ) THEN 't' ELSE 'f' END as update_priv,
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    JOIN pg_roles r ON da.defaclrole = r.oid
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND ('{username}' = ANY(string_to_array(array_to_string(da.defaclacl, ','), ','))
                         AND array_to_string(da.defaclacl, ',') LIKE '%{username}=%d%')
                ) THEN 't' ELSE 'f' END as delete_priv;
            """
        
        table_result = self.system_utils.execute_postgres_sql(table_perms_query, db_name)
        
        if table_result['exit_code'] == 0 and table_result['stdout'].strip():

            lines = table_result['stdout'].strip().split('\n')
            for line in lines:
                line = line.strip()
                if '|' in line and ('t' in line or 'f' in line) and not line.startswith('-') and 'priv' not in line:
                    parts = line.split('|')
                    if len(parts) >= 4:
                        select_perm = parts[0].strip().lower() == 't'
                        insert_perm = parts[1].strip().lower() == 't'
                        update_perm = parts[2].strip().lower() == 't'
                        delete_perm = parts[3].strip().lower() == 't'
                        break
        
        permissions = {
            'connect': connect_perm,
            'select': select_perm,
            'insert': insert_perm,
            'update': update_perm,
            'delete': delete_perm,
            'create': create_perm
        }
        

        return permissions