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
        return result['exit_code'] == 0 and result['stdout'].strip()
    
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
        else:
            return False, f"Invalid permission level: {permission_level}"
    
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
            f"GRANT CONNECT ON DATABASE {db_name} TO {username};"
        ]
        
        db_commands = [
            f"GRANT USAGE ON SCHEMA public TO {username};",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {username};",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {username};",
            f"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM {username};"
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
            f"GRANT CONNECT ON DATABASE {db_name} TO {username};",
            f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {username};"
        ]
        
        db_commands = [
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
        
        return True, f"Granted read-write permissions to user '{username}' on database '{db_name}'"
    
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
        if self.user_exists(username):
            success, message = self.update_user_password(username, password)
            if not success:
                return False, message
        else:
            success, message = self.create_user(username, password)
            if not success:
                return False, message
        
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
        
        query = f"""
        SELECT r.rolname as username,
               CASE WHEN r.rolsuper THEN 'superuser'
                    WHEN has_database_privilege(r.rolname, '{db_name}', 'CREATE') THEN 'read_write'
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
        
        query = f"""
        SELECT CASE WHEN r.rolsuper THEN 'superuser'
                    WHEN has_database_privilege('{username}', '{db_name}', 'CREATE') THEN 'read_write'
                    WHEN has_database_privilege('{username}', '{db_name}', 'CONNECT') THEN 'read_only'
                    ELSE 'no_access'
               END as permission_level
        FROM pg_roles r
        WHERE r.rolname = '{username}';
        """
        
        result = self.system_utils.execute_postgres_sql(query, db_name)
        
        if result['exit_code'] == 0 and result['stdout'].strip():
            return result['stdout'].strip()
        
        return 'no_access'