"""Database service layer for handling PostgreSQL operations.

This module provides a service layer that abstracts common database operations
and reduces code duplication in the routes.
"""

import time
from typing import Tuple, Optional, Dict, Any
from flask import flash
from app.models.database import VpsServer, RestoreLog, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.utils.backup_service import BackupService
from app.utils.permission_manager import PermissionManager


class DatabaseService:
    """Service class for database operations."""
    

    
    @staticmethod
    def create_postgres_manager(ssh: SSHManager) -> Optional[PostgresManager]:
        """Create PostgreSQL manager and verify installation.
        
        Args:
            ssh: Established SSH connection
            
        Returns:
            PostgresManager instance if PostgreSQL is installed, None otherwise
        """
        pg_manager = PostgresManager(ssh)
        
        if pg_manager.is_installed():
            return pg_manager
        return None
    
    @staticmethod
    def execute_with_postgres(server: VpsServer, operation_name: str, 
                            operation_func, *args, **kwargs) -> Tuple[bool, str]:
        """Execute operation with PostgreSQL manager and handle common errors.
        
        Args:
            server: VpsServer instance
            operation_name: Name of operation for error messages
            operation_func: Function to execute with pg_manager as first argument
            *args, **kwargs: Additional arguments for operation_func
            
        Returns:
            Tuple of (success, message)
        """
        ssh = None
        try:
            ssh = BackupService.create_ssh_connection(server)
            if not ssh:
                return False, f'{operation_name} failed: Could not connect to server via SSH'
            
            pg_manager = DatabaseService.create_postgres_manager(ssh)
            if not pg_manager:
                return False, f'{operation_name} failed: PostgreSQL is not installed on the server'
            
            return operation_func(pg_manager, *args, **kwargs)
            
        except Exception as e:
            return False, f'{operation_name} failed: {str(e)}'
        finally:
            if ssh:
                ssh.disconnect()
    

    
    @staticmethod
    def create_database_operation(pg_manager: PostgresManager, name: str, 
                                username: str, password: str) -> Tuple[bool, str]:
        """Create database operation following WAL-G best practices with comprehensive error handling.
        
        Following WAL-G documentation recommendations, this method:
        1. Creates the database with postgres superuser as owner (default)
        2. Creates the application user with appropriate permissions
        3. Includes rollback mechanisms for failed operations
        4. This approach ensures maximum compatibility during both creation and restoration
        
        This matches the WAL-G approach where databases are owned by postgres superuser
        and application users are granted specific permissions as needed.
        """
        database_created = False
        
        try:
            # Step 1: Create database with postgres superuser as owner (WAL-G best practice)
            db_success, db_message = pg_manager.create_database(name)
            
            # If creation failed due to configuration issues, try to fix and retry
            if not db_success and ('invalid line' in db_message.lower() or 'listen_addresses' in db_message.lower()):
                fix_success, fix_message = pg_manager.fix_postgresql_config()
                if fix_success:
                    # Retry database creation after fixing config
                    db_success, db_message = pg_manager.create_database(name)
                    if db_success:
                        db_message = "Database created successfully with postgres superuser ownership (after fixing PostgreSQL configuration)"
                    else:
                        db_message = f"Configuration fixed but database creation still failed: {db_message}"
                else:
                    db_message = f"Database creation failed due to configuration error. Fix attempt failed: {fix_message}"
            
            if not db_success:
                return False, db_message
            
            database_created = True
            
            # Step 2: Create the application user using unified interface
            user_success, user_message = DatabaseService.create_unified_database_user(
                pg_manager=pg_manager,
                username=username,
                password=password,
                db_name=name,
                permission_level='all_permissions',  # Primary user gets full access
                is_primary=True
            )
            
            if not user_success:
                # Rollback: Delete the database since user creation failed
                try:
                    rollback_success, rollback_message = pg_manager.delete_database(name)
                    if rollback_success:
                        return False, f"Database creation rolled back due to user creation failure: {user_message}"
                    else:
                        return False, f"User creation failed and database rollback also failed. Manual cleanup required for database '{name}'. User error: {user_message}. Rollback error: {rollback_message}"
                except Exception as rollback_error:
                    return False, f"User creation failed and database rollback encountered an exception. Manual cleanup required for database '{name}'. User error: {user_message}. Rollback exception: {str(rollback_error)}"
            
            return True, f"Database '{name}' created successfully with postgres superuser ownership and application user '{username}' configured"
            
        except Exception as e:
            # If any unexpected error occurs, attempt to clean up the database if it was created
            if database_created:
                try:
                    pg_manager.delete_database(name)
                except Exception:
                    pass  # Ignore cleanup errors in exception handler
            
            return False, f"Database creation operation failed with exception: {str(e)}"
    
    @staticmethod
    def update_user_password_operation(pg_manager: PostgresManager, 
                                     username: str, password: str) -> Tuple[bool, str]:
        """Update user password operation."""
        return pg_manager.update_user_password(username, password)
    
    @staticmethod
    def create_user_operation(pg_manager: PostgresManager, username: str, 
                            password: str, db_name: str, permission_level: str) -> Tuple[bool, str]:
        """Create user operation."""
        return pg_manager.user_manager.create_database_user(
            username=username,
            password=password,
            db_name=db_name,
            permission_level=permission_level
        )
    
    @staticmethod
    def create_unified_database_user(pg_manager: PostgresManager, username: str, 
                                   password: str, db_name: str, permission_level: str = 'all_permissions',
                                   is_primary: bool = True, individual_permissions: Dict[str, bool] = None) -> Tuple[bool, str]:
        """Unified user creation interface for both new and restored databases.
        
        This method provides a consistent interface for creating database users that works
        with the postgres superuser ownership model. It handles:
        1. Creating application users with appropriate permissions
        2. Proper permission assignment without ownership conflicts
        3. Consistent behavior for both new database creation and restoration scenarios
        4. Support for individual permission assignment
        
        Following WAL-G best practices, this assumes databases are owned by postgres superuser
        and application users are granted specific permissions as needed.
        
        Args:
            pg_manager: PostgresManager instance
            username: Username for the new user
            password: Password for the new user
            db_name: Database name to grant permissions on
            permission_level: Permission level ('read_only', 'read_write', 'all_permissions', 'individual')
            is_primary: Whether this is the primary user for the database
            individual_permissions: Dict of individual permissions (for 'individual' mode)
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            if permission_level == 'individual' and individual_permissions:
                # Handle individual permissions
                # First create user with no access
                success, message = pg_manager.user_manager.create_database_user(
                    username=username,
                    password=password,
                    db_name=db_name,
                    permission_level='no_access'
                )
                
                if not success:
                    return False, f"Failed to create database user: {message}"
                
                # Then apply individual permissions
                success, message = pg_manager.user_manager.grant_individual_permissions(
                    username=username,
                    password=password,
                    db_name=db_name,
                    permissions=individual_permissions
                )
                
                if not success:
                    return False, f"User created but failed to assign individual permissions: {message}"
                
                user_type = "Primary" if is_primary else "Application"
                return True, f"{user_type} user '{username}' created successfully with individual permissions on database '{db_name}'"
            else:
                # Handle preset permission levels
                success, message = pg_manager.user_manager.create_database_user(
                    username=username,
                    password=password,
                    db_name=db_name,
                    permission_level=permission_level
                )
                
                if not success:
                    return False, f"Failed to create database user: {message}"
                
                # For primary users, ensure they have comprehensive access
                if is_primary and permission_level == 'all_permissions':
                    # Verify the user has the expected permissions
                    users = pg_manager.user_manager.list_database_users(db_name)
                    user_found = any(user['username'] == username for user in users)
                    
                    if not user_found:
                        return False, f"User '{username}' was created but not found in database user list"
                    
                    return True, f"Primary user '{username}' created successfully with {permission_level} on database '{db_name}'"
                else:
                    return True, f"User '{username}' created successfully with {permission_level} on database '{db_name}'"
                
        except Exception as e:
            return False, f"Error creating unified database user: {str(e)}"
    
    @staticmethod
    def delete_user_operation(pg_manager: PostgresManager, username: str) -> Tuple[bool, str]:
        """Delete user operation."""
        try:
            pg_manager.delete_database_user(username)
            return True, 'User deleted successfully'
        except Exception as e:
            return False, str(e)
    
    @staticmethod
    def grant_individual_permissions_operation(pg_manager: PostgresManager, username: str, 
                                             password: str, db_name: str, permissions: Dict[str, bool]) -> Tuple[bool, str]:
        """Grant individual permissions operation."""
        return pg_manager.grant_individual_permissions(
            username=username,
            db_name=db_name,
            permissions=permissions
        )
    
    @staticmethod
    def get_user_permissions(server: VpsServer, database_name: str) -> Dict[str, str]:
        """Get user permissions from PostgreSQL server.
        
        Args:
            server: VpsServer instance
            database_name: Database name
            
        Returns:
            Dictionary mapping username to permission level
        """
        user_permissions = {}
        ssh = None
        try:
            ssh = BackupService.create_ssh_connection(server)
            if ssh:
                pg_manager = DatabaseService.create_postgres_manager(ssh)
                if pg_manager:
                    server_users = pg_manager.list_database_users(database_name)
                    for server_user in server_users:
                        user_permissions[server_user['username']] = server_user['permission_level']
        except Exception as e:
            flash(f'Warning: Could not retrieve user permissions from server: {str(e)}', 'warning')
        finally:
            if ssh:
                ssh.disconnect()
        
        return user_permissions
    
    @staticmethod
    def get_current_user_permission(server: VpsServer, database_name: str, 
                                  username: str) -> str:
        """Get current permission level for a specific user.
        
        Args:
            server: VpsServer instance
            database_name: Database name
            username: Username to check
            
        Returns:
            Permission level string, defaults to 'read_write' if not found
        """
        user_permissions = DatabaseService.get_user_permissions(server, database_name)
        return user_permissions.get(username, 'read_write')
    
    @staticmethod
    def get_user_individual_permissions(server: VpsServer, database_name: str) -> Dict[str, Dict[str, bool]]:
        """Get individual permissions for all users from PostgreSQL server.
        
        Args:
            server: VpsServer instance
            database_name: Database name
            
        Returns:
            Dictionary mapping username to individual permission flags
        """
        user_individual_permissions = {}
        ssh = None
        try:
            ssh = BackupService.create_ssh_connection(server)
            if ssh:
                pg_manager = DatabaseService.create_postgres_manager(ssh)
                if pg_manager:
                    server_users = pg_manager.list_database_users(database_name)
                    for server_user in server_users:
                        username = server_user['username']
                        individual_perms = pg_manager.get_user_individual_permissions(username, database_name)
                        
                        # Use the actual individual permissions detected by user_manager
                        # No need to override with legacy mapping - trust the real database state
                        
                        user_individual_permissions[username] = individual_perms
        except Exception:
            # Return empty dict on error
            pass
        finally:
            if ssh:
                ssh.disconnect()
        
        return user_individual_permissions
    
    @staticmethod
    def refresh_user_permissions_operation(pg_manager: PostgresManager, username: str, 
                                         db_name: str, permission_level: str = 'read_write') -> Tuple[bool, str]:
        """Refresh user permissions operation.
        
        This operation re-grants permissions to a user, which is useful after
        database recovery when tables are restored after user creation.
        
        Args:
            pg_manager: PostgresManager instance
            username: Username to refresh permissions for
            db_name: Database name
            permission_level: Permission level ('read_only', 'read_write', 'all_permissions')
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        return pg_manager.user_manager.refresh_table_permissions(
            username=username,
            db_name=db_name,
            permission_level=permission_level
        )
    
    @staticmethod
    def apply_permission_combination(server: VpsServer, database_name: str, username: str, combination: str) -> Dict[str, Any]:
        """Apply a predefined permission combination to a user.
        
        Args:
            server: VpsServer instance
            database_name: Database name
            username: Username to update
            combination: Permission combination key
            
        Returns:
            Dictionary with success status and message
        """
        try:
            # Get permissions for the combination
            permissions = PermissionManager.get_permissions_for_combination(combination)
            
            # Apply the permissions using the existing grant_individual_permissions_operation
            success, message = DatabaseService.execute_with_postgres(
                server,
                "Apply permission combination",
                DatabaseService.grant_individual_permissions_operation,
                username=username,
                password="",  # Password not needed for permission updates
                db_name=database_name,
                permissions=permissions
            )
            
            if success:
                combination_label = PermissionManager.get_combination_label(combination)
                return {
                    'success': True,
                    'message': f'Successfully applied "{combination_label}" permissions to user {username}',
                    'combination': combination,
                    'permissions': permissions
                }
            else:
                return {
                    'success': False,
                    'message': f'Failed to apply permissions: {message}'
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'Error applying permission combination: {str(e)}'
            }
    
    @staticmethod
    def check_postgres_status(server: VpsServer) -> Dict[str, Any]:
        """Check PostgreSQL status on server.
        
        Args:
            server: VpsServer instance
            
        Returns:
            Dictionary with success status and version or error message
        """
        ssh = None
        try:
            ssh = BackupService.create_ssh_connection(server)
            if not ssh:
                return {
                    'success': False,
                    'message': 'Failed to connect to server via SSH',
                    'can_install': False
                }
            
            pg_manager = DatabaseService.create_postgres_manager(ssh)
            if not pg_manager:
                return {
                    'success': False,
                    'message': 'PostgreSQL is not installed on the server',
                    'can_install': True
                }
            
            pg_version = pg_manager.get_postgres_version()
            walg_installed = pg_manager.backup_manager.is_walg_installed()
            
            return {
                'success': True,
                'postgres_version': pg_version,
                'walg_installed': walg_installed
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'can_install': False
            }
        finally:
            if ssh:
                ssh.disconnect()


class DatabaseImportService:
    """Service class for database import operations."""
    
    @staticmethod
    def update_log(restore_log: RestoreLog, message: str) -> None:
        """Update restore log with new message.
        
        Args:
            restore_log: RestoreLog instance
            message: Message to append
        """
        restore_log.log_output = restore_log.log_output + '\n' + message
        db.session.commit()
    
    @staticmethod
    def parse_connection_string(connection_string: str) -> Dict[str, str]:
        """Parse PostgreSQL connection string with SSL support.
        
        Args:
            connection_string: PostgreSQL connection URL
            
        Returns:
            Dictionary with connection parameters
        """
        # Parse basic URL structure - handle both postgresql:// and postgres://
        if connection_string.startswith('postgresql://'):
            url_part = connection_string.replace('postgresql://', '')
        elif connection_string.startswith('postgres://'):
            url_part = connection_string.replace('postgres://', '')
        else:
            raise ValueError('Connection string must start with postgresql:// or postgres://')
        
        # Split URL and query parameters
        if '?' in url_part:
            url_part, query_part = url_part.split('?', 1)
        else:
            query_part = ''
        
        # Parse main URL components
        parts = url_part.split('@')
        user_pass = parts[0].split(':')
        host_port_db = parts[1].split('/')
        host_port = host_port_db[0].split(':')
        
        result = {
            'username': user_pass[0],
            'password': user_pass[1],
            'host': host_port[0],
            'port': host_port[1],
            'database': host_port_db[1]
        }
        
        # Parse query parameters for SSL settings
        if query_part:
            for param in query_part.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    result[key] = value
        
        return result
    
    @staticmethod
    def generate_connection_string(host: str, port: int, username: str, password: str, 
                                 database: str, ssl_enabled: bool = False) -> str:
        """Generate PostgreSQL connection string with optional SSL.
        
        Args:
            host: Database host
            port: Database port
            username: Database username
            password: Database password
            database: Database name
            ssl_enabled: Whether to require SSL
            
        Returns:
            PostgreSQL connection string
        """
        base_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        if ssl_enabled:
            base_url += "?sslmode=require"
        
        return base_url
    
    @staticmethod
    def perform_database_import(pg_manager: PostgresManager, source_conn: str, 
                              target_db_name: str, restore_log: RestoreLog) -> Tuple[bool, str]:
        """Perform the actual database import operation.
        
        Args:
            pg_manager: PostgresManager instance
            source_conn: Source connection string
            target_db_name: Target database name
            restore_log: RestoreLog instance for progress tracking
            
        Returns:
            Tuple of (success, message)
        """
        temp_file = f"/tmp/db_import_{int(time.time())}.dump"
        temp_db_name = f"{target_db_name}_import_{int(time.time())}"
        
        try:
            # Check required tools
            DatabaseImportService.update_log(restore_log, 'Checking required database tools')
            tools_check = pg_manager.ssh.execute_command("which pg_dump pg_restore")
            if tools_check['exit_code'] != 0:
                DatabaseImportService.update_log(restore_log, 'Required tools pg_dump and pg_restore not found on server')
                return False, 'Required database tools not found on server'
            
            # Parse source connection
            conn_params = DatabaseImportService.parse_connection_string(source_conn)
            
            # Step 1: Export source database
            DatabaseImportService.update_log(restore_log, 'Starting export from source database')
            dump_cmd = (
                f"PGPASSWORD='{conn_params['password']}' pg_dump -Fc --no-acl --no-owner "
                f"-h {conn_params['host']} -p {conn_params['port']} "
                f"-U {conn_params['username']} -d {conn_params['database']} -f {temp_file}"
            )
            
            dump_result = pg_manager.ssh.execute_command(dump_cmd)
            if dump_result['exit_code'] != 0:
                DatabaseImportService.update_log(restore_log, f'Failed to export source database: {dump_result["stderr"]}')
                return False, 'Failed to export source database'
            
            DatabaseImportService.update_log(restore_log, 'Source database exported successfully')
            
            # Step 2: Create temporary database
            DatabaseImportService.update_log(restore_log, f'Creating temporary database: {temp_db_name}')
            create_temp_db = pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'CREATE DATABASE {temp_db_name};'")
            if create_temp_db['exit_code'] != 0:
                DatabaseImportService.update_log(restore_log, f'Failed to create temporary database: {create_temp_db["stderr"]}')
                return False, 'Failed to create temporary database'
            
            # Step 3: Import to temporary database
            DatabaseImportService.update_log(restore_log, 'Starting import to temporary database')
            restore_cmd = f"sudo -u postgres pg_restore --no-acl --no-owner -d {temp_db_name} {temp_file}"
            restore_result = pg_manager.ssh.execute_command(restore_cmd)
            
            if restore_result['exit_code'] != 0:
                DatabaseImportService.update_log(restore_log, f'Warning: Some errors occurred during import: {restore_result["stderr"]}')
            
            DatabaseImportService.update_log(restore_log, 'Import to temporary database completed')
            
            # Step 4: Replace target database
            DatabaseImportService.update_log(restore_log, f'Replacing target database: {target_db_name}')
            
            # Drop target database
            drop_cmd = pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'DROP DATABASE {target_db_name};'")
            if drop_cmd['exit_code'] != 0:
                DatabaseImportService.update_log(restore_log, f'Failed to drop target database: {drop_cmd["stderr"]}')
                # Clean up
                pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'DROP DATABASE {temp_db_name};'")
                return False, 'Failed to drop target database'
            
            # Rename temporary database
            rename_cmd = pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'ALTER DATABASE {temp_db_name} RENAME TO {target_db_name};'")
            if rename_cmd['exit_code'] != 0:
                DatabaseImportService.update_log(restore_log, f'Failed to rename database: {rename_cmd["stderr"]}')
                # Try to recreate original database
                pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'CREATE DATABASE {target_db_name};'")
                return False, 'Failed to rename database'
            
            DatabaseImportService.update_log(restore_log, 'Database import completed successfully')
            return True, 'Database import completed successfully'
            
        finally:
            # Clean up temporary files
            DatabaseImportService.update_log(restore_log, 'Cleaning up temporary files')
            pg_manager.ssh.execute_command(f"rm -f {temp_file}")