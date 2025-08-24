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
        """Create database operation."""
        # First attempt to create database
        success, message = pg_manager.create_database(name)
        
        # If creation failed due to configuration issues, try to fix and retry
        if not success and ('invalid line' in message.lower() or 'listen_addresses' in message.lower()):
            fix_success, fix_message = pg_manager.fix_postgresql_config()
            if fix_success:
                # Retry database creation after fixing config
                success, message = pg_manager.create_database(name)
                if success:
                    message = "Database created successfully (after fixing PostgreSQL configuration)"
                else:
                    message = f"Configuration fixed but database creation still failed: {message}"
            else:
                message = f"Database creation failed due to configuration error. Fix attempt failed: {fix_message}"
        
        return success, message
    
    @staticmethod
    def update_user_password_operation(pg_manager: PostgresManager, 
                                     username: str, password: str) -> Tuple[bool, str]:
        """Update user password operation."""
        return pg_manager.update_user_password(username, password)
    
    @staticmethod
    def create_user_operation(pg_manager: PostgresManager, username: str, 
                            password: str, db_name: str, permission_level: str) -> Tuple[bool, str]:
        """Create user operation."""
        return pg_manager.create_database_user(
            username=username,
            password=password,
            db_name=db_name,
            permission_level=permission_level
        )
    
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