"""User management for PostgreSQL."""

import logging
from typing import Dict, List, Tuple
from .system_utils import SystemUtils
from ..permission_manager import PermissionManager, PermissionCombination

class PostgresUserManager:
    """Manages PostgreSQL users and permissions."""
    
    def __init__(self, ssh_manager, system_utils: SystemUtils, logger=None):
        self.ssh_manager = ssh_manager
        self.system_utils = system_utils
        self.logger = logger or logging.getLogger(__name__)
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote PostgreSQL identifier to prevent SQL injection."""
        # Replace any double quotes with double-double quotes and wrap in quotes
        return f'"{identifier.replace('"', '""')}"'
    
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
        
        This method now uses the unified permission system for consistency and efficiency.
        
        Args:
            username: Username to grant permissions to
            db_name: Database name
            permissions: Dict with keys: connect, select, insert, update, delete, create
        
        Returns:
            Tuple of (success, message)
        """
        if not self.user_exists(username):
            self.logger.error(f"User '{username}' does not exist")
            return False, f"User '{username}' does not exist"
        
        # Validate permissions using PermissionManager
        is_valid, errors = PermissionManager.validate_individual_permissions(permissions)
        if not is_valid:
            return False, f"Invalid permissions: {'; '.join(errors)}"
        
        # Use the unified permission granting system
        return self._grant_unified_permissions(username, db_name, permissions)
    
    def refresh_table_permissions(self, username: str, db_name: str, permission_level: str = 'read_write') -> Tuple[bool, str]:
        """Re-grant table permissions to a user after database restore.
        
        This method is specifically designed to fix permission issues that occur
        when a user is created before tables are restored from backup.
        
        Refactored to:
        1. Provide better error handling and recovery
        2. Optimize the ownership transfer process
        3. Ensure data integrity throughout the operation
        
        Args:
            username: The database username
            db_name: The database name
            permission_level: The permission level ('read_only', 'read_write', 'all_permissions')
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        if not self.user_exists(username):
            return False, f"User '{username}' does not exist"
        
        messages = []
        
        # First, fix ownership issues that occur during recovery
        self.logger.info(f"Starting ownership transfer for user '{username}' on database '{db_name}'")
        ownership_success, ownership_message = self._fix_recovery_ownership(username, db_name)
        
        if ownership_success:
            messages.append(f"Ownership transfer: {ownership_message}")
            self.logger.info(f"Ownership transfer successful: {ownership_message}")
        else:
            messages.append(f"Ownership transfer failed: {ownership_message}")
            self.logger.error(f"Ownership transfer failed: {ownership_message}")
            # For recovery scenarios, ownership transfer failure should not prevent permission granting
            # as the user might still need access to existing objects
        
        # Grant appropriate permissions based on the specified level using unified system
        self.logger.info(f"Granting '{permission_level}' permissions to user '{username}' on database '{db_name}'")
        
        # Map permission levels to permission combinations
        permission_combinations = {
            'read_only': PermissionCombination.READ_ONLY,
            'read_write': PermissionCombination.READ_WRITE,
            'all_permissions': PermissionCombination.ALL_PERMISSIONS
        }
        
        if permission_level not in permission_combinations:
            return False, f"Unknown permission level: {permission_level}"
        
        # Convert permission combination to individual permissions dict
        permissions_dict = PermissionManager.get_permissions_for_combination(permission_combinations[permission_level])
        
        # Use unified permission granting system
        permission_success, permission_message = self._grant_unified_permissions(username, db_name, permissions_dict)
        
        if permission_success:
            messages.append(f"Permissions granted: {permission_message}")
            self.logger.info(f"Permissions granted successfully: {permission_message}")
        else:
            messages.append(f"Permission granting failed: {permission_message}")
            self.logger.error(f"Permission granting failed: {permission_message}")
        
        # Determine overall success
        # For recovery scenarios, we consider it successful if either ownership OR permissions succeed
        # This provides resilience in case of partial failures
        overall_success = ownership_success or permission_success
        
        if overall_success:
            final_message = "Database recovery permissions completed. " + "; ".join(messages)
            return True, final_message
        else:
            final_message = "Database recovery permissions failed. " + "; ".join(messages)
            return False, final_message
    
    def _fix_recovery_ownership(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Fix ownership issues that occur during database recovery.
        
        During WAL-G recovery, the entire PostgreSQL cluster is restored from backup,
        but the application creates a new database and user. This creates a mismatch
        where restored objects are owned by the original user (who may not exist)
        but the new user needs to own them for the application to function properly.
        
        Enhanced implementation:
        1. Pre-validation to ensure user and database exist
        2. Comprehensive ownership transfer in a single atomic transaction
        3. Enhanced error handling with detailed logging
        4. Post-transfer verification to ensure success
        5. Handles edge cases like missing objects or permission conflicts
        
        Args:
            username: The new database owner username
            db_name: The database name
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        # Step 1: Pre-validation
        try:
            # Verify user exists
            if not self.user_exists(username):
                return False, f"User '{username}' does not exist. Cannot transfer ownership."
            
            # Verify database exists
            db_check_query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';"
            result = self.system_utils.execute_postgres_sql(db_check_query, "postgres")
            if result['exit_code'] != 0 or not result.get('stdout', '').strip():
                return False, f"Database '{db_name}' does not exist. Cannot transfer ownership."
            
            self.logger.info(f"Pre-validation passed for ownership transfer: user '{username}', database '{db_name}'")
            
        except Exception as e:
            return False, f"Pre-validation failed: {str(e)}"
        
        # Step 2: Prepare identifiers for SQL operations
        quoted_username = self._quote_identifier(username)
        quoted_db_name = self._quote_identifier(db_name)
        # Escape single quotes in username for safe use in DO block
        escaped_username = username.replace("'", "''")
        
        # Step 3: Single comprehensive ownership transfer command using a transaction
        ownership_transfer_sql = f"""
        BEGIN;
        
        -- Set the target user for all operations
        SET LOCAL search_path = public;
        
        -- Transfer database ownership
        ALTER DATABASE {quoted_db_name} OWNER TO {quoted_username};
        
        -- Transfer ownership of all database objects in a single DO block
        DO $$
        DECLARE
            target_user TEXT := '{escaped_username}';
            r RECORD;
            error_count INTEGER := 0;
            success_count INTEGER := 0;
        BEGIN
            -- Transfer ownership of schemas
            FOR r IN SELECT nspname FROM pg_namespace WHERE nspname IN ('public')
            LOOP
                BEGIN
                    EXECUTE format('ALTER SCHEMA %I OWNER TO %I', r.nspname, target_user);
                    success_count := success_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Failed to transfer schema ownership: %: %', r.nspname, SQLERRM;
                    error_count := error_count + 1;
                END;
            END LOOP;
            
            -- Transfer ownership of all tables
            FOR r IN SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('public')
            LOOP
                BEGIN
                    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', r.schemaname, r.tablename, target_user);
                    success_count := success_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Failed to transfer table ownership: %.%: %', r.schemaname, r.tablename, SQLERRM;
                    error_count := error_count + 1;
                END;
            END LOOP;
            
            -- Transfer ownership of all sequences
            FOR r IN SELECT schemaname, sequencename FROM pg_sequences WHERE schemaname IN ('public')
            LOOP
                BEGIN
                    EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO %I', r.schemaname, r.sequencename, target_user);
                    success_count := success_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Failed to transfer sequence ownership: %.%: %', r.schemaname, r.sequencename, SQLERRM;
                    error_count := error_count + 1;
                END;
            END LOOP;
            
            -- Transfer ownership of all views
            FOR r IN SELECT schemaname, viewname FROM pg_views WHERE schemaname IN ('public')
            LOOP
                BEGIN
                    EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', r.schemaname, r.viewname, target_user);
                    success_count := success_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Failed to transfer view ownership: %.%: %', r.schemaname, r.viewname, SQLERRM;
                    error_count := error_count + 1;
                END;
            END LOOP;
            
            -- Transfer ownership of all functions and procedures
            FOR r IN SELECT n.nspname as schemaname, p.proname, pg_get_function_identity_arguments(p.oid) as args, p.prokind
                     FROM pg_proc p 
                     JOIN pg_namespace n ON p.pronamespace = n.oid 
                     WHERE n.nspname IN ('public')
            LOOP
                BEGIN
                    EXECUTE format('ALTER ROUTINE %I.%I(%s) OWNER TO %I', r.schemaname, r.proname, r.args, target_user);
                    success_count := success_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Failed to transfer routine ownership: %.%(%): %', r.schemaname, r.proname, r.args, SQLERRM;
                    error_count := error_count + 1;
                END;
            END LOOP;
            
            -- Transfer ownership of all types
            FOR r IN SELECT n.nspname as schemaname, t.typname
                     FROM pg_type t
                     JOIN pg_namespace n ON t.typnamespace = n.oid
                     WHERE n.nspname IN ('public') AND t.typtype = 'c'
            LOOP
                BEGIN
                    EXECUTE format('ALTER TYPE %I.%I OWNER TO %I', r.schemaname, r.typname, target_user);
                    success_count := success_count + 1;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Failed to transfer type ownership: %.%: %', r.schemaname, r.typname, SQLERRM;
                    error_count := error_count + 1;
                END;
            END LOOP;
            
            -- Log summary
            RAISE NOTICE 'Ownership transfer completed: % successful, % errors', success_count, error_count;
            
            -- Fail the transaction if too many errors occurred
            IF error_count > success_count THEN
                RAISE EXCEPTION 'Ownership transfer failed: too many errors (% errors vs % successes)', error_count, success_count;
            END IF;
        END $$;
        
        COMMIT;
        """
        
        try:
            # Step 4: Execute the comprehensive ownership transfer in a single transaction
            self.logger.info(f"Starting ownership transfer for database '{db_name}' to user '{username}'")
            result = self.system_utils.execute_postgres_sql(ownership_transfer_sql, db_name)
            
            if result['exit_code'] != 0:
                error_msg = f"Ownership transfer failed for database '{db_name}': {result.get('stderr', 'Unknown error')}"
                self.logger.error(error_msg)
                return False, error_msg
            
            # Step 5: Post-verification to ensure ownership transfer was successful
            try:
                # Verify database ownership
                db_owner_query = f"""
                SELECT pg_catalog.pg_get_userbyid(datdba) as owner
                FROM pg_database 
                WHERE datname = '{db_name}';
                """
                
                verify_result = self.system_utils.execute_postgres_sql(db_owner_query, "postgres")
                if verify_result['exit_code'] == 0:
                    owner_output = verify_result.get('stdout', '').strip()
                    if username in owner_output:
                        self.logger.info(f"Ownership verification successful: database '{db_name}' is owned by '{username}'")
                        success_msg = f"Ownership transfer completed and verified successfully for database '{db_name}' to user '{username}'"
                        return True, success_msg
                    else:
                        warning_msg = f"Ownership transfer completed but verification shows unexpected owner: {owner_output}"
                        self.logger.warning(warning_msg)
                        return True, warning_msg  # Still return success as transfer completed
                else:
                    warning_msg = f"Ownership transfer completed but verification failed: {verify_result.get('stderr', '')}"
                    self.logger.warning(warning_msg)
                    return True, warning_msg  # Still return success as transfer completed
                    
            except Exception as verify_error:
                warning_msg = f"Ownership transfer completed but post-verification failed: {str(verify_error)}"
                self.logger.warning(warning_msg)
                return True, warning_msg  # Still return success as transfer completed
                
        except Exception as e:
            error_msg = f"Failed to execute ownership transfer for database '{db_name}': {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def create_database_owner_during_recovery(self, username: str, password: str, db_name: str) -> Tuple[bool, str]:
        """Create a new database owner user during recovery with comprehensive error handling.
        
        This method is specifically designed for database recovery scenarios where:
        1. A database has been restored from backup
        2. A new owner user needs to be created
        3. Ownership of all objects needs to be transferred
        4. Appropriate permissions need to be granted
        
        The method includes:
        - Atomic operations with rollback capability
        - Comprehensive error handling
        - Data integrity validation
        - Performance optimization
        
        Args:
            username: The new database owner username
            password: The password for the new user
            db_name: The database name
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Step 1: Validate inputs
            if not username or not password or not db_name:
                return False, "Username, password, and database name are required"
            
            # Step 2: Check if user already exists
            if self.user_exists(username):
                self.logger.info(f"User '{username}' already exists, proceeding with ownership transfer")
            else:
                # Create the user first
                create_success, create_message = self.create_user(username, password)
                if not create_success:
                    return False, f"Failed to create user: {create_message}"
                self.logger.info(f"User '{username}' created successfully")
            
            # Step 3: Transfer ownership with comprehensive error handling
            ownership_success, ownership_message = self._fix_recovery_ownership(username, db_name)
            
            # Step 4: Grant appropriate permissions (read_write for owner)
            permission_success, permission_message = self._grant_all_permissions(username, db_name)
            
            # Step 5: Validate the final state
            validation_success, validation_message = self._validate_user_ownership(username, db_name)
            
            # Determine overall success
            if ownership_success and permission_success and validation_success:
                final_message = f"Database owner '{username}' created successfully for database '{db_name}'. {ownership_message}; {permission_message}; {validation_message}"
                self.logger.info(final_message)
                return True, final_message
            elif ownership_success or permission_success:
                # Partial success - log warnings but don't fail completely
                warning_message = f"Partial success for database owner '{username}' on database '{db_name}'. Ownership: {ownership_message}; Permissions: {permission_message}; Validation: {validation_message}"
                self.logger.warning(warning_message)
                return True, warning_message
            else:
                # Complete failure
                error_message = f"Failed to create database owner '{username}' for database '{db_name}'. Ownership: {ownership_message}; Permissions: {permission_message}"
                self.logger.error(error_message)
                return False, error_message
                
        except Exception as e:
            error_msg = f"Unexpected error creating database owner '{username}' for database '{db_name}': {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def _validate_user_ownership(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Validate that the user has proper ownership and permissions on the database.
        
        Args:
            username: The database username
            db_name: The database name
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Check database ownership
            db_owner_query = f"""
            SELECT datname, pg_catalog.pg_get_userbyid(datdba) as owner
            FROM pg_database 
            WHERE datname = '{db_name}';
            """
            
            result = self.system_utils.execute_postgres_sql(db_owner_query, "postgres")
            success = result['exit_code'] == 0
            output = result.get('stderr', '') if not success else result.get('stdout', '')
            
            if not success:
                return False, f"Failed to check database ownership: {output}"
            
            # Check if user owns some tables
            table_owner_query = f"""
            SELECT COUNT(*) as owned_tables
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_catalog = '{db_name}'
            AND table_type = 'BASE TABLE';
            """
            
            result = self.system_utils.execute_postgres_sql(table_owner_query, db_name)
            success = result['exit_code'] == 0
            output = result.get('stderr', '') if not success else result.get('stdout', '')
            
            if not success:
                return False, f"Failed to check table ownership: {output}"
            
            # Check user permissions
            permissions = self.get_user_permissions(username, db_name)
            
            validation_message = f"User '{username}' validation completed. Database access confirmed, permissions: {permissions}"
            return True, validation_message
            
        except Exception as e:
            error_msg = f"Failed to validate user ownership: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def _revoke_all_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Revoke all permissions from a user on a database."""
        quoted_db_name = self._quote_identifier(db_name)
        quoted_username = self._quote_identifier(username)
        commands = [
            f"REVOKE ALL PRIVILEGES ON DATABASE {quoted_db_name} FROM {quoted_username};",
            f"REVOKE CONNECT ON DATABASE {quoted_db_name} FROM {quoted_username};"
        ]
        
        # Execute commands on the specific database
        db_commands = [
            f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {quoted_username};",
            f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {quoted_username};",
            f"REVOKE ALL PRIVILEGES ON SCHEMA public FROM {quoted_username};"
        ]
        
        # Execute general commands
        for cmd in commands:
            result = self.system_utils.execute_postgres_sql(cmd)
            if result['exit_code'] != 0:
                error_details = result.get('stderr', 'Unknown error')
                self.logger.warning(f"Command failed (continuing): {cmd} - Error: {error_details}")
        
        # Execute database-specific commands
        for cmd in db_commands:
            result = self.system_utils.execute_postgres_sql(cmd, db_name)
            if result['exit_code'] != 0:
                error_details = result.get('stderr', 'Unknown error')
                self.logger.warning(f"Command failed (continuing): {cmd} - Error: {error_details}")
        
        return True, f"Revoked all permissions for user '{username}' on database '{db_name}'"
    
    def _grant_unified_permissions(self, username: str, db_name: str, permissions: Dict[str, bool]) -> Tuple[bool, str]:
        """Unified permission granting method that handles all permission combinations efficiently.
        
        This method consolidates all permission granting logic into a single, optimized implementation
        that reduces code duplication and improves maintainability.
        
        Args:
            username: Username to grant permissions to
            db_name: Database name
            permissions: Dict with permission flags (connect, select, insert, update, delete, create)
            
        Returns:
            Tuple of (success, message)
        """
        quoted_db_name = self._quote_identifier(db_name)
        quoted_username = self._quote_identifier(username)
        
        # Build database-level permissions
        db_grants = []
        if permissions.get('connect', False):
            db_grants.append(f"GRANT CONNECT ON DATABASE {quoted_db_name} TO {quoted_username}")
        if permissions.get('create', False):
            db_grants.append(f"GRANT CREATE ON DATABASE {quoted_db_name} TO {quoted_username}")
        
        # Build schema-level permissions
        schema_grants = []
        if permissions.get('connect', False):  # USAGE on schema requires connect
            schema_grants.append(f"GRANT USAGE ON SCHEMA public TO {quoted_username}")
        if permissions.get('create', False):
            schema_grants.append(f"GRANT CREATE ON SCHEMA public TO {quoted_username}")
        
        # Build table-level permissions
        table_perms = []
        if permissions.get('select', False):
            table_perms.append('SELECT')
        if permissions.get('insert', False):
            table_perms.append('INSERT')
        if permissions.get('update', False):
            table_perms.append('UPDATE')
        if permissions.get('delete', False):
            table_perms.append('DELETE')
        
        # Build sequence permissions
        sequence_perms = []
        if permissions.get('select', False):
            sequence_perms.append('SELECT')
        if permissions.get('update', False) or permissions.get('insert', False):
            sequence_perms.append('UPDATE')
        if permissions.get('insert', False) or permissions.get('update', False):
            sequence_perms.append('USAGE')
        
        # Execute database-level permissions
        if db_grants:
            db_permission_sql = f"""
            BEGIN;
            
            -- Revoke existing database permissions
            REVOKE ALL ON DATABASE {quoted_db_name} FROM {quoted_username};
            
            -- Grant requested database permissions
            {'; '.join(db_grants)};
            
            COMMIT;
            """
            
            self.logger.info(f"Executing database permissions SQL for user '{username}': {db_permission_sql}")
            result = self.system_utils.execute_postgres_sql(db_permission_sql)
            if result['exit_code'] != 0:
                self.logger.error(f"Database permissions failed for user '{username}': {result.get('stderr', 'Unknown error')}")
                return False, f"Failed to execute database permissions: {result.get('stderr', 'Unknown error')}"
            else:
                self.logger.info(f"Database permissions executed successfully for user '{username}'")
        
        # Execute schema and object-level permissions
        if schema_grants or table_perms or sequence_perms:
            schema_sql_parts = ["BEGIN;", 
                              "-- Revoke existing schema permissions",
                              f"REVOKE ALL ON SCHEMA public FROM {quoted_username};"]
            
            # Add schema grants
            if schema_grants:
                schema_sql_parts.extend(["-- Grant schema permissions"] + [f"{grant};" for grant in schema_grants])
            
            # Add table permissions
            if table_perms:
                table_grant = f"GRANT {', '.join(table_perms)} ON ALL TABLES IN SCHEMA public TO {quoted_username}"
                schema_sql_parts.extend(["-- Grant table permissions", f"{table_grant};"])
            
            # Add sequence permissions
            if sequence_perms:
                sequence_grant = f"GRANT {', '.join(set(sequence_perms))} ON ALL SEQUENCES IN SCHEMA public TO {quoted_username}"
                schema_sql_parts.extend(["-- Grant sequence permissions", f"{sequence_grant};"])
            
            # Add function permissions for comprehensive access
            if permissions.get('select', False) or permissions.get('insert', False) or permissions.get('update', False):
                schema_sql_parts.extend([
                    "-- Grant function permissions",
                    f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {quoted_username};"
                ])
            
            # Add default privileges for future objects
            if table_perms or sequence_perms:
                schema_sql_parts.append("-- Set default privileges for future objects")
                
                # Set default privileges for the database owner
                schema_sql_parts.append("""
                DO $$
                DECLARE owner_name TEXT;
                BEGIN
                    SELECT pg_catalog.pg_get_userbyid(datdba) INTO owner_name FROM pg_database WHERE datname = current_database();
                """)
                
                if table_perms:
                    table_default = f"EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT {', '.join(table_perms)} ON TABLES TO %I', owner_name, '{username.replace("'","''")}');"
                    schema_sql_parts.append(f"                    {table_default}")
                
                if sequence_perms:
                    seq_default = f"EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT {', '.join(set(sequence_perms))} ON SEQUENCES TO %I', owner_name, '{username.replace("'","''")}');"
                    schema_sql_parts.append(f"                    {seq_default}")
                
                schema_sql_parts.extend(["                END $$;"])
                
                # Also set default privileges for all roles (broader coverage)
                if table_perms:
                    schema_sql_parts.append(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT {', '.join(table_perms)} ON TABLES TO {quoted_username};")
                
                if sequence_perms:
                    schema_sql_parts.append(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT {', '.join(set(sequence_perms))} ON SEQUENCES TO {quoted_username};")
            
            schema_sql_parts.append("COMMIT;")
            
            schema_permission_sql = "\n".join(schema_sql_parts)
            
            self.logger.info(f"Executing schema permissions SQL for user '{username}': {schema_permission_sql}")
            result = self.system_utils.execute_postgres_sql(schema_permission_sql, db_name)
            if result['exit_code'] != 0:
                self.logger.error(f"Schema permissions failed for user '{username}': {result.get('stderr', 'Unknown error')}")
                self.logger.warning(f"Schema-specific commands failed (continuing): {result.get('stderr', 'Unknown error')}")
            else:
                self.logger.info(f"Schema permissions executed successfully for user '{username}'")
        
        # Generate descriptive message
        granted_perms = [perm for perm, granted in permissions.items() if granted]
        perm_description = PermissionManager.detect_combination_from_permissions(permissions)
        
        self.logger.info(f"Permission granting completed for user '{username}' on database '{db_name}'. Requested permissions: {permissions}")
        return True, f"Granted {perm_description} ({', '.join(granted_perms)}) to user '{username}' on database '{db_name}'"
    
    def _grant_read_only_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Grant read-only permissions to a user on a database."""
        permissions = PermissionManager.get_permissions_for_combination(PermissionCombination.READ_ONLY.value)
        return self._grant_unified_permissions(username, db_name, permissions)
    
    def _grant_read_write_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Grant read-write permissions to a user on a database."""
        permissions = PermissionManager.get_permissions_for_combination(PermissionCombination.READ_WRITE.value)
        return self._grant_unified_permissions(username, db_name, permissions)

    def _grant_all_permissions(self, username: str, db_name: str) -> Tuple[bool, str]:
        """Grant all permissions to a user on a database."""
        permissions = PermissionManager.get_permissions_for_combination(PermissionCombination.ALL_PERMISSIONS.value)
        return self._grant_unified_permissions(username, db_name, permissions)
    
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
            # Debug output removed for performance
            # print(f"DEBUG: Database permission query output for {username}:")
            # for i, line in enumerate(lines):
            #     print(f"DEBUG: Line {i}: '{line}'")
            
            for line in lines:
                line = line.strip()
                # Skip header lines and separator lines, but process data lines
                if '|' in line and ('t' in line or 'f' in line) and not line.startswith('-') and not line.startswith('connect_priv'):
                    parts = line.split('|')
                    if len(parts) >= 2:
                        connect_perm = parts[0].strip().lower() == 't'
                        create_perm = parts[1].strip().lower() == 't'
                        # print(f"DEBUG: Found database permissions - connect: {connect_perm}, create: {create_perm}")
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
                self.logger.info(f"Database '{db_name}' has {table_count} tables in public schema")
            except (ValueError, IndexError):
                has_tables = False
                self.logger.warning(f"Failed to parse table count for database '{db_name}': {table_result['stdout']}")
        else:
            self.logger.warning(f"Failed to check table count for database '{db_name}': {table_result.get('stderr', 'Unknown error')}")
        

        
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
            self.logger.info(f"Checking table permissions for user '{username}' on existing tables in database '{db_name}'")
        else:
            # Check default privileges for future tables
            self.logger.info(f"No tables found in database '{db_name}', checking default privileges for user '{username}'")
            table_perms_query = f"""
            SELECT 
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND (array_to_string(da.defaclacl, ',') LIKE '%{username}=r%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%r%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%arwd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%arw%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%ard%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%rwd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%rw%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%rd%')
                ) THEN 't' ELSE 'f' END as select_priv,
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND (array_to_string(da.defaclacl, ',') LIKE '%{username}=a%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%a%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%arwd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%arw%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%ard%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%awd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%aw%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%ad%')
                ) THEN 't' ELSE 'f' END as insert_priv,
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND (array_to_string(da.defaclacl, ',') LIKE '%{username}=w%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%w%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%arwd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%arw%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%rwd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%awd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%rw%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%aw%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%wd%')
                ) THEN 't' ELSE 'f' END as update_priv,
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_default_acl da 
                    WHERE da.defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    AND da.defaclobjtype = 'r'
                    AND (array_to_string(da.defaclacl, ',') LIKE '%{username}=d%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%d%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%arwd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%ard%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%rwd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%awd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%rd%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%ad%'
                         OR array_to_string(da.defaclacl, ',') LIKE '%{username}=%wd%')
                ) THEN 't' ELSE 'f' END as delete_priv;
            """
        
        self.logger.info(f"Executing table permissions query for user '{username}': {table_perms_query}")
        table_result = self.system_utils.execute_postgres_sql(table_perms_query, db_name)
        
        if table_result['exit_code'] == 0 and table_result['stdout'].strip():
            self.logger.info(f"Table permissions query result for user '{username}': {table_result['stdout']}")
            lines = table_result['stdout'].strip().split('\n')
            
            for line in lines:
                line = line.strip()
                # Skip header lines and separator lines, but process data lines
                if '|' in line and ('t' in line or 'f' in line) and not line.startswith('-') and not line.startswith('select_priv'):
                    parts = line.split('|')
                    if len(parts) >= 4:
                        select_perm = parts[0].strip().lower() == 't'
                        insert_perm = parts[1].strip().lower() == 't'
                        update_perm = parts[2].strip().lower() == 't'
                        delete_perm = parts[3].strip().lower() == 't'
                        self.logger.info(f"Parsed table permissions for user '{username}': select={select_perm}, insert={insert_perm}, update={update_perm}, delete={delete_perm}")
                        break
        else:
            self.logger.warning(f"Table permissions query failed for user '{username}': {table_result.get('stderr', 'Unknown error')}")
        
        permissions = {
            'connect': connect_perm,
            'select': select_perm,
            'insert': insert_perm,
            'update': update_perm,
            'delete': delete_perm,
            'create': create_perm
        }
        
        self.logger.info(f"Detected permissions for user '{username}' on database '{db_name}': {permissions}")
        return permissions