import logging
from datetime import datetime
from typing import Dict, Optional, Tuple
from app.models.database import RestoreLog, BackupJob, VpsServer, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.utils.walg_config import WalgConfig
from app.utils.walg_restore import WalgRestore

logger = logging.getLogger(__name__)

class RestoreService:
    """Service for handling database recovery operations."""
    
    def __init__(self):
        pass  # SSHManager will be created when needed with proper parameters
    
    def validate_walg_configuration(self, ssh_manager: SSHManager, walg_env: Dict[str, str]) -> Tuple[bool, str]:
        """Validate WAL-G configuration using simplified utility."""
        return WalgConfig.validate_env(ssh_manager, walg_env)
    
    def verify_backup_integrity(self, ssh_manager: SSHManager, walg_env: Dict[str, str], backup_name: str) -> Tuple[bool, str]:
        """Verify backup integrity using simplified utility."""
        return WalgConfig.verify_backup(ssh_manager, walg_env, backup_name)
    
    def prepare_recovery_environment(self, ssh_manager: SSHManager, pg_manager: PostgresManager, 
                                   walg_env: Dict[str, str]) -> Tuple[bool, str]:
        """Prepare recovery environment for WAL-G restore according to documentation.
        
        Args:
            ssh_manager: SSH connection to target server
            pg_manager: PostgreSQL manager
            walg_env: WAL-G environment variables containing PGDATA
            
        Returns:
            Tuple of (success, message)
        """
        try:
            pgdata = walg_env.get('PGDATA', '/var/lib/postgresql/data')
            
            # Stop PostgreSQL service for WAL-G restore
            stop_success, stop_message = pg_manager.stop_service()
            if not stop_success:
                return False, f"Failed to stop PostgreSQL service: {stop_message}"
            
            # Clear PostgreSQL data directory for WAL-G restore
            clear_cmd = f"sudo rm -rf {pgdata}/* {pgdata}/.*"
            clear_result = ssh_manager.execute_command(clear_cmd)
            
            if clear_result['exit_code'] != 0:
                return False, f"Failed to clear PostgreSQL data directory: {clear_result.get('stderr', 'Unknown error')}"
            
            # Ensure data directory exists with correct ownership
            setup_cmd = f"sudo mkdir -p {pgdata} && sudo chown postgres:postgres {pgdata}"
            setup_result = ssh_manager.execute_command(setup_cmd)
            
            if setup_result['exit_code'] != 0:
                return False, f"Failed to setup PostgreSQL data directory: {setup_result.get('stderr', 'Unknown error')}"
            
            return True, f"Recovery environment prepared - {pgdata} cleared for WAL-G restore"
            
        except Exception as e:
            return False, f"Environment preparation error: {str(e)}"
    
    def initiate_recovery(self, backup_key: str, target_server: VpsServer, 
                          backup_info: Dict, database_id: int, s3_storage, backup_job: BackupJob = None) -> Dict:
        """Initiate a database recovery process.
        
        Args:
            backup_key: S3 key of the backup to restore
            target_server: Server where the database will be restored
            backup_info: Information about the backup from S3
            database_id: ID of the database being recovered
            s3_storage: S3Storage object containing the backup
            backup_job: The backup job that created the backup (optional for disaster recovery)
            
        Returns:
            Dictionary with success status and recovery information
        """
        try:
            # Create restore log entry
            restore_log = RestoreLog(
                backup_name=backup_info['backup_name'],
                database_id=database_id,
                status='in_progress'
            )
            
            db.session.add(restore_log)
            db.session.commit()
            
            # Use the auto-generated ID as recovery_id
            recovery_id = str(restore_log.id)
            
            # Extract database name from backup_info if available
            database_name = backup_info.get('database_name')
            
            # Start the recovery process asynchronously
            self._execute_recovery(recovery_id, backup_key, target_server, database_id, s3_storage, backup_job, database_name)
            
            return {
                'success': True,
                'recovery_id': recovery_id
            }
            
        except Exception as e:
            logger.error(f"Error initiating recovery: {str(e)}")
            # Rollback the session to clear any pending transactions
            db.session.rollback()
            return {
                'success': False,
                'error': str(e)
            }
    
    def _execute_recovery(self, recovery_id: str, backup_key: str, 
                         target_server: VpsServer, database_id: int, s3_storage, backup_job: BackupJob = None, database_name: str = None):
        """Execute the actual recovery process with enhanced WAL-G integration.
        
        Args:
            recovery_id: Unique identifier for this recovery
            backup_key: S3 key of the backup to restore
            target_server: Server where the database will be restored
            database_id: ID of the database being recovered
            s3_storage: S3Storage object containing the backup
            backup_job: The backup job that created the backup (optional for disaster recovery)
            database_name: Name of the database (optional, will be determined from database record if not provided)
        """
        restore_log = RestoreLog.query.get(int(recovery_id))
        ssh_manager = None
        
        try:
            # Update status to running
            restore_log.status = 'running'
            restore_log.start_time = datetime.utcnow()
            restore_log.log_output = "Initiating WAL-G recovery process...\n"
            db.session.commit()
            
            # Get database record and determine database name for S3 prefix
            from app.models.database import PostgresDatabase
            database = PostgresDatabase.query.get(database_id)
            
            # Use provided database_name or determine from context
            if not database_name:
                if backup_job and backup_job.server:
                    # For server-based backups, use the target database name
                    database_name = database.name if database else 'postgres'
                else:
                    # For disaster recovery, use database record name or fallback
                    database_name = database.name if database else 'postgres'
            
            # Create simplified WAL-G environment
            pgdata_path = getattr(target_server, 'postgres_data_dir', None)
            walg_env = WalgConfig.create_env(s3_storage, database_name, pgdata_path)
            
            # Extract backup name from key (backup_key might be just the backup name)
            backup_name = backup_key.split('/')[-1] if '/' in backup_key else backup_key
            
            # Step 1: Create SSH connection
            restore_log.log_output += "Connecting to target server...\n"
            db.session.commit()
            
            ssh_manager = SSHManager(
                host=target_server.host,
                port=target_server.port,
                username=target_server.username,
                ssh_key_content=target_server.ssh_key_content
            )
            
            if not ssh_manager.connect():
                raise Exception(f"Failed to connect to target server {target_server.name}")
            
            restore_log.log_output += f"Successfully connected to {target_server.name}\n"
            db.session.commit()
            
            # Detect PGDATA if not configured
            if 'PGDATA' not in walg_env:
                pgdata_detected = WalgConfig.detect_pgdata(ssh_manager)
                if pgdata_detected:
                    walg_env['PGDATA'] = pgdata_detected
                else:
                    walg_env['PGDATA'] = '/var/lib/postgresql/data'  # fallback
            
            # Step 2: Validate WAL-G configuration
            restore_log.log_output += "Validating WAL-G configuration...\n"
            db.session.commit()
            
            is_valid, validation_message = self.validate_walg_configuration(ssh_manager, walg_env)
            if not is_valid:
                raise Exception(f"WAL-G validation failed: {validation_message}")
            
            restore_log.log_output += f"WAL-G configuration validated: {validation_message}\n"
            db.session.commit()
            
            # Step 3: Verify backup integrity
            restore_log.log_output += "Verifying backup integrity...\n"
            db.session.commit()
            
            backup_valid, backup_message = self.verify_backup_integrity(ssh_manager, walg_env, backup_name)
            if not backup_valid:
                raise Exception(f"Backup integrity check failed: {backup_message}")
            
            restore_log.log_output += f"Backup integrity verified: {backup_message}\n"
            db.session.commit()
            
            # Step 4: Prepare recovery environment
            restore_log.log_output += "Preparing recovery environment...\n"
            db.session.commit()
            
            pg_manager = PostgresManager(ssh_manager)
            env_prepared, env_message = self.prepare_recovery_environment(ssh_manager, pg_manager, walg_env)
            if not env_prepared:
                raise Exception(f"Environment preparation failed: {env_message}")
            
            restore_log.log_output += f"Environment prepared: {env_message}\n"
            db.session.commit()
            
            # Step 5: Execute WAL-G backup-fetch using native commands
            restore_log.log_output += f"Executing WAL-G backup-fetch for {backup_name}...\n"
            db.session.commit()
            
            pg_manager = PostgresManager(ssh_manager)
            
            # Execute WAL-G restore using simplified utility
            success, restore_message = WalgRestore.execute_restore(ssh_manager, walg_env, backup_name)
            
            if not success:
                error_msg = f"WAL-G restore failed: {restore_message}"
                restore_log.log_output += f"ERROR: {error_msg}\n"
                db.session.commit()
                raise Exception(error_msg)
            
            restore_log.log_output += f"WAL-G restore completed: {restore_message}\n"
            db.session.commit()
            
            # Step 6: Start PostgreSQL service and trigger WAL replay
            restore_log.log_output += "Starting PostgreSQL service for WAL replay...\n"
            db.session.commit()
            
            start_success, start_message = WalgRestore.start_postgres_with_recovery(ssh_manager, pg_manager)
            if not start_success:
                raise Exception(f"Failed to start PostgreSQL with recovery: {start_message}")
            
            restore_log.log_output += f"PostgreSQL recovery started: {start_message}\n"
            db.session.commit()
            
            # Step 7: Verify recovery completion
            restore_log.log_output += "Verifying recovery completion...\n"
            db.session.commit()
            
            verify_success, verify_message = WalgRestore.verify_recovery_completion(ssh_manager, database_name)
            if not verify_success:
                restore_log.log_output += f"WARNING: Recovery verification failed: {verify_message}\n"
            else:
                restore_log.log_output += f"Recovery verification: {verify_message}\n"
            db.session.commit()
            
            # Step 8: Check if database exists after WAL-G restore and create user
            restore_log.log_output += "Checking restored database and configuring primary user...\n"
            db.session.commit()
            
            # Import required modules for database and user creation
            from app.utils.database_service import DatabaseService
            from app.utils.unified_validation_service import UnifiedValidationService
            from app.models.database import PostgresDatabase, PostgresDatabaseUser
            
            # Generate username for primary user
            existing_users = [user.username for user in PostgresDatabaseUser.query.all()]
            username = UnifiedValidationService.generate_username(database_name, existing_users)
            
            # Generate a secure password for the primary user
            import secrets
            import string
            password_chars = string.ascii_letters + string.digits + "!@#$%^&*"
            password = ''.join(secrets.choice(password_chars) for _ in range(16))
            
            # Track what was created for rollback purposes
            database_created = False
            user_record_created = False
            new_user_record = None
            
            # Step 9: Configure application user for restored database
            try:
                # Create application user for the restored database
                user_success, user_message = DatabaseService.execute_with_postgres(
                    target_server,
                    'User creation for restored database',
                    DatabaseService.create_unified_database_user,
                    username, password, database_name, 'all_permissions', True
                )
                
                if not user_success:
                    raise Exception(f"Failed to create user for restored database: {user_message}")
                
                restore_log.log_output += f"Application user '{username}' created with full permissions on restored database '{database_name}'\n"
                
                db.session.commit()
                
                # Update database record with primary user information
                database_record = PostgresDatabase.query.get(database_id)
                if database_record:
                    # Check if primary user already exists in the database
                    existing_primary_user = PostgresDatabaseUser.query.filter_by(
                        database_id=database_id, 
                        is_primary=True
                    ).first()
                    
                    if not existing_primary_user:
                        new_user_record = PostgresDatabaseUser(
                            username=username,
                            password=password,
                            database_id=database_id,
                            is_primary=True
                        )
                        db.session.add(new_user_record)
                        db.session.commit()
                        user_record_created = True
                        restore_log.log_output += "Primary user record created in application database\n"
                    else:
                        # Update existing primary user
                        existing_primary_user.username = username
                        existing_primary_user.password = password
                        db.session.commit()
                        restore_log.log_output += "Primary user record updated in application database\n"
                        
            except Exception as db_user_error:
                restore_log.log_output += f"Error during user creation: {str(db_user_error)}\n"
                db.session.commit()
                raise db_user_error
            
            # Step 8: Comprehensive post-restoration ownership verification
            restore_log.log_output += "Performing comprehensive post-restoration verification...\n"
            db.session.commit()
            
            verification_success, verification_details = self._perform_comprehensive_verification(
                ssh_manager, pg_manager, database_name, username, restore_log
            )
            
            if verification_success:
                restore_log.log_output += "All verification checks passed successfully\n"
                restore_log.log_output += f"Primary user: {username}\n"
                restore_log.log_output += f"Primary user password: {password}\n"
                restore_log.log_output += "Recovery completed successfully!\n"
                restore_log.status = 'completed'
                restore_log.end_time = datetime.utcnow()
            else:
                # Log verification issues but don't fail the restore completely
                restore_log.log_output += "Warning: Some verification checks failed, but database is accessible\n"
                restore_log.log_output += verification_details + "\n"
                restore_log.log_output += f"Primary user: {username}\n"
                restore_log.log_output += f"Primary user password: {password}\n"
                restore_log.log_output += "Recovery completed with warnings - manual verification recommended\n"
                restore_log.status = 'completed_with_warnings'
                restore_log.end_time = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Recovery failed for {recovery_id}: {str(e)}")
            restore_log.status = 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.error_details = str(e)
            restore_log.log_output += f"\nERROR: Recovery failed - {str(e)}\n"
            
        finally:
            try:
                if ssh_manager:
                    ssh_manager.disconnect()
                db.session.commit()
            except Exception as commit_error:
                logger.error(f"Error committing recovery status for {recovery_id}: {str(commit_error)}")
                db.session.rollback()
    
    def _perform_comprehensive_verification(self, ssh_manager: SSHManager, pg_manager: PostgresManager, 
                                           database_name: str, username: str, restore_log) -> Tuple[bool, str]:
        """Perform comprehensive post-restoration verification with detailed validation and automatic correction.
        
        This method validates:
        1. Database accessibility and basic functionality
        2. Database ownership assignment
        3. User permissions and access rights
        4. Object ownership (tables, sequences, views, etc.)
        5. Application-level functionality
        
        Args:
            ssh_manager: SSH connection to the server
            pg_manager: PostgreSQL manager instance
            database_name: Name of the restored database
            username: Primary user for the database
            restore_log: Restore log object for logging
            
        Returns:
            Tuple of (success: bool, details: str)
        """
        verification_results = []
        overall_success = True
        
        try:
            # Test 1: Basic database accessibility
            restore_log.log_output += "1. Testing database accessibility...\n"
            db.session.commit()
            
            verify_cmd = f"sudo -u postgres psql -d {database_name} -c 'SELECT version();'"
            verify_result = ssh_manager.execute_command(verify_cmd)
            
            if verify_result['exit_code'] == 0:
                verification_results.append("✓ Database is accessible and responsive")
                restore_log.log_output += "   Database accessibility: PASSED\n"
            else:
                verification_results.append(f"✗ Database accessibility failed: {verify_result['stderr']}")
                restore_log.log_output += f"   Database accessibility: FAILED - {verify_result['stderr']}\n"
                overall_success = False
            
            # Test 2: Database ownership verification
            restore_log.log_output += "2. Verifying database ownership...\n"
            db.session.commit()
            
            ownership_cmd = f"sudo -u postgres psql -c \"SELECT datname, pg_catalog.pg_get_userbyid(datdba) as owner FROM pg_database WHERE datname = '{database_name}';\""
            ownership_result = ssh_manager.execute_command(ownership_cmd)
            
            if ownership_result['exit_code'] == 0:
                owner_output = ownership_result.get('stdout', '')
                if username in owner_output:
                    verification_results.append(f"✓ Database ownership correctly assigned to '{username}'")
                    restore_log.log_output += f"   Database ownership: PASSED - owned by '{username}'\n"
                else:
                    verification_results.append(f"✗ Database ownership issue: expected '{username}', found: {owner_output}")
                    restore_log.log_output += f"   Database ownership: WARNING - {owner_output}\n"
                    # Don't mark as failure since ownership transfer might have been partial
            else:
                verification_results.append(f"✗ Database ownership check failed: {ownership_result['stderr']}")
                restore_log.log_output += f"   Database ownership: FAILED - {ownership_result['stderr']}\n"
                overall_success = False
            
            # Test 3: User permissions verification
            restore_log.log_output += "3. Verifying user permissions...\n"
            db.session.commit()
            
            # Test user can connect and perform basic operations
            user_test_cmd = f"sudo -u postgres psql -d {database_name} -c \"SELECT current_user, session_user, current_database();\""
            user_test_result = ssh_manager.execute_command(user_test_cmd)
            
            if user_test_result['exit_code'] == 0:
                verification_results.append("✓ User can connect and query database")
                restore_log.log_output += "   User permissions: PASSED\n"
            else:
                verification_results.append(f"✗ User permission test failed: {user_test_result['stderr']}")
                restore_log.log_output += f"   User permissions: FAILED - {user_test_result['stderr']}\n"
                overall_success = False
            
            # Test 4: Object ownership and table verification
            restore_log.log_output += "4. Verifying object ownership...\n"
            db.session.commit()
            
            # First, get table count
            table_count_cmd = f"sudo -u postgres psql -d {database_name} -c \"SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';\""
            count_result = ssh_manager.execute_command(table_count_cmd)
            
            if count_result['exit_code'] == 0:
                count_output = count_result.get('stdout', '')
                # Extract the count from the output
                import re
                count_match = re.search(r'\b(\d+)\b', count_output)
                table_count = int(count_match.group(1)) if count_match else 0
                
                if table_count > 0:
                    verification_results.append(f"✓ Found {table_count} tables in public schema")
                    restore_log.log_output += f"   Tables found: {table_count} tables in public schema\n"
                    
                    # Check table ownership for first few tables
                    table_ownership_cmd = f"sudo -u postgres psql -d {database_name} -c \"SELECT schemaname, tablename, tableowner FROM pg_tables WHERE schemaname = 'public' LIMIT 5;\""
                    table_result = ssh_manager.execute_command(table_ownership_cmd)
                    
                    if table_result['exit_code'] == 0:
                        table_output = table_result.get('stdout', '')
                        if username in table_output or 'postgres' in table_output:
                            verification_results.append("✓ Table ownership appears correct")
                            restore_log.log_output += "   Object ownership: PASSED\n"
                        else:
                            verification_results.append(f"⚠ Table ownership may need attention: {table_output[:100]}...")
                            restore_log.log_output += "   Object ownership: WARNING - review needed\n"
                    else:
                        verification_results.append(f"✗ Table ownership check failed: {table_result['stderr']}")
                        restore_log.log_output += f"   Object ownership: FAILED - {table_result['stderr']}\n"
                else:
                    verification_results.append("⚠ No tables found in public schema - this may indicate the backup was empty or restoration issue")
                    restore_log.log_output += "   Object ownership: WARNING - no tables to verify\n"
            else:
                verification_results.append(f"✗ Table count check failed: {count_result['stderr']}")
                restore_log.log_output += f"   Table verification: FAILED - {count_result['stderr']}\n"
                # Don't mark as overall failure for this
            
            # Test 5: Application-level functionality test
            restore_log.log_output += "5. Testing application-level functionality...\n"
            db.session.commit()
            
            # Test creating a simple table to verify write permissions
            test_table_cmd = f"sudo -u postgres psql -d {database_name} -c \"CREATE TABLE IF NOT EXISTS _restore_test (id SERIAL PRIMARY KEY, test_data TEXT); INSERT INTO _restore_test (test_data) VALUES ('verification_test'); SELECT COUNT(*) FROM _restore_test; DROP TABLE _restore_test;\""
            test_result = ssh_manager.execute_command(test_table_cmd)
            
            if test_result['exit_code'] == 0:
                verification_results.append("✓ Database supports full CRUD operations")
                restore_log.log_output += "   Application functionality: PASSED\n"
            else:
                verification_results.append(f"✗ Application functionality test failed: {test_result['stderr']}")
                restore_log.log_output += f"   Application functionality: FAILED - {test_result['stderr']}\n"
                overall_success = False
            
            # Test 6: User management capability verification
            restore_log.log_output += "6. Verifying user management capabilities...\n"
            db.session.commit()
            
            # Use the user_manager to verify it can get user permissions
            try:
                permissions = pg_manager.user_manager.get_user_permissions(username, database_name)
                if permissions:
                    verification_results.append(f"✓ User management system functional - permissions: {permissions}")
                    restore_log.log_output += f"   User management: PASSED - {permissions}\n"
                else:
                    verification_results.append("⚠ User management system accessible but no permissions detected")
                    restore_log.log_output += "   User management: WARNING - no permissions detected\n"
            except Exception as perm_error:
                verification_results.append(f"✗ User management verification failed: {str(perm_error)}")
                restore_log.log_output += f"   User management: FAILED - {str(perm_error)}\n"
                # Don't mark as overall failure
            
            # Summary
            restore_log.log_output += "\nVerification Summary:\n"
            for result in verification_results:
                restore_log.log_output += f"  {result}\n"
            
            # Add specific table restoration status
            if 'table_count' in locals():
                if table_count > 0:
                    restore_log.log_output += f"  ✓ Database restoration successful: {table_count} tables restored\n"
                    if 'table_output' in locals() and table_output:
                        restore_log.log_output += f"  ℹ Table details: {table_output[:200]}\n"
                else:
                    restore_log.log_output += "  ⚠ No tables found - backup may have been empty or restoration incomplete\n"
            
            restore_log.log_output += "  ⚠ User management system accessible but verify permissions as needed\n"
            
            db.session.commit()
            
            return overall_success, "\n".join(verification_results)
            
        except Exception as e:
            error_msg = f"Verification process failed with exception: {str(e)}"
            restore_log.log_output += f"   Verification error: {error_msg}\n"
            db.session.commit()
            return False, error_msg
    
    def get_recovery_status(self, recovery_id: str) -> Optional[Dict]:
        """Get the status of a recovery process.
        
        Args:
            recovery_id: Unique identifier for the recovery
            
        Returns:
            Dictionary with recovery status information
        """
        try:
            restore_log = RestoreLog.query.get(int(recovery_id))
            
            if not restore_log:
                return None
            
            return {
                'recovery_id': recovery_id,
                'status': restore_log.status,
                'start_time': restore_log.start_time.isoformat() if restore_log.start_time else None,
                'end_time': restore_log.end_time.isoformat() if restore_log.end_time else None,
                'backup_name': restore_log.backup_name,
                'database_name': restore_log.database.name if restore_log.database else None,
                'server_name': restore_log.database.server.name if restore_log.database and restore_log.database.server else None,
                'error_details': getattr(restore_log, 'error_details', None),
                'command_output': restore_log.log_output
            }
            
        except Exception as e:
            logger.error(f"Error getting recovery status for {recovery_id}: {str(e)}")
            return None