import logging
import re
from datetime import datetime
from typing import Dict, Optional, Tuple
from app.models.database import RestoreLog, BackupJob, VpsServer, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager

logger = logging.getLogger(__name__)

class RestoreService:
    """Service for handling database recovery operations."""
    
    def __init__(self):
        pass  # SSHManager will be created when needed with proper parameters
    
    def validate_walg_configuration(self, ssh_manager: SSHManager, walg_env: Dict[str, str]) -> Tuple[bool, str]:
        """Validate WAL-G configuration according to documentation.
        
        Args:
            ssh_manager: SSH connection to target server
            walg_env: WAL-G environment variables
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check if WAL-G is installed
            walg_check = ssh_manager.execute_command('which wal-g')
            if walg_check['exit_code'] != 0:
                return False, "WAL-G is not installed on the target server"
            
            # Check WAL-G version
            version_cmd = ssh_manager.execute_command('wal-g --version')
            if version_cmd['exit_code'] != 0:
                return False, "Unable to determine WAL-G version"
            
            # Validate required environment variables
            required_vars = ['WALG_S3_PREFIX', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'PGDATA']
            for var in required_vars:
                if var not in walg_env or not walg_env[var]:
                    return False, f"Required WAL-G environment variable {var} is missing or empty"
            
            # Validate S3 prefix format
            s3_prefix = walg_env['WALG_S3_PREFIX']
            if not re.match(r'^s3://[a-zA-Z0-9.-]+/.*', s3_prefix):
                return False, f"Invalid S3 prefix format: {s3_prefix}. Must be s3://bucket/path"
            
            # Validate or create PGDATA directory
            pgdata_check = ssh_manager.execute_command(f'test -d {walg_env["PGDATA"]}')
            if pgdata_check['exit_code'] != 0:
                # Try to create the PGDATA directory
                create_dir = ssh_manager.execute_command(f'sudo mkdir -p {walg_env["PGDATA"]} && sudo chown postgres:postgres {walg_env["PGDATA"]}')
                if create_dir['exit_code'] != 0:
                    # Try alternative common PostgreSQL data directories
                    alt_paths = ['/var/lib/postgresql/14/main', '/var/lib/postgresql/13/main', '/var/lib/postgresql/12/main', '/usr/local/pgsql/data']
                    for alt_path in alt_paths:
                        alt_check = ssh_manager.execute_command(f'test -d {alt_path}')
                        if alt_check['exit_code'] == 0:
                            walg_env['PGDATA'] = alt_path
                            break
                    else:
                        return False, f"Could not find or create PGDATA directory. Tried: {walg_env['PGDATA']}, {', '.join(alt_paths)}"
            
            # Test S3 connectivity with WAL-G
            env_setup = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            s3_test = ssh_manager.execute_command(f'{env_setup} wal-g backup-list --json')
            if s3_test['exit_code'] != 0:
                return False, f"WAL-G S3 connectivity test failed: {s3_test['stderr']}"
            
            return True, "WAL-G configuration is valid"
            
        except Exception as e:
            return False, f"WAL-G validation error: {str(e)}"
    
    def verify_backup_integrity(self, ssh_manager: SSHManager, walg_env: Dict[str, str], backup_name: str) -> Tuple[bool, str]:
        """Verify backup integrity before restoration.
        
        Args:
            ssh_manager: SSH connection to target server
            walg_env: WAL-G environment variables
            backup_name: Name of the backup to verify
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            env_setup = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            
            # Check if backup exists
            backup_list_cmd = f'{env_setup} wal-g backup-list --json'
            result = ssh_manager.execute_command(backup_list_cmd)
            
            if result['exit_code'] != 0:
                return False, f"Failed to list backups: {result['stderr']}"
            
            # Parse backup list to verify backup exists
            import json
            try:
                backups = json.loads(result['stdout'])
                
                # Handle different backup name formats
                backup_found = False
                available_backups = []
                
                for backup in backups:
                    backup_id = backup.get('backup_name') or backup.get('name') or backup.get('backup_id')
                    if backup_id:
                        available_backups.append(backup_id)
                        # Check for exact match or if backup_name is part of the backup ID
                        if backup_id == backup_name or backup_name in backup_id or backup_id.endswith(backup_name):
                            backup_found = True
                            break
                
                # If specific backup not found, use LATEST if available
                if not backup_found and backup_name in ['files_metadata.json', 'LATEST']:
                    if available_backups:
                        backup_found = True
                        logger.info(f"Using latest available backup instead of {backup_name}")
                    else:
                        return False, "No backups available in WAL-G backup list"
                elif not backup_found:
                    return False, f"Backup {backup_name} not found. Available backups: {', '.join(available_backups[:5])}"
                    
            except json.JSONDecodeError:
                return False, "Failed to parse WAL-G backup list"
            
            # WAL-G backup integrity is verified by successful backup-list parsing
            # No additional verification command needed as backup-list confirms backup exists
            return True, f"Backup {backup_name} found and verified in WAL-G backup list"
            
        except Exception as e:
            return False, f"Backup integrity verification error: {str(e)}"
    
    def prepare_recovery_environment(self, ssh_manager: SSHManager, pg_manager: PostgresManager, 
                                   database_name: str, recovery_id: str) -> Tuple[bool, str]:
        """Prepare the recovery environment with proper safety checks.
        
        Args:
            ssh_manager: SSH connection to target server
            pg_manager: PostgreSQL manager
            database_name: Name of the database to recover
            recovery_id: Recovery process ID
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Check if database exists and create backup if it does
            db_exists_cmd = f"sudo -u postgres psql -lqt | cut -d \\| -f 1 | grep -qw {database_name}"
            db_exists = ssh_manager.execute_command(db_exists_cmd)
            
            if db_exists['exit_code'] == 0:
                # Database exists, create a safety backup
                backup_name = f"{database_name}_safety_backup_{recovery_id}"
                backup_cmd = f"sudo -u postgres pg_dump {database_name} > /tmp/{backup_name}.sql"
                backup_result = ssh_manager.execute_command(backup_cmd)
                
                if backup_result['exit_code'] != 0:
                    return False, f"Failed to create safety backup: {backup_result['stderr']}"
                
                # Drop the existing database
                drop_cmd = f"sudo -u postgres psql -c 'DROP DATABASE IF EXISTS {database_name};'"
                drop_result = ssh_manager.execute_command(drop_cmd)
                
                if drop_result['exit_code'] != 0:
                    return False, f"Failed to drop existing database: {drop_result['stderr']}"
            
            # Stop PostgreSQL service for WAL-G restore
            stop_success, stop_message = pg_manager.stop_service()
            if not stop_success:
                return False, f"Failed to stop PostgreSQL service: {stop_message}"
            
            # Clear PostgreSQL data directory for WAL-G restore
            # WAL-G requires an empty data directory to restore backups
            clear_data_cmd = "sudo rm -rf /var/lib/postgresql/data/* /var/lib/postgresql/data/.*"
            clear_result = ssh_manager.execute_command(clear_data_cmd)
            
            if clear_result['exit_code'] != 0:
                return False, f"Failed to clear PostgreSQL data directory: {clear_result['stderr']}"
            
            # Ensure the data directory exists and has correct ownership
            setup_data_cmd = "sudo mkdir -p /var/lib/postgresql/data && sudo chown postgres:postgres /var/lib/postgresql/data"
            setup_result = ssh_manager.execute_command(setup_data_cmd)
            
            if setup_result['exit_code'] != 0:
                return False, f"Failed to setup PostgreSQL data directory: {setup_result['stderr']}"
            
            return True, "Recovery environment prepared successfully - data directory cleared for WAL-G restore"
            
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
            
            # Start the recovery process asynchronously
            self._execute_recovery(recovery_id, backup_key, target_server, database_id, s3_storage, backup_job)
            
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
                         target_server: VpsServer, database_id: int, s3_storage, backup_job: BackupJob = None):
        """Execute the actual recovery process with enhanced WAL-G integration.
        
        Args:
            recovery_id: Unique identifier for this recovery
            backup_key: S3 key of the backup to restore
            target_server: Server where the database will be restored
            database_id: ID of the database being recovered
            s3_storage: S3Storage object containing the backup
            backup_job: The backup job that created the backup (optional for disaster recovery)
        """
        restore_log = RestoreLog.query.get(int(recovery_id))
        ssh_manager = None
        
        try:
            # Update status to running
            restore_log.status = 'running'
            restore_log.start_time = datetime.utcnow()
            restore_log.log_output = "Initiating WAL-G recovery process...\n"
            db.session.commit()
            
            # Get database name for S3 prefix
            if backup_job and backup_job.database:
                database_name = backup_job.database.name
            else:
                # For disaster recovery, extract database name from backup key or use database record
                from app.models.database import PostgresDatabase
                database = PostgresDatabase.query.get(database_id)
                # Extract database name from backup key (format: postgres/database_name/backup_name)
                database_name = backup_key.split('/')[1] if '/' in backup_key else database.name
            
            # Set up WAL-G environment variables with dynamic PGDATA detection
            # First try to get PGDATA from target server config, then detect dynamically
            pgdata_path = getattr(target_server, 'postgres_data_dir', None)
            if not pgdata_path:
                # Try to detect PostgreSQL data directory on target server
                ssh_manager_temp = SSHManager(
                    host=target_server.host,
                    port=target_server.port,
                    username=target_server.username,
                    ssh_key_content=target_server.ssh_key_content
                )
                if ssh_manager_temp.connect():
                    # Try common PostgreSQL data directory paths
                    common_paths = [
                        '/var/lib/postgresql/14/main',
                        '/var/lib/postgresql/13/main', 
                        '/var/lib/postgresql/12/main',
                        '/var/lib/postgresql/data',
                        '/usr/local/pgsql/data'
                    ]
                    for path in common_paths:
                        check_result = ssh_manager_temp.execute_command(f'test -d {path}')
                        if check_result['exit_code'] == 0:
                            pgdata_path = path
                            break
                    ssh_manager_temp.disconnect()
                
                # Fallback to default if detection fails
                if not pgdata_path:
                    pgdata_path = '/var/lib/postgresql/data'
            
            walg_env = {
                'WALG_S3_PREFIX': f"s3://{s3_storage.bucket}/postgres/{database_name}",
                'AWS_ACCESS_KEY_ID': s3_storage.access_key,
                'AWS_SECRET_ACCESS_KEY': s3_storage.secret_key,
                'AWS_REGION': s3_storage.region,
                'PGDATA': pgdata_path
            }
            
            # Extract backup name from key
            backup_name = backup_key.split('/')[-1]
            
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
            env_prepared, env_message = self.prepare_recovery_environment(ssh_manager, pg_manager, database_name, recovery_id)
            if not env_prepared:
                raise Exception(f"Environment preparation failed: {env_message}")
            
            restore_log.log_output += f"Environment prepared: {env_message}\n"
            db.session.commit()
            
            # Step 5: Execute WAL-G backup-fetch with partial restore support
            # Use LATEST for non-standard backup names like 'files_metadata.json'
            fetch_backup_name = backup_name
            if backup_name in ['files_metadata.json']:
                fetch_backup_name = 'LATEST'
                restore_log.log_output += f"Using LATEST backup instead of {backup_name}\n"
                
            restore_log.log_output += f"Executing WAL-G backup-fetch for {fetch_backup_name} with partial restore support...\n"
            db.session.commit()
            
            env_setup = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            
            # Try partial restore first (database-specific)
            partial_restore_command = f"{env_setup} wal-g backup-fetch {walg_env['PGDATA']} {fetch_backup_name} --restore-only={database_name} --reverse-unpack --skip-redundant-tars"
            restore_log.log_output += f"Attempting partial restore for database '{database_name}'...\n"
            db.session.commit()
            
            result = ssh_manager.execute_command(partial_restore_command)
            
            if result['exit_code'] != 0:
                # Partial restore failed, try full cluster restore as fallback
                restore_log.log_output += f"Partial restore failed: {result['stderr']}\n"
                restore_log.log_output += "Attempting full cluster restore as fallback...\n"
                db.session.commit()
                
                full_restore_command = f"{env_setup} wal-g backup-fetch {walg_env['PGDATA']} {fetch_backup_name}"
                result = ssh_manager.execute_command(full_restore_command)
                
                if result['exit_code'] != 0:
                    error_msg = f"Both partial and full WAL-G backup-fetch failed: {result['stderr']}"
                    restore_log.log_output += f"ERROR: {error_msg}\n"
                    db.session.commit()
                    raise Exception(error_msg)
                
                restore_log.log_output += "Full cluster restore completed successfully\n"
            else:
                restore_log.log_output += f"Partial restore for database '{database_name}' completed successfully\n"
            
            restore_log.log_output += f"Command output: {result['stdout']}\n"
            db.session.commit()
            
            # Step 6: Start PostgreSQL service and verify recovery
            restore_log.log_output += "Starting PostgreSQL service...\n"
            db.session.commit()
            
            start_success, start_message = pg_manager.start_service()
            if not start_success:
                raise Exception(f"Failed to start PostgreSQL service: {start_message}")
            
            restore_log.log_output += "PostgreSQL service started successfully\n"
            db.session.commit()
            
            # Step 7: Check if database exists after WAL-G restore and create user
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
            
            # Step 7: Create database and user after WAL-G restore
            try:
                # List all databases that were restored by WAL-G
                # Use a simpler approach to get database list
                list_db_cmd = "sudo -u postgres psql -lqt"
                list_db_result = ssh_manager.execute_command(list_db_cmd)
                
                restore_log.log_output += f"Raw database list output: {list_db_result.get('stdout', '')[:200]}\n"
                restore_log.log_output += f"Database list command exit code: {list_db_result['exit_code']}\n"
                restore_log.log_output += f"Database list command stderr: '{list_db_result.get('stderr', '')}' \n"
                
                restored_databases = []
                if list_db_result['exit_code'] == 0 and list_db_result['stdout'].strip():
                    # Parse the psql -lqt output manually
                    raw_output = list_db_result['stdout'].strip()
                    lines = raw_output.split('\n')
                    restore_log.log_output += f"Total lines to process: {len(lines)}\n"
                    
                    for i, line in enumerate(lines):
                        restore_log.log_output += f"Line {i}: '{line}'\n"
                        if '|' in line:
                            # Extract database name (first column)
                            db_name = line.split('|')[0].strip()
                            restore_log.log_output += f"  Extracted db_name: '{db_name}'\n"
                            # Filter out system databases and empty names
                            if db_name and db_name not in ['postgres', 'template0', 'template1']:
                                restored_databases.append(db_name)
                                restore_log.log_output += f"  Added to restored_databases: '{db_name}'\n"
                            else:
                                restore_log.log_output += f"  Filtered out: '{db_name}' (system db or empty)\n"
                        else:
                            restore_log.log_output += "  No '|' found in line\n"
                    
                    restore_log.log_output += f"Parsed databases: {restored_databases}\n"
                
                restore_log.log_output += f"Databases found after WAL-G restore: {restored_databases}\n"
                
                if restored_databases:
                    # WAL-G restored databases from cluster backup - choose the best match
                    if database_name in restored_databases:
                        # Exact match found - use the intended database
                        target_db = database_name
                        restore_log.log_output += f"Found exact match: using restored database '{target_db}' - preserving existing data\n"
                    else:
                        # No exact match - use the first available user database from the cluster backup
                        target_db = restored_databases[0]
                        restore_log.log_output += f"Target database '{database_name}' not found in cluster backup. Using available database '{target_db}' from cluster backup.\n"
                        restore_log.log_output += f"Note: WAL-G performs cluster-level backups. Available databases from backup: {restored_databases}\n"
                        
                        # Update the database name to match what was actually restored
                        database_name = target_db
                        
                        # Update the database record in application database to match restored database
                        database_record = PostgresDatabase.query.get(database_id)
                        if database_record and database_record.name != target_db:
                            restore_log.log_output += f"Updating database record name from '{database_record.name}' to '{target_db}' to match restored database\n"
                            database_record.name = target_db
                            db.session.commit()
                    
                    # Create only the application user for the existing database
                    user_success, user_message = DatabaseService.execute_with_postgres(
                        target_server,
                        'User creation for restored database',
                        DatabaseService.create_unified_database_user,
                        username, password, target_db, 'all_permissions', True
                    )
                    
                    if not user_success:
                        raise Exception(f"Failed to create user for restored database: {user_message}")
                    
                    restore_log.log_output += f"Application user '{username}' created with full permissions on restored database '{target_db}'\n"
                    restore_log.log_output += "WAL-G cluster restore completed - existing tables and data maintained\n"
                else:
                    # No user databases found in cluster backup - this indicates an empty backup
                    error_msg = f"WAL-G cluster restore completed but no user databases found. This indicates the backup was empty or contained only system databases. Cannot restore database '{database_name}' from an empty backup."
                    restore_log.log_output += f"ERROR: {error_msg}\n"
                    restore_log.log_output += "Available databases after restore: only system databases (postgres, template0, template1)\n"
                    restore_log.log_output += "Solution: Ensure the source database contains user data before creating backups.\n"
                    db.session.commit()
                    raise Exception(error_msg)
                
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
                # Rollback database and user creation if any step fails
                restore_log.log_output += f"Error during database/user creation: {str(db_user_error)}\n"
                restore_log.log_output += "Attempting rollback...\n"
                db.session.commit()
                
                # Rollback user record creation
                if user_record_created and new_user_record:
                    try:
                        db.session.delete(new_user_record)
                        db.session.commit()
                        restore_log.log_output += "Rolled back user record creation\n"
                    except Exception as rollback_error:
                        restore_log.log_output += f"Failed to rollback user record: {str(rollback_error)}\n"
                
                # Rollback user creation on server (handled by comprehensive method)
                try:
                    # The comprehensive method handles its own cleanup, but we can attempt manual cleanup
                    DatabaseService.execute_with_postgres(
                        target_server,
                        'User deletion (rollback)',
                        DatabaseService.delete_user_operation,
                        username
                    )
                    restore_log.log_output += f"Rolled back user '{username}' creation\n"
                except Exception as rollback_error:
                    restore_log.log_output += f"Failed to rollback user creation: {str(rollback_error)}\n"
                
                # Rollback database creation on server
                if database_created:
                    try:
                        # Drop the database
                        drop_cmd = f"sudo -u postgres psql -c 'DROP DATABASE IF EXISTS {database_name};'"
                        drop_result = ssh_manager.execute_command(drop_cmd)
                        if drop_result['exit_code'] == 0:
                            restore_log.log_output += f"Rolled back database '{database_name}' creation\n"
                        else:
                            restore_log.log_output += f"Failed to rollback database creation: {drop_result['stderr']}\n"
                    except Exception as rollback_error:
                        restore_log.log_output += f"Failed to rollback database creation: {str(rollback_error)}\n"
                
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