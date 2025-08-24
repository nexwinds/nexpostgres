import logging
from datetime import datetime
from typing import Dict, Optional
from app.models.database import RestoreLog, BackupJob, VpsServer, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager

logger = logging.getLogger(__name__)

class RestoreService:
    """Service for handling database recovery operations."""
    
    def __init__(self):
        pass  # SSHManager will be created when needed with proper parameters
    
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
        """Execute the actual recovery process.
        
        Args:
            recovery_id: Unique identifier for this recovery
            backup_key: S3 key of the backup to restore
            target_server: Server where the database will be restored
            database_id: ID of the database being recovered
            s3_storage: S3Storage object containing the backup
            backup_job: The backup job that created the backup (optional for disaster recovery)
        """
        restore_log = RestoreLog.query.get(int(recovery_id))
        
        try:
            # Update status to running
            restore_log.status = 'running'
            db.session.commit()
            
            # Create SSH manager with target server parameters
            ssh_manager = SSHManager(
                host=target_server.host,
                port=target_server.port,  # SSH port
                username=target_server.username,
                ssh_key_content=target_server.ssh_key_content
            )
            
            # Connect to target server
            ssh_connection_success = ssh_manager.connect()
            
            if not ssh_connection_success:
                raise Exception(f"Failed to connect to target server {target_server.name}")
            
            # Create PostgreSQL manager for this connection
            pg_manager = PostgresManager(ssh_manager)
            
            # Stop PostgreSQL service if running
            stop_success, stop_message = pg_manager.stop_service()
            if not stop_success:
                logger.warning(f"Could not stop PostgreSQL: {stop_message}")
            
            # Get database name for S3 prefix
            if backup_job and backup_job.database:
                database_name = backup_job.database.name
            else:
                # For disaster recovery, extract database name from backup key or use database record
                from app.models.database import PostgresDatabase
                database = PostgresDatabase.query.get(database_id)
                # Extract database name from backup key (format: postgres/database_name/backup_name)
                database_name = backup_key.split('/')[1] if '/' in backup_key else database.name
            
            # Set up WAL-G environment variables
            walg_env = {
                'WALG_S3_PREFIX': f"s3://{s3_storage.bucket}/postgres/{database_name}",
                'AWS_ACCESS_KEY_ID': s3_storage.access_key,
                'AWS_SECRET_ACCESS_KEY': s3_storage.secret_key,
                'AWS_REGION': s3_storage.region,
                'PGDATA': getattr(target_server, 'postgres_data_dir', None) or '/var/lib/postgresql/data'
            }
            
            # Create environment setup command
            env_setup = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            
            # Extract backup name from key
            backup_name = backup_key.split('/')[-1]
            
            # Execute WAL-G restore command
            restore_command = f"{env_setup} wal-g backup-fetch {walg_env['PGDATA']} {backup_name}"
            
            result = ssh_manager.execute_command(restore_command)
            exit_status = result['exit_code']
            output = result['stdout']
            
            if exit_status == 0:
                # Start PostgreSQL service
                start_success, start_message = pg_manager.start_service()
                
                if start_success:
                    restore_log.status = 'completed'
                    restore_log.end_time = datetime.utcnow()
                    restore_log.log_output = output
                    logger.info(f"Recovery {recovery_id} completed successfully")
                else:
                    restore_log.status = 'failed'
                    restore_log.end_time = datetime.utcnow()
                    restore_log.log_output = f"Failed to start PostgreSQL: {start_message}\n{output}"
            else:
                restore_log.status = 'failed'
                restore_log.end_time = datetime.utcnow()
                restore_log.log_output = f"WAL-G restore failed with exit code {exit_status}\n{output}"
                
            ssh_manager.disconnect()
            
        except Exception as e:
            logger.error(f"Error during recovery {recovery_id}: {str(e)}")
            if restore_log:
                restore_log.status = 'failed'
                restore_log.end_time = datetime.utcnow()
                # Add error_details field if it exists, otherwise use log_output
                if hasattr(restore_log, 'error_details'):
                    restore_log.error_details = str(e)
                else:
                    restore_log.log_output = f"Error: {str(e)}"
        
        finally:
            try:
                db.session.commit()
            except Exception as commit_error:
                logger.error(f"Error committing recovery status for {recovery_id}: {str(commit_error)}")
                db.session.rollback()
    
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