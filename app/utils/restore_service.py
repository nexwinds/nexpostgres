import uuid
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
        self.ssh_manager = SSHManager()
    
    def initiate_recovery(self, backup_job: BackupJob, backup_key: str, 
                          target_server: VpsServer, backup_info: Dict) -> Dict:
        """Initiate a database recovery process.
        
        Args:
            backup_job: The backup job that created the backup
            backup_key: S3 key of the backup to restore
            target_server: Server where the database will be restored
            backup_info: Information about the backup from S3
            
        Returns:
            Dictionary with success status and recovery information
        """
        try:
            # Generate unique recovery ID
            recovery_id = str(uuid.uuid4())
            
            # Create restore log entry
            restore_log = RestoreLog(
                backup_name=backup_info['backup_name'],
                database_id=backup_job.database_id,
                status='in_progress'
            )
            restore_log.id = recovery_id  # Set custom ID
            
            db.session.add(restore_log)
            db.session.commit()
            
            # Start the recovery process asynchronously
            self._execute_recovery(recovery_id, backup_job, backup_key, target_server)
            
            return {
                'success': True,
                'recovery_id': recovery_id
            }
            
        except Exception as e:
            logger.error(f"Error initiating recovery: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _execute_recovery(self, recovery_id: str, backup_job: BackupJob, 
                         backup_key: str, target_server: VpsServer):
        """Execute the actual recovery process.
        
        Args:
            recovery_id: Unique identifier for this recovery
            backup_job: The backup job that created the backup
            backup_key: S3 key of the backup to restore
            target_server: Server where the database will be restored
        """
        restore_log = RestoreLog.query.get(recovery_id)
        
        try:
            # Update status to running
            restore_log.status = 'running'
            db.session.commit()
            
            # Connect to target server
            ssh_connection = self.ssh_manager.connect(
                target_server.host,
                target_server.ssh_port,
                target_server.ssh_username,
                target_server.ssh_password
            )
            
            if not ssh_connection:
                raise Exception(f"Failed to connect to target server {target_server.name}")
            
            # Create PostgreSQL manager for this connection
            pg_manager = PostgresManager(ssh_connection)
            
            # Stop PostgreSQL service if running
            stop_success, stop_message = pg_manager.stop_service()
            if not stop_success:
                logger.warning(f"Could not stop PostgreSQL: {stop_message}")
            
            # Set up WAL-G environment variables
            walg_env = {
                'WALG_S3_PREFIX': f"s3://{backup_job.s3_storage.bucket}/backups/{backup_job.database.name}",
                'AWS_ACCESS_KEY_ID': backup_job.s3_storage.access_key,
                'AWS_SECRET_ACCESS_KEY': backup_job.s3_storage.secret_key,
                'AWS_REGION': backup_job.s3_storage.region,
                'PGDATA': target_server.postgres_data_dir or '/var/lib/postgresql/data'
            }
            
            # Create environment setup command
            env_setup = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            
            # Extract backup name from key
            backup_name = backup_key.split('/')[-1]
            
            # Execute WAL-G restore command
            restore_command = f"{env_setup} wal-g backup-fetch {walg_env['PGDATA']} {backup_name}"
            
            stdin, stdout, stderr = ssh_connection.exec_command(restore_command)
            exit_status = stdout.channel.recv_exit_status()
            
            output = stdout.read().decode('utf-8')
            stderr.read().decode('utf-8')  # Read stderr but don't store
            
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
                
            ssh_connection.close()
            
        except Exception as e:
            logger.error(f"Error during recovery {recovery_id}: {str(e)}")
            restore_log.status = 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.error_details = str(e)
        
        finally:
            db.session.commit()
    
    def get_recovery_status(self, recovery_id: str) -> Optional[Dict]:
        """Get the status of a recovery process.
        
        Args:
            recovery_id: Unique identifier for the recovery
            
        Returns:
            Dictionary with recovery status information
        """
        try:
            restore_log = RestoreLog.query.get(recovery_id)
            
            if not restore_log:
                return None
            
            return {
                'recovery_id': recovery_id,
                'status': restore_log.status,
                'start_time': restore_log.start_time.isoformat() if restore_log.start_time else None,
                'end_time': restore_log.end_time.isoformat() if restore_log.end_time else None,
                'backup_name': restore_log.backup_name,
                'database_name': restore_log.database.name if restore_log.database else None,
                'server_name': restore_log.server.name if restore_log.server else None,
                'error_details': restore_log.error_details,
                'command_output': restore_log.command_output
            }
            
        except Exception as e:
            logger.error(f"Error getting recovery status for {recovery_id}: {str(e)}")
            return None