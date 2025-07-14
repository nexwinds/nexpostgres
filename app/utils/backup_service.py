from app.models.database import BackupJob, BackupLog, PostgresDatabase, RestoreLog, S3Storage, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.utils.scheduler import schedule_backup_job, execute_manual_backup
from datetime import datetime, timedelta
import os
import time
import re


class BackupService:
    """Service class for handling backup operations and SSH connections."""
    
    @staticmethod
    def create_ssh_connection(server):
        """Create and test SSH connection to server.
        
        Args:
            server: Server object with connection details
            
        Returns:
            tuple: (ssh_manager, success_message) or (None, error_message)
        """
        try:
            ssh = SSHManager(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_content=server.ssh_key_content
            )
            
            if not ssh.connect():
                return None, 'Failed to connect to server via SSH'
            
            return ssh, 'SSH connection established successfully'
        except Exception as e:
            return None, f'SSH connection error: {str(e)}'
    
    @staticmethod
    def create_postgres_manager(ssh):
        """Create PostgreSQL manager from SSH connection.
        
        Args:
            ssh: Active SSH connection
            
        Returns:
            PostgresManager: Configured PostgreSQL manager
        """
        return PostgresManager(ssh)
    
    @staticmethod
    def execute_with_postgres(server, operation_name, operation_func, *args, **kwargs):
        """Execute operation with PostgreSQL manager and proper cleanup.
        
        Args:
            server: Server object
            operation_name: Name of operation for error messages
            operation_func: Function to execute with pg_manager
            *args, **kwargs: Arguments for operation_func
            
        Returns:
            tuple: (success, message)
        """
        ssh = None
        try:
            ssh, ssh_message = BackupService.create_ssh_connection(server)
            if not ssh:
                return False, ssh_message
            
            pg_manager = BackupService.create_postgres_manager(ssh)
            result = operation_func(pg_manager, *args, **kwargs)
            
            return True, result if isinstance(result, str) else 'Operation completed successfully'
            
        except Exception as e:
            return False, f'{operation_name} failed: {str(e)}'
        finally:
            if ssh:
                ssh.disconnect()
    
    @staticmethod
    def validate_backup_job_data(name, database_id, backup_type, s3_storage_id, retention_count, backup_job_id=None):
        """Validate backup job form data with one-to-one relationship enforcement.
        
        Args:
            name: Backup job name
            database_id: Database ID
            backup_type: Type of backup (full/incr)
            s3_storage_id: S3 storage configuration ID
            retention_count: Number of backups to retain
            backup_job_id: Existing backup job ID (for updates, None for new jobs)
            
        Returns:
            tuple: (is_valid, error_message, database, s3_storage)
        """
        from app.utils.unified_validation_service import UnifiedValidationService
        
        # Validate database
        database = PostgresDatabase.query.get(database_id)
        if not database:
            return False, 'Selected database does not exist', None, None
        
        # Validate one-to-one relationship
        is_valid, error = UnifiedValidationService.validate_one_to_one_backup_relationship(database_id, backup_job_id)
        if not is_valid:
            return False, error, None, None
        
        # Validate S3 storage
        s3_storage = S3Storage.query.get(s3_storage_id)
        if not s3_storage:
            return False, 'Selected S3 storage configuration does not exist', None, None
        
        # Validate backup type
        if backup_type not in ['full', 'incr']:
            return False, 'Invalid backup type', None, None
        
        # Validate retention count
        if not retention_count or retention_count < 1:
            return False, 'Retention count must be at least 1', None, None
        
        return True, 'Validation successful', database, s3_storage
    
    @staticmethod
    def create_backup_job(name, database_id, backup_type, cron_expression, s3_storage_id, retention_count):
        """Create and save backup job.
        
        Args:
            name: Backup job name
            database_id: Database ID
            backup_type: Type of backup
            cron_expression: Cron schedule expression
            s3_storage_id: S3 storage ID
            retention_count: Retention count
            
        Returns:
            BackupJob: Created backup job
        """
        database = PostgresDatabase.query.get(database_id)
        
        backup_job = BackupJob(
            name=name,
            database_id=database_id,
            vps_server_id=database.vps_server_id,
            backup_type=backup_type,
            cron_expression=cron_expression,
            s3_storage_id=s3_storage_id,
            retention_count=retention_count
        )
        
        db.session.add(backup_job)
        db.session.commit()
        
        return backup_job
    
    @staticmethod
    def update_backup_job(backup_job, name, database_id, backup_type, cron_expression, enabled, s3_storage_id, retention_count):
        """Update existing backup job.
        
        Args:
            backup_job: Existing backup job
            name: New name
            database_id: New database ID
            backup_type: New backup type
            cron_expression: New cron expression
            enabled: Whether job is enabled
            s3_storage_id: New S3 storage ID
            retention_count: New retention count
        """
        database = PostgresDatabase.query.get(database_id)
        
        backup_job.name = name
        backup_job.database_id = database_id
        backup_job.vps_server_id = database.vps_server_id
        backup_job.backup_type = backup_type
        backup_job.cron_expression = cron_expression
        backup_job.enabled = enabled
        backup_job.s3_storage_id = s3_storage_id
        backup_job.retention_count = retention_count
        
        db.session.commit()
    
    @staticmethod
    def check_and_configure_backup(database, s3_storage):
        """Check and configure backup system if needed.
        
        Args:
            database: Database object
            s3_storage: S3Storage object
            
        Returns:
            tuple: (success, message)
        """
        def configure_operation(pg_manager):
            # Check if configuration is valid
            check_cmd = f"sudo -u postgres pgbackrest --stanza={database.name} check"
            ssh = pg_manager.ssh
            check_result = ssh.execute_command(check_cmd)
            
            if check_result['exit_code'] != 0:
                # Configure pgBackRest based on storage type
                if s3_storage:
                    s3_config = {
                        'bucket': s3_storage.bucket,
                        'region': s3_storage.region,
                        'endpoint': s3_storage.endpoint or '',
                        'access_key': s3_storage.access_key,
                        'secret_key': s3_storage.secret_key
                    }
                    pg_manager.setup_pgbackrest(s3_config)
                else:
                    pg_manager.setup_pgbackrest()
                
                return 'Backup system configured successfully'
            else:
                return 'Backup configuration is valid'
        
        return BackupService.execute_with_postgres(
            database.server, 'Backup configuration check', configure_operation
        )
    
    @staticmethod
    def schedule_backup_job_safe(backup_job):
        """Schedule backup job with error handling.
        
        Args:
            backup_job: BackupJob object
            
        Returns:
            tuple: (success, message)
        """
        try:
            schedule_backup_job(backup_job)
            return True, 'Backup job scheduled successfully'
        except Exception as e:
            return False, f'Scheduling failed: {str(e)}'
    
    def execute_backup(self, backup_job):
        """Execute backup job.
        
        Args:
            backup_job: BackupJob object to execute
            
        Returns:
            tuple: (success, message)
        """
        try:
            success, message = execute_manual_backup(backup_job.id)
            return success, message
        except Exception as e:
            return False, f'Error executing backup: {str(e)}'
    
    @staticmethod
    def execute_backup_safe(backup_job_id):
        """Execute backup with error handling.
        
        Args:
            backup_job_id: ID of backup job to execute
            
        Returns:
            tuple: (success, message)
        """
        try:
            success, message = execute_manual_backup(backup_job_id)
            return success, message
        except Exception as e:
            return False, f'Error executing backup: {str(e)}'
    
    @staticmethod
    def build_backup_logs_query(job_id=None, status=None, days=None):
        """Build filtered query for backup logs.
        
        Args:
            job_id: Filter by backup job ID
            status: Filter by status
            days: Filter by number of days
            
        Returns:
            Query: Filtered backup logs query
        """
        query = BackupLog.query
        
        if job_id:
            query = query.filter(BackupLog.backup_job_id == job_id)
        
        if status:
            query = query.filter(BackupLog.status == status)
        
        if days and days != 'all':
            date_threshold = datetime.utcnow() - timedelta(days=int(days))
            query = query.filter(BackupLog.start_time >= date_threshold)
        
        return query.order_by(BackupLog.start_time.desc())
    
    @staticmethod
    def build_restore_logs_query(database_id=None, status=None, days=None):
        """Build filtered query for restore logs.
        
        Args:
            database_id: Filter by database ID
            status: Filter by status
            days: Filter by number of days
            
        Returns:
            Query: Filtered restore logs query
        """
        query = RestoreLog.query.join(RestoreLog.backup_log, isouter=True)
        
        if database_id:
            query = query.filter(RestoreLog.database_id == database_id)
        
        if status:
            query = query.filter(RestoreLog.status == status)
        
        if days and days != 'all':
            date_threshold = datetime.utcnow() - timedelta(days=int(days))
            query = query.filter(RestoreLog.start_time >= date_threshold)
        
        return query.order_by(RestoreLog.start_time.desc())
    
    @staticmethod
    def apply_retention_policy(backup_job):
        """Apply retention policy to backup job.
        
        Args:
            backup_job: BackupJob object
            
        Returns:
            tuple: (success, message)
        """
        def retention_operation(pg_manager):
            return pg_manager.cleanup_old_backups(
                backup_job.database.name, 
                backup_job.retention_count
            )
        
        return BackupService.execute_with_postgres(
            backup_job.database.server, 'Retention policy application', retention_operation
        )
    
    @staticmethod
    def get_backup_logs_for_api(backup_job_id):
        """Get backup logs for API endpoint.
        
        Args:
            backup_job_id: Backup job ID
            
        Returns:
            list: List of backup log dictionaries
        """
        logs = BackupLog.query.filter_by(
            backup_job_id=backup_job_id,
            status='success'
        ).order_by(BackupLog.end_time.desc()).all()
        
        return [{
            'id': log.id,
            'status': log.status,
            'start_time': log.start_time.strftime('%Y-%m-%d %H:%M:%S') if log.start_time else None,
            'end_time': log.end_time.strftime('%Y-%m-%d %H:%M:%S') if log.end_time else None,
            'backup_type': log.backup_type or 'full',
            'size_mb': round(log.size_bytes / (1024 * 1024), 2) if log.size_bytes else None,
            'output': log.log_output,
            'error_message': None
        } for log in logs]


class BackupRestoreService:
    """Service class for handling backup restore operations."""
    
    @staticmethod
    def validate_restore_data(backup_job_id, database_id, backup_log_id, use_recovery_time, recovery_time):
        """Validate restore form data.
        
        Args:
            backup_job_id: Backup job ID
            database_id: Target database ID
            backup_log_id: Backup log ID
            use_recovery_time: Whether to use point-in-time recovery
            recovery_time: Recovery time string
            
        Returns:
            tuple: (is_valid, error_message, backup_job, database, backup_log)
        """
        # Validate backup job
        if not backup_job_id:
            return False, 'Please select a backup job', None, None, None
        
        backup_job = BackupJob.query.get(backup_job_id)
        if not backup_job:
            return False, 'Selected backup job does not exist', None, None, None
        
        # Set database ID from backup job if not set
        if not database_id:
            database_id = backup_job.database_id
        
        # Validate database
        database = PostgresDatabase.query.get(database_id)
        if not database:
            return False, 'Selected database does not exist', None, None, None
        
        # Validate backup log if provided
        backup_log = None
        if backup_log_id:
            backup_log = BackupLog.query.get(backup_log_id)
            if not backup_log:
                return False, 'Selected backup does not exist', None, None, None
        
        # Validate recovery time if using point-in-time recovery
        if use_recovery_time and not recovery_time:
            return False, 'Recovery time is required for point-in-time recovery', None, None, None
        
        return True, 'Validation successful', backup_job, database, backup_log
    
    @staticmethod
    def find_backup_name(backup_job, backup_log_id=None):
        """Find backup name for restore operation.
        
        Args:
            backup_job: BackupJob object
            backup_log_id: Optional backup log ID
            
        Returns:
            tuple: (backup_name, updated_backup_log_id)
        """
        ssh = None
        try:
            ssh, ssh_message = BackupService.create_ssh_connection(backup_job.server)
            if not ssh:
                return None, backup_log_id
            
            pg_manager = BackupService.create_postgres_manager(ssh)
            backups = pg_manager.list_backups(backup_job.database.name)
            
            if not backups:
                return None, backup_log_id
            
            # If backup_log_id is provided, find corresponding backup
            if backup_log_id:
                backup_log = BackupLog.query.get(backup_log_id)
                if backup_log:
                    backup_name = BackupRestoreService._find_backup_by_time(
                        backups, backup_log.start_time, backup_job.id
                    )
                    if backup_name:
                        return backup_name, backup_log_id
            
            # Use most recent backup
            backup_name = backups[0].get('name', 'latest')
            
            # Try to find corresponding backup log
            if backup_name != 'latest':
                updated_backup_log_id = BackupRestoreService._find_backup_log_by_name(
                    backup_name, backup_job.id
                )
                if updated_backup_log_id:
                    backup_log_id = updated_backup_log_id
            
            return backup_name, backup_log_id
            
        except Exception as e:
            return None, backup_log_id
        finally:
            if ssh:
                ssh.disconnect()
    
    @staticmethod
    def _find_backup_by_time(backups, target_time, backup_job_id):
        """Find backup name by matching timestamp."""
        for backup in backups:
            timestamp_str = backup.get('info', {}).get('timestamp', '')
            if timestamp_str:
                try:
                    backup_time = BackupRestoreService._parse_backup_timestamp(timestamp_str)
                    if backup_time:
                        time_diff = abs((backup_time - target_time).total_seconds())
                        if time_diff <= 60:  # Within 60 seconds
                            return backup.get('name', '')
                except:
                    continue
        return None
    
    @staticmethod
    def _find_backup_log_by_name(backup_name, backup_job_id):
        """Find backup log ID by backup name."""
        try:
            backup_time = BackupRestoreService._parse_backup_name_timestamp(backup_name)
            if not backup_time:
                return None
            
            potential_logs = BackupLog.query.filter(
                BackupLog.backup_job_id == backup_job_id,
                BackupLog.status == 'success',
                BackupLog.start_time >= backup_time - timedelta(seconds=60),
                BackupLog.start_time <= backup_time + timedelta(seconds=60)
            ).all()
            
            if potential_logs:
                closest_log = min(potential_logs, 
                    key=lambda log: abs((log.start_time - backup_time).total_seconds()))
                return closest_log.id
            
        except Exception:
            pass
        
        return None
    
    @staticmethod
    def _parse_backup_timestamp(timestamp_str):
        """Parse backup timestamp string to datetime."""
        try:
            timestamp_parts = timestamp_str.split('-')
            if len(timestamp_parts) >= 6:
                year = int(timestamp_parts[0])
                month = int(timestamp_parts[1])
                day = int(timestamp_parts[2])
                hour = int(timestamp_parts[3])
                minute = int(timestamp_parts[4])
                second = int(timestamp_parts[5].split('.')[0])
                
                return datetime(year, month, day, hour, minute, second)
        except:
            pass
        return None
    
    @staticmethod
    def _parse_backup_name_timestamp(backup_name):
        """Parse timestamp from backup name."""
        try:
            timestamp_parts = backup_name.split('-')
            if len(timestamp_parts) >= 2:
                date_part = timestamp_parts[0]
                time_part = timestamp_parts[1][:-1]  # Remove F or I suffix
                
                year = int(date_part[0:4])
                month = int(date_part[4:6])
                day = int(date_part[6:8])
                hour = int(time_part[0:2])
                minute = int(time_part[2:4])
                second = int(time_part[4:6])
                
                return datetime(year, month, day, hour, minute, second)
        except:
            pass
        return None
    
    @staticmethod
    def create_restore_log(database_id, backup_log_id, recovery_time, use_recovery_time):
        """Create restore log entry.
        
        Args:
            database_id: Database ID
            backup_log_id: Backup log ID
            recovery_time: Recovery time string
            use_recovery_time: Whether using point-in-time recovery
            
        Returns:
            RestoreLog: Created restore log
        """
        restore_log = RestoreLog(
            database_id=database_id,
            backup_log_id=backup_log_id if backup_log_id else None,
            restore_point=datetime.fromisoformat(recovery_time) if recovery_time and use_recovery_time else None,
            status='in_progress'
        )
        
        db.session.add(restore_log)
        db.session.commit()
        
        return restore_log
    
    @staticmethod
    def _execute_restore_operation(database, backup_name, recovery_time, restore_log):
        """Execute restore operation.
        
        Args:
            database: Database object
            backup_name: Name of backup to restore
            recovery_time: Recovery time (optional)
            restore_log: RestoreLog object to update
            
        Returns:
            tuple: (success, message)
        """
        def restore_operation(pg_manager):
            success, log_output = pg_manager.restore_database(
                database.name, backup_name
            )
            
            # Update restore log
            restore_log.status = 'success' if success else 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = log_output
            
            # Try to find actual backup used from log output
            if success and backup_name and "restore backup set" in log_output:
                BackupRestoreService._update_restore_log_with_actual_backup(
                    restore_log, log_output
                )
            
            db.session.commit()
            
            if success:
                return 'Restore completed successfully'
            else:
                return f'Restore failed: {log_output}'
        
        try:
            success, message = BackupService.execute_with_postgres(
                database.server, 'Database restore', restore_operation
            )
            
            if not success:
                # Update restore log with error
                restore_log.status = 'failed'
                restore_log.end_time = datetime.utcnow()
                restore_log.log_output = message
                db.session.commit()
            
            return success, message
            
        except Exception as e:
            # Update restore log with error
            restore_log.status = 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = str(e)
            db.session.commit()
            
            return False, f'Error during restore: {str(e)}'
    
    @staticmethod
    def _update_restore_log_with_actual_backup(restore_log, log_output):
        """Update restore log with actual backup used."""
        try:
            backup_set_match = re.search(r'restore backup set ([0-9\-A-Z]+)', log_output)
            if backup_set_match:
                actual_backup_name = backup_set_match.group(1)
                
                # If we don't have a backup_log_id yet, try to find one
                if not restore_log.backup_log_id:
                    backup_time = BackupRestoreService._parse_backup_name_timestamp(actual_backup_name)
                    if backup_time:
                        # Find matching backup log
                        potential_logs = BackupLog.query.filter(
                            BackupLog.status == 'success',
                            BackupLog.start_time >= backup_time - timedelta(seconds=60),
                            BackupLog.start_time <= backup_time + timedelta(seconds=60)
                        ).all()
                        
                        if potential_logs:
                            closest_log = min(potential_logs, 
                                key=lambda log: abs((log.start_time - backup_time).total_seconds()))
                            restore_log.backup_log_id = closest_log.id
        except Exception:
            pass  # Ignore errors in this optional enhancement
    
    def execute_restore(self, validated_data):
        """Execute restore operation with validated data.
        
        Args:
            validated_data: Dictionary containing validated form data
            
        Returns:
            tuple: (success, message)
        """
        try:
            # Extract data
            backup_job = validated_data['backup_job']
            database = validated_data['database']
            backup_log_id = validated_data.get('backup_log_id')
            use_recovery_time = validated_data.get('use_recovery_time', False)
            recovery_time = validated_data.get('recovery_time')
            
            # Find backup name
            backup_name, updated_backup_log_id = self.find_backup_name(backup_job, backup_log_id)
            if not backup_name:
                return False, 'No suitable backup found for restore'
            
            # Create restore log
            restore_log = self.create_restore_log(
                database.id, 
                updated_backup_log_id, 
                recovery_time, 
                use_recovery_time
            )
            
            # Execute restore
            success, message = self._execute_restore_operation(
                database, 
                backup_name, 
                recovery_time, 
                restore_log
            )
            
            return success, message
            
        except Exception as e:
            return False, f'Error during restore operation: {str(e)}'


class S3TestService:
    """Service class for testing S3 connections."""
    
    @staticmethod
    def test_s3_connection(bucket, region, access_key, secret_key):
        """Test S3 connection with provided credentials.
        
        Args:
            bucket: S3 bucket name
            region: AWS region
            access_key: AWS access key
            secret_key: AWS secret key
            
        Returns:
            tuple: (success, message)
        """
        try:
            # Create test file
            test_content = f"Test file for S3 connection to {bucket} - {datetime.utcnow().isoformat()}"
            test_file = f"/tmp/s3_test_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.txt"
            
            # Write test script
            script_content = f"""#!/bin/bash
export AWS_ACCESS_KEY_ID="{access_key}"
export AWS_SECRET_ACCESS_KEY="{secret_key}"
export AWS_DEFAULT_REGION="{region}"

# Create test file
echo "{test_content}" > {test_file}

# Try to upload to S3
aws s3 cp {test_file} s3://{bucket}/test/ 2>&1

# Check result
if [ $? -eq 0 ]; then
  echo "S3 connection successful"
  exit 0
else
  echo "S3 connection failed"
  exit 1
fi
"""
            
            script_file = f"/tmp/s3_test_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.sh"
            
            with open(script_file, 'w') as f:
                f.write(script_content)
            
            # Make script executable
            os.chmod(script_file, 0o755)
            
            # Execute script
            result = os.popen(script_file).read()
            
            # Clean up
            os.remove(script_file)
            if os.path.exists(test_file):
                os.remove(test_file)
            
            # Check result
            if "S3 connection successful" in result:
                return True, 'S3 connection successful'
            else:
                return False, f'S3 connection failed: {result}'
                
        except Exception as e:
            return False, f'Error testing S3 connection: {str(e)}'