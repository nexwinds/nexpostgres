from app.models.database import BackupJob, PostgresDatabase, RestoreLog, S3Storage, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.utils.scheduler import schedule_backup_job, execute_manual_backup
from datetime import datetime, timedelta
import os
import re
import hashlib
import threading
from functools import lru_cache


class BackupService:
    """Service class for handling backup operations and SSH connections."""
    
    # In-memory cache for configuration validation results
    _config_cache = {}
    _cache_lock = threading.Lock()
    _cache_ttl = 3600  # 1 hour cache TTL
    
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
    def validate_backup_job_data(name, database_id, s3_storage_id, retention_count, backup_job_id=None):
        """Validate backup job form data with one-to-one relationship enforcement.
        
        Args:
            name: Backup job name
            database_id: Database ID
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
        
        # Validate retention count
        if not retention_count or retention_count < 1:
            return False, 'Retention count must be at least 1', None, None
        
        return True, 'Validation successful', database, s3_storage
    
    @staticmethod
    def create_backup_job(name, database_id, cron_expression, s3_storage_id, retention_count):
        """Create and save backup job.
        
        Args:
            name: Backup job name
            database_id: Database ID
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
            cron_expression=cron_expression,
            s3_storage_id=s3_storage_id,
            retention_count=retention_count
        )
        
        db.session.add(backup_job)
        db.session.commit()
        
        return backup_job
    
    @staticmethod
    def update_backup_job(backup_job, name, database_id, cron_expression, enabled, s3_storage_id, retention_count):
        """Update existing backup job.
        
        Args:
            backup_job: Existing backup job
            name: New name
            database_id: New database ID
            cron_expression: New cron expression
            enabled: Whether job is enabled
            s3_storage_id: New S3 storage ID
            retention_count: New retention count
        """
        database = PostgresDatabase.query.get(database_id)
        
        backup_job.name = name
        backup_job.database_id = database_id
        backup_job.vps_server_id = database.vps_server_id
        backup_job.cron_expression = cron_expression
        backup_job.enabled = enabled
        backup_job.s3_storage_id = s3_storage_id
        backup_job.retention_count = retention_count
        
        db.session.commit()
    
    @staticmethod
    def check_and_configure_backup(backup_job):
        """Configure backup system for the given backup job with intelligent caching.
        
        Args:
            backup_job: BackupJob object containing all necessary configuration
            
        Returns:
            dict: {'success': bool, 'message': str}
        """
        # Generate cache key and check for cached result
        cache_key = BackupService._generate_cache_key(backup_job)
        cached_result = BackupService._get_cached_result(cache_key)
        
        if cached_result:
            return {'success': True, 'message': 'WAL-G backup system already configured and validated (cached)'}
        
        def configure_operation(pg_manager):
            # WAL-G is pre-installed during server provisioning, so skip installation check
            
            # Configure WAL-G based on storage type
            if backup_job.s3_storage:
                s3_config = {
                    'bucket': backup_job.s3_storage.bucket,
                    'region': backup_job.s3_storage.region,
                    'endpoint': backup_job.s3_storage.endpoint or '',
                    'access_key': backup_job.s3_storage.access_key,
                    'secret_key': backup_job.s3_storage.secret_key
                }
                success, message = pg_manager.backup_manager.create_walg_config(s3_config, backup_job)
            else:
                return {'success': False, 'message': 'S3 storage configuration is required for WAL-G'}
            
            if not success:
                return {'success': False, 'message': f'Failed to configure WAL-G: {message}'}
            
            # Configure PostgreSQL archiving (required for WAL-G)
            success, message = pg_manager.configure_postgresql_archiving(backup_job.database.name)
            if not success:
                return {'success': False, 'message': f'Failed to configure PostgreSQL archiving: {message}'}
            
            return {'success': True, 'message': 'WAL-G backup system configured successfully'}
        
        try:
            ssh, ssh_message = BackupService.create_ssh_connection(backup_job.database.server)
            if not ssh:
                return {'success': False, 'message': ssh_message}
            
            pg_manager = BackupService.create_postgres_manager(ssh)
            result = configure_operation(pg_manager)
            
            # Cache successful configuration
            if result.get('success'):
                BackupService._cache_result(cache_key, result)
            
            ssh.disconnect()
            return result
        except Exception as e:
            return {'success': False, 'message': f'Configuration error: {str(e)}'}
    
    @staticmethod
    def _generate_cache_key(backup_job):
        """Generate a unique cache key for the backup job configuration.
        
        Args:
            backup_job: BackupJob object
            
        Returns:
            str: SHA256 hash of configuration for caching
        """
        # Use only essential configuration elements for cache key
        key_elements = [
            str(backup_job.id),
            str(backup_job.database_id), 
            str(backup_job.vps_server_id),
            str(backup_job.s3_storage_id),
            str(backup_job.encryption_key or '')
        ]
        key_string = '|'.join(key_elements)
        return hashlib.sha256(key_string.encode()).hexdigest()[:12]
    
    @staticmethod
    def _get_cached_result(cache_key):
        """Get cached configuration result if valid.
        
        Args:
            cache_key: Configuration cache key
            
        Returns:
            dict or None: Cached result if valid, None otherwise
        """
        with BackupService._cache_lock:
            if cache_key in BackupService._config_cache:
                cached_data = BackupService._config_cache[cache_key]
                cache_time = cached_data.get('timestamp', 0)
                
                # Check if cache is still valid (within TTL)
                if (datetime.utcnow().timestamp() - cache_time) < BackupService._cache_ttl:
                    return cached_data.get('result')
                else:
                    # Remove expired cache entry
                    del BackupService._config_cache[cache_key]
        
        return None
    
    @staticmethod
    def _cache_result(cache_key, result):
        """Cache configuration result.
        
        Args:
            cache_key: Configuration cache key
            result: Result to cache
        """
        with BackupService._cache_lock:
            BackupService._config_cache[cache_key] = {
                'result': result,
                'timestamp': datetime.utcnow().timestamp()
            }
            
            # Clean up old cache entries to prevent memory leaks
            current_time = datetime.utcnow().timestamp()
            expired_keys = [
                key for key, data in BackupService._config_cache.items()
                if (current_time - data.get('timestamp', 0)) >= BackupService._cache_ttl
            ]
            
            for key in expired_keys:
                del BackupService._config_cache[key]
    
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
        """Get filtered backup logs from WAL-G/S3.
        
        Args:
            job_id: Filter by backup job ID
            status: Filter by status
            days: Filter by number of days
            
        Returns:
            List: Filtered backup logs
        """
        from app.utils.backup_metadata_service import BackupMetadataService
        
        if job_id:
            return BackupMetadataService.get_backup_logs_for_job(job_id, status, days)
        else:
            return BackupMetadataService.get_all_backup_logs(job_id, status, days)
    
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
    
    # Removed apply_retention_policy - WAL-G handles retention through cleanup commands
    
    @staticmethod
    def delete_backup_job(backup_job):
        """Delete a backup job and its associated logs.
        
        Args:
            backup_job: BackupJob object to delete
            
        Returns:
            tuple: (success, message)
        """
        try:
            # Note: Backup logs are now stored in WAL-G/S3, no database cleanup needed
            
            # Delete the backup job
            db.session.delete(backup_job)
            db.session.commit()
            
            return True, f'Backup job "{backup_job.name}" deleted successfully'
            
        except Exception as e:
            db.session.rollback()
            return False, f'Error deleting backup job: {str(e)}'
    
    @staticmethod
    def get_backup_logs_for_api(backup_job_id):
        """Get backup logs for API endpoint.
        
        Args:
            backup_job_id: Backup job ID
            
        Returns:
            list: List of backup log dictionaries
        """
        from app.utils.backup_metadata_service import BackupMetadataService
        
        logs = BackupMetadataService.get_backup_logs_for_job(backup_job_id, status='success')
        
        return [{
            'id': log['id'],
            'status': log['status'],
            'start_time': log['start_time'],
            'end_time': log['end_time'],
            'backup_type': log['backup_type'],
            'size_mb': log['size_mb'],
            'output': log['log_output'],
            'error_message': log['error_message']
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
        backup_name = None
        if backup_log_id:
            from app.utils.backup_metadata_service import BackupMetadataService
            backup_log = BackupMetadataService.find_backup_by_name_or_time(backup_job_id, backup_name=backup_log_id)
            if backup_log:
                backup_name = backup_log.get('backup_name')
            if not backup_name:
                return False, 'Selected backup does not exist', None, None, None
        
        # Validate recovery time if using point-in-time recovery
        if use_recovery_time and not recovery_time:
            return False, 'Recovery time is required for point-in-time recovery', None, None, None
        
        return True, 'Validation successful', backup_job, database, backup_name
    
    @staticmethod
    def find_backup_name(backup_job, backup_log_id=None):
        """Find backup name for restore operation on source server.
        
        Args:
            backup_job: BackupJob object
            backup_log_id: Optional backup log ID
            
        Returns:
            tuple: (backup_name, updated_backup_log_id)
        """
        ssh = None
        try:
            ssh, ssh_message = BackupService.create_ssh_connection(backup_job.database.server)
            if not ssh:
                return None, backup_log_id
            
            pg_manager = BackupService.create_postgres_manager(ssh)
            backups = pg_manager.list_backups(backup_job.database.name)
            
            if not backups:
                return None, backup_log_id
            
            # If backup_log_id is provided, find corresponding backup
            if backup_log_id:
                from app.utils.backup_metadata_service import BackupMetadataService
                backup_log = BackupMetadataService.find_backup_by_name_or_time(backup_job.id, backup_name=backup_log_id)
                if backup_log:
                    backup_name = backup_log.get('backup_name')
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
            
        except Exception:
            return None, backup_log_id
        finally:
            if ssh:
                ssh.disconnect()
    
    @staticmethod
    def find_backup_name_on_target(backup_job, target_database, backup_log_id=None):
        """Find backup name for restore operation on target server.
        
        Args:
            backup_job: BackupJob object
            target_database: Target database object
            backup_log_id: Optional backup log ID
            
        Returns:
            tuple: (backup_name, updated_backup_log_id)
        """
        ssh = None
        try:
            print(f"DEBUG: Connecting to target server {target_database.server.host}")
            ssh, ssh_message = BackupService.create_ssh_connection(target_database.server)
            if not ssh:
                print(f"DEBUG: Failed to connect to target server: {ssh_message}")
                return None, backup_log_id
            
            print("DEBUG: Connected successfully, creating postgres manager")
            pg_manager = BackupService.create_postgres_manager(ssh)
            
            # Use the original database name from backup_job for listing backups
            print(f"DEBUG: Listing backups for database '{backup_job.database.name}' on target server")
            backups = pg_manager.list_backups(backup_job.database.name)
            
            print(f"DEBUG: Found {len(backups) if backups else 0} backups: {backups}")
            
            if not backups:
                print(f"DEBUG: No backups found for database '{backup_job.database.name}'")
                return None, backup_log_id
            
            # If backup_log_id is provided, find corresponding backup
            if backup_log_id:
                print(f"DEBUG: Looking for specific backup with log_id {backup_log_id}")
                from app.utils.backup_metadata_service import BackupMetadataService
                backup_log = BackupMetadataService.find_backup_by_name_or_time(backup_job.id, backup_name=backup_log_id)
                if backup_log:
                    backup_name = backup_log.get('backup_name')
                    if backup_name:
                        print(f"DEBUG: Found matching backup by time: {backup_name}")
                        return backup_name, backup_log_id
            
            # Use most recent backup
            backup_name = backups[0].get('name', 'latest')
            print(f"DEBUG: Using most recent backup: {backup_name}")
            
            # Try to find corresponding backup log
            if backup_name != 'latest':
                updated_backup_log_id = BackupRestoreService._find_backup_log_by_name(
                    backup_name, backup_job.id
                )
                if updated_backup_log_id:
                    backup_log_id = updated_backup_log_id
                    print(f"DEBUG: Found corresponding backup log: {backup_log_id}")
            
            return backup_name, backup_log_id
            
        except Exception as e:
            print(f"DEBUG: Exception in find_backup_name_on_target: {str(e)}")
            import traceback
            print(f"DEBUG: Traceback: {traceback.format_exc()}")
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
                except (ValueError, TypeError, AttributeError):
                    continue
        return None
    
    @staticmethod
    def _find_backup_log_by_name(backup_name, backup_job_id):
        """Find backup log ID by backup name."""
        try:
            backup_time = BackupRestoreService._parse_backup_name_timestamp(backup_name)
            if not backup_time:
                return None
            
            from app.utils.backup_metadata_service import BackupMetadataService
            backup_log = BackupMetadataService.find_backup_by_name_or_time(backup_job_id, backup_time=backup_time)
            
            if backup_log:
                return backup_log['id']
            
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
        except (ValueError, TypeError, IndexError):
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
        except (ValueError, TypeError, IndexError):
            pass
        return None
    
    @staticmethod
    def get_recovery_points(database):
        """Get available recovery points for a database.
        
        Args:
            database: PostgresDatabase object
            
        Returns:
            tuple: (success, recovery_points_list)
        """
        ssh = None
        try:
            ssh, ssh_message = BackupService.create_ssh_connection(database.server)
            if not ssh:
                return False, []
            
            pg_manager = BackupService.create_postgres_manager(ssh)
            backups = pg_manager.list_backups(database.name)
            
            if not backups:
                return True, []
            
            recovery_points = []
            for backup in backups:
                backup_info = backup.get('info', {})
                backup_name = backup.get('name', '')
                timestamp_str = backup_info.get('timestamp', '')
                
                if timestamp_str and backup_name:
                    try:
                        # Parse the timestamp
                        backup_time = BackupRestoreService._parse_backup_timestamp(timestamp_str)
                        if backup_time:
                            recovery_points.append({
                                'datetime': backup_time.isoformat(),
                                'formatted': backup_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'backup_name': backup_name,
                                'type': backup_info.get('type', 'unknown')
                            })
                    except Exception:
                        continue
            
            # Sort by datetime descending (newest first)
            recovery_points.sort(key=lambda x: x['datetime'], reverse=True)
            
            return True, recovery_points
            
        except Exception:
            return False, []
        finally:
            if ssh:
                ssh.disconnect()
    
    @staticmethod
    def create_restore_log(database_id, backup_name, recovery_time, use_recovery_time):
        """Create restore log entry.
        
        Args:
            database_id: Database ID
            backup_name: WAL-G backup name
            recovery_time: Recovery time string
            use_recovery_time: Whether using point-in-time recovery
            
        Returns:
            RestoreLog: Created restore log
        """
        restore_log = RestoreLog(
            database_id=database_id,
            backup_name=backup_name if backup_name else None,
            restore_point=datetime.fromisoformat(recovery_time) if recovery_time and use_recovery_time else None,
            status='in_progress'
        )
        
        db.session.add(restore_log)
        db.session.commit()
        
        return restore_log
    
    @staticmethod
    def _execute_restore_operation(database, backup_name, recovery_time, restore_log):
        """Execute restore operation using WAL-G.
        
        Args:
            database: Database object
            backup_name: Name of backup to restore
            recovery_time: Recovery time (optional)
            restore_log: RestoreLog object to update
            
        Returns:
            tuple: (success, message)
        """
        def restore_operation(pg_manager):
            # WAL-G doesn't use stanzas, just restore directly
            success, log_output = pg_manager.restore_database(
                backup_name
            )
            
            # Update restore log
            restore_log.status = 'success' if success else 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = log_output
            
            # Try to find actual backup used from log output
            if success and backup_name and "backup-fetch" in log_output:
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
                
                # If we don't have a backup_name yet, set it from the actual backup used
                if not restore_log.backup_name:
                    restore_log.backup_name = actual_backup_name
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
            
            print(f"DEBUG: Restore operation - Source server ID: {backup_job.database.server.id}, Target server ID: {database.server.id}")
            print(f"DEBUG: Source server host: {backup_job.database.server.host}, Target server host: {database.server.host}")
            print(f"DEBUG: Target database: {database.name} on server: {database.server.name}")
            print(f"DEBUG: Target server SSH details - Host: {database.server.host}, Port: {database.server.port}, Username: {database.server.username}")
            print(f"DEBUG: Target server SSH key length: {len(database.server.ssh_key_content) if database.server.ssh_key_content else 0}")
            print(f"DEBUG: Source database: {backup_job.database.name} on server: {backup_job.database.server.name}")
            
            # Always configure WAL-G on target server for S3 restore (disaster recovery scenario)
            # This ensures we can restore from S3 even if the original server is down
            print("DEBUG: Configuring S3 restore on target server (disaster recovery mode)")
            s3_storage = backup_job.s3_storage
            print(f"DEBUG: S3 storage config - Bucket: {s3_storage.bucket if s3_storage else 'None'}, Region: {s3_storage.region if s3_storage else 'None'}")
            print(f"DEBUG: Backup job encryption key present: {bool(backup_job.encryption_key)}")
            
            config_result = BackupService.check_and_configure_backup(backup_job)
            if not config_result['success']:
                print(f"DEBUG: Backup configuration failed: {config_result['message']}")
                return False, f'Failed to configure backup system on target server: {config_result["message"]}'
            
            print(f"DEBUG: Backup configuration successful: {config_result['message']}")
            
            # Configure WAL-G on target server for the source database
            def configure_walg_operation(pg_manager):
                # For S3 restore, we need to configure WAL-G to access S3 backups
                data_dir = pg_manager.config_manager.get_data_directory()
                if not data_dir:
                    return False, "Could not determine PostgreSQL data directory"
                
                print(f"DEBUG: Configuring WAL-G for database '{backup_job.database.name}' with data_dir '{data_dir}'")
                
                # WAL-G configuration is already handled by check_and_configure_backup
                # Just verify WAL-G is properly installed and configured
                if not pg_manager.backup_manager.is_walg_installed():
                    print("DEBUG: WAL-G not found, installing...")
                    success, message = pg_manager.backup_manager.install_walg()
                    if not success:
                        print(f"DEBUG: WAL-G installation failed: {message}")
                        return False, f"WAL-G installation failed: {message}"
                
                print("DEBUG: WAL-G configured successfully for S3 restore")
                return True, "WAL-G configured for S3 restore"
            
            success, walg_message = BackupService.execute_with_postgres(
                database.server, 'WAL-G configuration', configure_walg_operation
            )
            if not success:
                return False, f'Failed to configure WAL-G on target server: {walg_message}. This is required for S3 restores.'
            

            
            # Find backup name - now check on target server since it should be configured
            backup_name, updated_backup_log_id = BackupRestoreService.find_backup_name_on_target(backup_job, database, backup_log_id)
            if not backup_name:
                return False, 'No suitable backup found for restore. Ensure the target server has access to the S3 storage with the correct encryption key.'
            
            # Create restore log
            restore_log = self.create_restore_log(
                database.id, 
                backup_name, 
                recovery_time, 
                use_recovery_time
            )
            
            # Execute restore using WAL-G
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