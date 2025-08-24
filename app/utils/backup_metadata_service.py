from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re
import logging
import boto3
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.models.database import BackupJob

logger = logging.getLogger(__name__)

class BackupMetadataService:
    """Service to retrieve backup metadata directly from WAL-G/S3 instead of database storage."""
    
    @staticmethod
    def get_backup_logs_for_job(backup_job_id: int, status: Optional[str] = None, days: Optional[int] = None) -> List[Dict]:
        """Get backup logs for a specific backup job directly from WAL-G/server.
        
        Args:
            backup_job_id: Backup job ID
            status: Filter by status (optional)
            days: Filter by number of days (optional)
            
        Returns:
            List of backup log dictionaries
        """
        backup_job = BackupJob.query.get(backup_job_id)
        if not backup_job:
            logger.error(f"Backup job {backup_job_id} not found")
            return []
            
        ssh = None
        try:
            # Connect to server
            ssh = SSHManager(
                host=backup_job.database.server.host,
                port=backup_job.database.server.port,
                username=backup_job.database.server.username,
                ssh_key_content=backup_job.database.server.ssh_key_content
            )
            
            if not ssh.connect():
                logger.error(f"Failed to connect to server {backup_job.database.server.host}")
                return []
                
            pg_manager = PostgresManager(ssh)
            logger.info(f"Attempting to list backups for database: {backup_job.database.name}")
            backups = pg_manager.list_backups(backup_job.database.name)
            
            logger.info(f"Found {len(backups) if backups else 0} backups from WAL-G")
            if backups:
                logger.debug(f"First backup data: {backups[0] if backups else 'None'}")
            
            if not backups:
                logger.warning(f"No backups found for database {backup_job.database.name}")
                return []
                
            # Convert WAL-G backup list to backup log format
            backup_logs = []
            for backup in backups:
                # Parse backup metadata
                backup_log = BackupMetadataService._convert_walg_backup_to_log(backup, backup_job_id)
                
                # Apply filters
                if status and backup_log.get('status') != status:
                    continue
                    
                if days and days != 'all':
                    backup_time = datetime.fromisoformat(backup_log['start_time'])
                    date_threshold = datetime.utcnow() - timedelta(days=int(days))
                    if backup_time < date_threshold:
                        continue
                        
                backup_logs.append(backup_log)
                
            # Sort by start time descending, handling None values
            backup_logs.sort(key=lambda x: x['start_time'] or '', reverse=True)
            return backup_logs
            
        except Exception as e:
            logger.error(f"Error retrieving backup logs for job {backup_job_id}: {str(e)}")
            return []
        finally:
            if ssh:
                ssh.disconnect()
    
    @staticmethod
    def get_all_backup_logs(job_id: Optional[int] = None, status: Optional[str] = None, days: Optional[int] = None) -> List[Dict]:
        """Get all backup logs across all backup jobs.
        
        Args:
            job_id: Filter by backup job ID
            status: Filter by status
            days: Filter by number of days
            
        Returns:
            List of backup log dictionaries
        """
        if job_id:
            return BackupMetadataService.get_backup_logs_for_job(job_id, status, days)
            
        # Get all backup jobs
        backup_jobs = BackupJob.query.all()
        all_logs = []
        
        for job in backup_jobs:
            job_logs = BackupMetadataService.get_backup_logs_for_job(job.id, status, days)
            all_logs.extend(job_logs)
            
        # Sort by start time descending, handling None values
        all_logs.sort(key=lambda x: x['start_time'] or '', reverse=True)
        return all_logs
    
    @staticmethod
    def get_backup_statistics(days: int = 30) -> List[Dict]:
        """Get daily backup statistics for the specified number of days.
        
        Args:
            days: Number of days to get statistics for
            
        Returns:
            List of dictionaries with date, successful, and failed counts
        """
        from collections import defaultdict
        
        # Get recent logs
        recent_logs = BackupMetadataService.get_all_backup_logs(days=days)
        
        # Group by date
        daily_stats = defaultdict(lambda: {'successful': 0, 'failed': 0})
        
        for log in recent_logs:
            if log.get('start_time'):
                try:
                    log_date = datetime.fromisoformat(log['start_time']).date()
                    status = log.get('status', 'unknown')
                    
                    if status == 'success':
                        daily_stats[log_date]['successful'] += 1
                    elif status == 'failed':
                        daily_stats[log_date]['failed'] += 1
                except (ValueError, TypeError):
                    continue
        
        # Convert to list format expected by dashboard
        result = []
        for date, stats in daily_stats.items():
            result.append({
                'date': date,
                'successful': stats['successful'],
                'failed': stats['failed']
            })
        
        # Sort by date
        result.sort(key=lambda x: x['date'])
        return result
    
    @staticmethod
    def get_overall_backup_statistics() -> Dict[str, int]:
        """Get overall backup statistics across all servers.
        
        Returns:
            Dictionary with successful, failed, and in_progress counts
        """
        stats = {'successful': 0, 'failed': 0, 'in_progress': 0}
        
        # Get recent logs from last 30 days
        recent_logs = BackupMetadataService.get_all_backup_logs(days=30)
        
        for log in recent_logs:
            status = log.get('status', 'unknown')
            if status == 'success':
                stats['successful'] += 1
            elif status == 'failed':
                stats['failed'] += 1
            elif status == 'in_progress':
                stats['in_progress'] += 1
                
        return stats
    
    @staticmethod
    def get_failed_backup_jobs():
        """Get backup jobs that have failed in their most recent backup."""
        from app.models.database import BackupJob
        failed_jobs = []
        
        backup_jobs = BackupJob.query.all()
        for job in backup_jobs:
            recent_logs = BackupMetadataService.get_backup_logs_for_job(job.id, days=1)
            if recent_logs and recent_logs[0]['status'] == 'failed':
                failed_jobs.append(job)
        
        return failed_jobs
    
    @staticmethod
    def get_recent_backup_logs(limit: int = 10) -> List[Dict]:
        """Get recent backup logs for dashboard.
        
        Args:
            limit: Maximum number of logs to return
            
        Returns:
            List of recent backup log dictionaries
        """
        all_logs = BackupMetadataService.get_all_backup_logs(days=7)
        return all_logs[:limit]
    
    @staticmethod
    def get_s3_backup_structure() -> Dict[str, List[Dict]]:
        """Get the complete S3 backup folder structure organized by database.
        
        Returns:
            Dictionary with database names as keys and list of backup info as values
        """
        from app.models.database import BackupJob, S3Storage
        backup_structure = {}
        
        # Get all backup jobs with their S3 storage configurations
        backup_jobs = BackupJob.query.join(S3Storage).all()
        
        for job in backup_jobs:
            try:
                # Initialize S3 client
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=job.s3_storage.access_key,
                    aws_secret_access_key=job.s3_storage.secret_key,
                    region_name=job.s3_storage.region
                )
                
                # List objects in the S3 bucket with WAL-G's postgres prefix
                prefix = f"postgres/{job.database.name}/"
                response = s3_client.list_objects_v2(
                    Bucket=job.s3_storage.bucket,
                    Prefix=prefix
                )
                
                database_backups = []
                if 'Contents' in response:
                    for obj in response['Contents']:
                        # Extract backup information from S3 object
                        backup_info = {
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'backup_name': obj['Key'].split('/')[-1],
                            'job_id': job.id,
                            'job_name': job.name,
                            'database_name': job.database.name,
                            'server_name': job.server.name,
                            'server_id': job.server.id
                        }
                        database_backups.append(backup_info)
                
                # Sort backups by last modified date (newest first)
                database_backups.sort(key=lambda x: x['last_modified'], reverse=True)
                backup_structure[job.database.name] = database_backups
                
            except Exception as e:
                logger.error(f"Error accessing S3 for job {job.name}: {str(e)}")
                backup_structure[job.database.name] = []
        
        return backup_structure
    
    @staticmethod
    def get_database_backups(database_name: str) -> List[Dict]:
        """Get all available backups for a specific database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            List of backup information dictionaries
        """
        backup_structure = BackupMetadataService.get_s3_backup_structure()
        return backup_structure.get(database_name, [])
    
    @staticmethod
    def get_s3_databases_with_metadata(s3_storage) -> List[Dict]:
        """Get databases with metadata from a specific S3 storage.
        
        Args:
            s3_storage: S3Storage instance
            
        Returns:
            List of database dictionaries with metadata (name, last_backup_date, size)
        """
        import boto3
        from botocore.exceptions import ClientError
        
        databases = []
        
        try:
            # Initialize S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=s3_storage.access_key,
                aws_secret_access_key=s3_storage.secret_key,
                region_name=s3_storage.region,
                endpoint_url=s3_storage.endpoint if s3_storage.endpoint else None
            )
            
            # List all objects in the S3 bucket with postgres prefix
            response = s3_client.list_objects_v2(
                Bucket=s3_storage.bucket,
                Prefix='postgres/',
                Delimiter='/'
            )
            
            # Extract database names from common prefixes
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    # Extract database name from prefix like 'postgres/database_name/'
                    database_name = prefix['Prefix'].replace('postgres/', '').rstrip('/')
                    if database_name:
                        # Get metadata for this database
                        db_metadata = BackupMetadataService._get_database_metadata_from_s3(
                            s3_client, s3_storage.bucket, database_name
                        )
                        if db_metadata:
                            databases.append(db_metadata)
            
            # Sort by last backup date (newest first)
            databases.sort(key=lambda x: x.get('last_backup_date', ''), reverse=True)
            
        except ClientError as e:
            logger.error(f"Error accessing S3 storage {s3_storage.name}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error accessing S3 storage {s3_storage.name}: {str(e)}")
            
        return databases
    
    @staticmethod
    def _get_database_metadata_from_s3(s3_client, bucket, database_name) -> Optional[Dict]:
        """Get metadata for a specific database from S3.
        
        Args:
            s3_client: Boto3 S3 client
            bucket: S3 bucket name
            database_name: Name of the database
            
        Returns:
            Dictionary with database metadata or None if no backups found
        """
        try:
            # List objects for this database
            prefix = f"postgres/{database_name}/"
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response or not response['Contents']:
                return None
                
            # Find the most recent backup and calculate total size
            latest_backup = None
            total_size = 0
            
            for obj in response['Contents']:
                total_size += obj['Size']
                if not latest_backup or obj['LastModified'] > latest_backup['LastModified']:
                    latest_backup = obj
            
            if not latest_backup:
                return None
                
            return {
                'name': database_name,
                'last_backup_date': latest_backup['LastModified'].isoformat(),
                'total_size': total_size,
                'backup_count': len(response['Contents']),
                'latest_backup_key': latest_backup['Key']
            }
            
        except Exception as e:
            logger.error(f"Error getting metadata for database {database_name}: {str(e)}")
            return None
    
    @staticmethod
    def get_all_databases_with_backups() -> List[str]:
        """Get list of all databases that have backups available.
        
        Returns:
            List of database names that have backups
        """
        backup_structure = BackupMetadataService.get_s3_backup_structure()
        return [db_name for db_name, backups in backup_structure.items() if backups]
    
    @staticmethod
    def find_backup_by_name_or_time(backup_job_id: int, backup_name: Optional[str] = None, backup_time: Optional[datetime] = None) -> Optional[Dict]:
        """Find a specific backup by name or time.
        
        Args:
            backup_job_id: Backup job ID
            backup_name: Backup name to search for
            backup_time: Backup time to search for
            
        Returns:
            Backup log dictionary or None
        """
        logs = BackupMetadataService.get_backup_logs_for_job(backup_job_id)
        
        for log in logs:
            if backup_name and log.get('backup_name') == backup_name:
                return log
                
            if backup_time and log.get('start_time'):
                try:
                    log_time = datetime.fromisoformat(log['start_time'])
                    time_diff = abs((log_time - backup_time).total_seconds())
                    if time_diff <= 60:  # Within 1 minute
                        return log
                except (ValueError, TypeError):
                    continue
                    
        return None
    
    @staticmethod
    def _convert_walg_backup_to_log(backup: Dict, backup_job_id: int) -> Dict:
        """Convert WAL-G backup info to backup log format.
        
        Args:
            backup: WAL-G backup dictionary
            backup_job_id: Associated backup job ID
            
        Returns:
            Backup log dictionary
        """
        from app.models.database import BackupJob
        
        # Get backup job with relationships
        backup_job = BackupJob.query.get(backup_job_id)
        backup_job_name = backup_job.name if backup_job else f'Job {backup_job_id}'
        
        # Get database and server information through relationships
        database_name = backup_job.database.name if backup_job and backup_job.database else 'Unknown'
        server_name = backup_job.server.name if backup_job and backup_job.server else 'Unknown'
        
        # Get S3 storage information
        s3_bucket = backup_job.s3_storage.bucket if backup_job and backup_job.s3_storage else None
        s3_region = backup_job.s3_storage.region if backup_job and backup_job.s3_storage else None
        
        # Get backup name
        backup_name = backup.get('name', '')
        
        # Use timestamp from backup data with improved field mapping
        start_time = None
        
        # Try start_time field first (from WAL-G detailed output)
        if 'start_time' in backup and backup['start_time']:
            try:
                start_time = datetime.fromisoformat(backup['start_time'].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        # Fallback to timestamp field
        if not start_time and 'timestamp' in backup:
            try:
                start_time = datetime.fromisoformat(backup['timestamp'].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        # Last resort: parse from backup name
        if not start_time:
            start_time = BackupMetadataService._parse_backup_name_timestamp(backup_name)
        
        # Calculate end time and duration using actual WAL-G timing data
        end_time = None
        duration = None
        
        # Helper function to parse datetime with timezone handling
        def parse_datetime(dt_str):
            if not dt_str:
                return None
            try:
                return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                return None
        
        # Try finish_time field first (from WAL-G detailed output)
        end_time = parse_datetime(backup.get('finish_time'))
        
        # Fallback to end_time field
        if not end_time:
            end_time = parse_datetime(backup.get('end_time'))
        
        # Calculate actual duration from start and end times
        if start_time and end_time:
            duration = int((end_time - start_time).total_seconds())
        elif start_time:
            # If we only have start_time, try to derive end_time from duration field
            if 'duration' in backup:
                try:
                    duration = int(backup['duration'])
                    end_time = start_time + timedelta(seconds=duration)
                except (ValueError, TypeError):
                    pass
            
            # If still no duration, set to None instead of estimating
            # This will show as unknown rather than a fake value
            if not duration:
                duration = None
                end_time = None
        
        # Construct backup path in S3
        backup_path = f"postgres/{database_name}/{backup_name}" if database_name != 'Unknown' else backup.get('path', '')
        
        # Get size in bytes and convert to MB using improved field mapping
        size_bytes = backup.get('size', 0)  # This is uncompressed_size from WAL-G
        compressed_size_bytes = backup.get('compressed_size', 0)  # Additional compressed size info
        size_mb = round(size_bytes / (1024 * 1024), 2) if size_bytes > 0 else 0
        compressed_size_mb = round(compressed_size_bytes / (1024 * 1024), 2) if compressed_size_bytes > 0 else 0
        
        return {
            'id': f"{backup_job_id}_{backup_name}",  # Synthetic ID
            'backup_job_id': backup_job_id,
            'backup_job_name': backup_job_name,
            'database_name': database_name,
            'server_name': server_name,
            'backup_name': backup_name,
            'start_time': start_time.isoformat() if start_time else None,
            'end_time': end_time.isoformat() if end_time else None,
            'duration': duration,
            'status': 'success',  # WAL-G only lists successful backups
            'backup_type': backup.get('type', 'full'),
            'size_bytes': size_bytes,  # Uncompressed size from WAL-G
            'size_mb': size_mb,  # Convert bytes to MB for display
            'compressed_size_bytes': compressed_size_bytes,  # Compressed size from WAL-G
            'compressed_size_mb': compressed_size_mb,  # Compressed size in MB
            'is_permanent': backup.get('is_permanent', False),  # WAL-G permanent backup flag
            'wal_file_name': backup.get('wal_file_name', ''),  # WAL file reference
            'hostname': backup.get('hostname', ''),  # Server hostname from backup
            'data_dir': backup.get('data_dir', ''),  # PostgreSQL data directory
            'pg_version': backup.get('pg_version', ''),  # PostgreSQL version
            'start_lsn': backup.get('start_lsn', ''),  # Log Sequence Number start
            'finish_lsn': backup.get('finish_lsn', ''),  # Log Sequence Number finish
            'log_output': f"Backup {backup_name} completed successfully",
            'manual': False,  # Cannot determine from WAL-G data
            'backup_path': backup_path,
            's3_bucket': s3_bucket,
            's3_region': s3_region,
            'error_message': None
        }
    
    @staticmethod
    def _parse_backup_name_timestamp(backup_name: str) -> Optional[datetime]:
        """Parse timestamp from WAL-G backup name.
        
        Args:
            backup_name: WAL-G backup name
            
        Returns:
            Parsed datetime or None
        """
        try:
            # WAL-G backup names typically follow format: base_YYYYMMDDTHHMMSSZ
            timestamp_match = re.search(r'(\d{8}T\d{6}Z)', backup_name)
            if timestamp_match:
                timestamp_str = timestamp_match.group(1)
                return datetime.strptime(timestamp_str, '%Y%m%dT%H%M%SZ')
                
            # Alternative format: YYYY-MM-DDTHH:MM:SSZ
            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', backup_name)
            if timestamp_match:
                timestamp_str = timestamp_match.group(1)
                return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
                
        except (ValueError, AttributeError):
            pass
            
        return None