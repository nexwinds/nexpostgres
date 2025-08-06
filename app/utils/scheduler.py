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
        
        # Configure pgBackRest and create stanza if needed
        logger.info(f"Configuring pgBackRest for job {job.id}...")
        from app.utils.backup_service import BackupService
        config_result = BackupService.check_and_configure_backup(job)
        if not config_result['success']:
            error_msg = f"Failed to configure backup: {config_result['message']}"
            logger.error(error_msg)
            raise Exception(error_msg)
                
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
                
                # Get backup size from pgBackRest's native reporting
                try:
                    backups = pg_manager.backup_manager.list_backups(job.database.name)
                    if backups:
                        latest_backup = backups[-1]
                        backup_log.size_bytes = latest_backup.get('size', 0)
                        logger.info(f"Backup size: {backup_log.size_bytes} bytes")
                except Exception as e:
                    logger.warning(f"Could not retrieve backup size: {str(e)}")
                    backup_log.size_bytes = 0  # Let pgBackRest handle size reporting
                    
            except Exception as e:
                logger.warning(f"Backup execution error: {str(e)}")
                backup_log.size_bytes = 0
    
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