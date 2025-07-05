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
            func=execute_backup_job,
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
    """Execute a backup job (common function for scheduled and manual backups)"""
    job = BackupJob.query.get(job_id)
    if not job:
        logger.error(f"Backup job {job_id} not found")
        return False, "Backup job not found"
    
    database = job.database
    server = job.server
    s3_storage = job.s3_storage
    
    # Create a backup log entry
    backup_log = BackupLog(
        backup_job_id=job.id,
        status="in_progress",
        backup_type=job.backup_type,
        manual=manual
    )
    
    db.session.add(backup_log)
    db.session.commit()
    
    try:
        # Connect to server
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_path=server.ssh_key_path,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            raise Exception("Failed to connect to server via SSH")
        
        # Setup PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Check if PostgreSQL is installed
        if not pg_manager.check_postgres_installed():
            raise Exception("PostgreSQL is not installed on the server")
        
        # Check if pgBackRest is installed
        if not pg_manager.check_pgbackrest_installed():
            raise Exception("pgBackRest is not installed on the server")
        
        # Verify and fix PostgreSQL configuration
        config_success, config_message = pg_manager.verify_and_fix_postgres_config(database.name)
        if not config_success:
            raise Exception(f"Failed to verify PostgreSQL configuration: {config_message}")
        
        # Configure pgBackRest if S3 storage is set
        if s3_storage:
            success = pg_manager.setup_pgbackrest_config(
                database.name,
                s3_storage.bucket,
                s3_storage.region,
                s3_storage.access_key,
                s3_storage.secret_key
            )
            
            if not success:
                raise Exception("Failed to configure pgBackRest")
        
        # For incremental backups, check if a full backup exists first
        if job.backup_type == 'incr':
            fix_success, fix_message = pg_manager.fix_incremental_backup_config(database.name)
            if not fix_success:
                raise Exception(f"Failed to fix incremental backup configuration: {fix_message}")
        
        # Execute backup
        success, log_output = pg_manager.execute_backup(database.name, job.backup_type)
        
        # Update backup log
        backup_log.status = "success" if success else "failed"
        backup_log.end_time = datetime.utcnow()
        backup_log.log_output = log_output
        
        db.session.commit()
        
        # Disconnect SSH
        ssh.disconnect()
        
        message = "Backup completed successfully" if success else f"Backup failed: {log_output}"
        logger.info(f"Backup job {job.name} (ID: {job.id}) completed with status: {backup_log.status}")
        
        return success, message
        
    except Exception as e:
        # Update backup log with error
        backup_log.status = "failed"
        backup_log.end_time = datetime.utcnow()
        backup_log.log_output = str(e)
        
        db.session.commit()
        
        logger.error(f"Error executing backup job {job.name} (ID: {job.id}): {str(e)}")
        return False, str(e)

def execute_backup_job(job_id):
    """Execute a scheduled backup job"""
    return execute_backup(job_id, manual=False)

def execute_manual_backup(job_id):
    """Execute a manual backup job"""
    return execute_backup(job_id, manual=True) 