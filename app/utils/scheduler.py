import logging
from datetime import datetime
from flask_apscheduler import APScheduler
from app.models.database import BackupJob, BackupLog, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager

scheduler = APScheduler()
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
    
    # Schedule new job
    scheduler.add_job(
        func=execute_backup_job,
        trigger='cron',
        id=job_id,
        name=f"Backup job {job.name}",
        args=[job.id],
        **parse_cron_expression(job.cron_expression)
    )
    
    logger.info(f"Scheduled backup job {job.name} (ID: {job.id})")

def parse_cron_expression(cron_expression):
    """Parse cron expression into APScheduler parameters"""
    # Basic cron expression format: minute hour day_of_month month day_of_week
    parts = cron_expression.strip().split()
    
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expression}")
    
    return {
        'minute': parts[0],
        'hour': parts[1],
        'day': parts[2],
        'month': parts[3],
        'day_of_week': parts[4]
    }

def execute_backup_job(job_id):
    """Execute a backup job"""
    logger.info(f"Executing backup job with ID: {job_id}")
    
    # Get job from database
    job = BackupJob.query.get(job_id)
    if not job:
        logger.error(f"Backup job with ID {job_id} not found")
        return
    
    # Create backup log entry
    backup_log = BackupLog(
        backup_job_id=job.id,
        status='in_progress',
        backup_type=job.backup_type,
        is_manual=False
    )
    
    db.session.add(backup_log)
    db.session.commit()
    
    try:
        # Connect to the server
        ssh = SSHManager(
            host=job.server.host,
            port=job.server.port,
            username=job.server.username,
            ssh_key_path=job.server.ssh_key_path,
            ssh_key_content=job.server.ssh_key_content
        )
        
        if not ssh.connect():
            raise ConnectionError(f"Failed to connect to server {job.server.name}")
        
        # Initialize PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Execute backup
        success, log_output = pg_manager.execute_backup(job.database.name, job.backup_type)
        
        # Update backup log
        backup_log.status = 'success' if success else 'failed'
        backup_log.end_time = datetime.utcnow()
        backup_log.log_output = log_output
        
        # Calculate backup size if available
        if success:
            backups = pg_manager.list_backups(job.database.name)
            if backups:
                last_backup = backups[0]  # Assuming the first backup is the latest
                if 'size' in last_backup.get('info', {}):
                    try:
                        backup_log.size_bytes = int(last_backup['info']['size'])
                    except (ValueError, TypeError):
                        pass
        
        # Disconnect from server
        ssh.disconnect()
        
    except Exception as e:
        logger.exception(f"Error executing backup job {job.name}: {str(e)}")
        
        # Update backup log on error
        backup_log.status = 'failed'
        backup_log.end_time = datetime.utcnow()
        backup_log.log_output = str(e)
    
    finally:
        # Save backup log
        db.session.commit()

def execute_manual_backup(job_id):
    """Execute a manual backup job"""
    logger.info(f"Executing manual backup for job ID: {job_id}")
    
    # Get job from database
    job = BackupJob.query.get(job_id)
    if not job:
        logger.error(f"Backup job with ID {job_id} not found")
        return False, "Job not found"
    
    # Create backup log entry
    backup_log = BackupLog(
        backup_job_id=job.id,
        status='in_progress',
        backup_type=job.backup_type,
        is_manual=True
    )
    
    db.session.add(backup_log)
    db.session.commit()
    
    try:
        # Connect to the server
        ssh = SSHManager(
            host=job.server.host,
            port=job.server.port,
            username=job.server.username,
            ssh_key_path=job.server.ssh_key_path,
            ssh_key_content=job.server.ssh_key_content
        )
        
        if not ssh.connect():
            raise ConnectionError(f"Failed to connect to server {job.server.name}")
        
        # Initialize PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Execute backup
        success, log_output = pg_manager.execute_backup(job.database.name, job.backup_type)
        
        # Update backup log
        backup_log.status = 'success' if success else 'failed'
        backup_log.end_time = datetime.utcnow()
        backup_log.log_output = log_output
        
        # Disconnect from server
        ssh.disconnect()
        
        db.session.commit()
        return success, log_output
        
    except Exception as e:
        logger.exception(f"Error executing manual backup job {job.name}: {str(e)}")
        
        # Update backup log on error
        backup_log.status = 'failed'
        backup_log.end_time = datetime.utcnow()
        backup_log.log_output = str(e)
        db.session.commit()
        
        return False, str(e) 