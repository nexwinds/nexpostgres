from flask import Blueprint, render_template
from flask_login import login_required
from app.models.database import VpsServer, PostgresDatabase, BackupJob, BackupLog, db
from app.routes.auth import first_login_required
from sqlalchemy import func, text
from datetime import datetime, timedelta

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
@login_required
@first_login_required
def index():
    # Get basic stats for current user
    user_servers = VpsServer.query
    user_databases = PostgresDatabase.query.join(VpsServer)
    user_backup_jobs = BackupJob.query.join(VpsServer)
    
    counts = {
        'servers': user_servers.count(),
        'databases': user_databases.count(),
        'backup_jobs': user_backup_jobs.count(),
        'successful': BackupLog.query.join(BackupJob).join(VpsServer).filter(BackupLog.status == 'success').count(),
        'failed': BackupLog.query.join(BackupJob).join(VpsServer).filter(BackupLog.status == 'failed').count(),
        'in_progress': BackupLog.query.join(BackupJob).join(VpsServer).filter(BackupLog.status == 'in_progress').count()
    }
    
    # Get recent backup logs for current user
    recent_logs = BackupLog.query.join(BackupJob).join(VpsServer).order_by(BackupLog.start_time.desc()).limit(10).all()
    
    # Calculate backup statistics for last 7 days
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    
    # Use raw SQL for complex aggregations with SQLite, filtered by user
    daily_stats = db.session.execute(
        text("""
            SELECT 
                date(bl.start_time) AS date, 
                SUM(CASE WHEN bl.status = 'success' THEN 1 ELSE 0 END) AS successful,
                SUM(CASE WHEN bl.status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM backup_log bl
            JOIN backup_job bj ON bl.backup_job_id = bj.id
            JOIN vps_server vs ON bj.vps_server_id = vs.id
            WHERE bl.start_time >= :seven_days_ago
            GROUP BY date(bl.start_time)
            ORDER BY date(bl.start_time)
        """), 
        {"seven_days_ago": seven_days_ago}
    ).all()
    
    # Prepare chart data
    # Convert date strings to date objects for easier comparison
    stats_dict = {datetime.strptime(stat.date, '%Y-%m-%d').date(): (stat.successful, stat.failed) for stat in daily_stats}
    
    # Generate the last 7 days date range
    today = datetime.utcnow().date()
    date_list = [(today - timedelta(days=x)) for x in range(6, -1, -1)]
    
    dates = []
    successful = []
    failed = []
    
    for date in date_list:
        # Format date for display
        dates.append(date.strftime('%Y-%m-%d'))
        
        # Get stats for this date if available
        if date in stats_dict:
            successful.append(stats_dict[date][0] or 0)
            failed.append(stats_dict[date][1] or 0)
        else:
            successful.append(0)
            failed.append(0)
    
    # Get backup jobs that need attention for current user
    failed_jobs = BackupJob.query.join(VpsServer).join(BackupLog).filter(
        BackupLog.status == 'failed',
        BackupLog.id.in_(
            db.session.query(func.max(BackupLog.id)).group_by(BackupLog.backup_job_id)
        )
    ).all()
    
    return render_template(
        'dashboard/index.html',
        servers_count=counts['servers'],
        databases_count=counts['databases'],
        backup_jobs_count=counts['backup_jobs'],
        successful_count=counts['successful'],
        failed_count=counts['failed'],
        in_progress_count=counts['in_progress'],
        recent_logs=recent_logs,
        dates=dates,
        successful_data=successful,
        failed_data=failed,
        failed_jobs=failed_jobs
    )