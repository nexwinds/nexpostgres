from flask import Blueprint, render_template
from app.models.database import VpsServer, PostgresDatabase, BackupJob, BackupLog, db
from app.routes.auth import login_required, first_login_required
from sqlalchemy import func, text
from datetime import datetime, timedelta

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
@login_required
@first_login_required
def index():
    # Get basic stats
    counts = {
        'servers': VpsServer.query.count(),
        'databases': PostgresDatabase.query.count(),
        'backup_jobs': BackupJob.query.count(),
        'successful': BackupLog.query.filter_by(status='success').count(),
        'failed': BackupLog.query.filter_by(status='failed').count(),
        'in_progress': BackupLog.query.filter_by(status='in_progress').count()
    }
    
    # Get recent backup logs
    recent_logs = BackupLog.query.order_by(BackupLog.start_time.desc()).limit(10).all()
    
    # Calculate backup statistics for last 7 days
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    
    # Use raw SQL for complex aggregations with SQLite
    daily_stats = db.session.execute(
        text("""
            SELECT 
                date(start_time) AS date, 
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM backup_log
            WHERE start_time >= :seven_days_ago
            GROUP BY date(start_time)
        """), 
        {"seven_days_ago": seven_days_ago}
    ).all()
    
    # Prepare chart data
    stats_dict = {stat.date: (stat.successful, stat.failed) for stat in daily_stats}
    date_list = [(datetime.utcnow() - timedelta(days=x)).date() for x in range(6, -1, -1)]
    
    dates = []
    successful = []
    failed = []
    
    for date in date_list:
        dates.append(date.strftime('%Y-%m-%d'))
        if date in stats_dict:
            successful.append(stats_dict[date][0] or 0)
            failed.append(stats_dict[date][1] or 0)
        else:
            successful.append(0)
            failed.append(0)
    
    # Get backup jobs that need attention
    failed_jobs = BackupJob.query.join(BackupLog).filter(
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