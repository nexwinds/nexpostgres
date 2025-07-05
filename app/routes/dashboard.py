from flask import Blueprint, render_template
from app.models.database import VpsServer, PostgresDatabase, BackupJob, BackupLog, db
from app.routes.auth import login_required, first_login_required
from sqlalchemy import func, text
from collections import defaultdict
from datetime import datetime, timedelta

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
@login_required
@first_login_required
def index():
    # Count servers, databases, and backup jobs
    servers_count = VpsServer.query.count()
    databases_count = PostgresDatabase.query.count()
    backup_jobs_count = BackupJob.query.count()
    
    # Get recent backup logs
    recent_logs = BackupLog.query.order_by(BackupLog.start_time.desc()).limit(10).all()
    
    # Get backup status summary
    successful_count = BackupLog.query.filter_by(status='success').count()
    failed_count = BackupLog.query.filter_by(status='failed').count()
    in_progress_count = BackupLog.query.filter_by(status='in_progress').count()
    
    # Calculate backup statistics for last 7 days
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    
    # Use raw SQL for complex aggregations with SQLite
    daily_stats_query = text("""
        SELECT 
            date(start_time) AS date, 
            COUNT(id) AS total,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM backup_log
        WHERE start_time >= :seven_days_ago
        GROUP BY date(start_time)
    """)
    
    daily_stats = db.session.execute(
        daily_stats_query, 
        {"seven_days_ago": seven_days_ago}
    ).all()
    
    # Format for chart display
    dates = []
    successful = []
    failed = []
    
    # Generate a list of the last 7 days
    date_list = [(datetime.utcnow() - timedelta(days=x)).date() for x in range(6, -1, -1)]
    
    # Create a lookup dictionary for our db results
    stats_dict = {stat.date: (stat.successful, stat.failed) for stat in daily_stats}
    
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
        servers_count=servers_count,
        databases_count=databases_count,
        backup_jobs_count=backup_jobs_count,
        successful_count=successful_count,
        failed_count=failed_count,
        in_progress_count=in_progress_count,
        recent_logs=recent_logs,
        dates=dates,
        successful_data=successful,
        failed_data=failed,
        failed_jobs=failed_jobs
    ) 