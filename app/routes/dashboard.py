from flask import Blueprint, render_template
from flask_login import login_required
from app.models.database import VpsServer, PostgresDatabase, BackupJob
from app.routes.auth import first_login_required
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
    
    # Get backup statistics from WAL-G/S3
    from app.utils.backup_metadata_service import BackupMetadataService
    backup_stats = BackupMetadataService.get_overall_backup_statistics()
    
    counts = {
        'servers': user_servers.count(),
        'databases': user_databases.count(),
        'backup_jobs': user_backup_jobs.count(),
        'successful': backup_stats['successful'],
        'failed': backup_stats['failed'],
        'in_progress': backup_stats['in_progress']
    }
    
    # Get recent backup logs
    recent_logs = BackupMetadataService.get_recent_backup_logs(limit=10)
    
    # Calculate backup statistics for last 7 days using BackupMetadataService
    
    # Get backup statistics from S3/WAL-G instead of database
    daily_stats = BackupMetadataService.get_backup_statistics(days=7)
    
    # Prepare chart data
    stats_dict = {stat['date']: (stat['successful'], stat['failed']) for stat in daily_stats}
    
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
    failed_jobs = BackupMetadataService.get_failed_backup_jobs()
    
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