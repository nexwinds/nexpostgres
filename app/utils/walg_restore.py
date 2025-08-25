import logging
from typing import Dict, Tuple
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager

logger = logging.getLogger(__name__)

class WalgRestore:
    """Simplified WAL-G restore utility following official documentation."""
    
    @staticmethod
    def execute_restore(ssh_manager: SSHManager, walg_env: Dict[str, str], backup_name: str) -> Tuple[bool, str]:
        """Execute WAL-G backup-fetch according to documentation.
        
        Args:
            ssh_manager: SSH connection to target server
            walg_env: WAL-G environment variables
            backup_name: Name of backup to restore (or LATEST)
            
        Returns:
            Tuple of (success, message)
        """
        try:
            pgdata = walg_env.get('PGDATA', '/var/lib/postgresql/data')
            
            # Normalize backup name
            fetch_name = 'LATEST' if backup_name in ['files_metadata.json', 'LATEST'] else backup_name
            
            # Build WAL-G environment string
            env_str = ' '.join([f'{k}={v}' for k, v in walg_env.items()])
            
            # Execute WAL-G backup-fetch command
            restore_cmd = f'{env_str} wal-g backup-fetch {pgdata} {fetch_name}'
            logger.info(f"Executing WAL-G restore: {restore_cmd}")
            
            result = ssh_manager.execute_command(restore_cmd)
            
            if result['exit_code'] != 0:
                error_msg = result.get('stderr', '').strip() or result.get('stdout', '').strip() or 'Unknown error'
                return False, f"WAL-G backup-fetch failed: {error_msg}"
            
            return True, f"WAL-G backup-fetch completed successfully for {fetch_name}"
            
        except Exception as e:
            return False, f"WAL-G restore error: {str(e)}"
    
    @staticmethod
    def start_postgres_with_recovery(ssh_manager: SSHManager, pg_manager: PostgresManager) -> Tuple[bool, str]:
        """Start PostgreSQL service to trigger WAL replay.
        
        Args:
            ssh_manager: SSH connection to target server
            pg_manager: PostgreSQL manager
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Start PostgreSQL service - WAL replay happens automatically
            success, message = pg_manager.start_service()
            if not success:
                return False, f"Failed to start PostgreSQL: {message}"
            
            # Wait a moment for service to stabilize
            import time
            time.sleep(2)
            
            # Verify PostgreSQL is running
            status_result = ssh_manager.execute_command('sudo systemctl is-active postgresql')
            if status_result['exit_code'] != 0:
                return False, "PostgreSQL service failed to start properly"
            
            return True, "PostgreSQL started successfully - WAL replay in progress"
            
        except Exception as e:
            return False, f"PostgreSQL startup error: {str(e)}"
    
    @staticmethod
    def verify_recovery_completion(ssh_manager: SSHManager, database_name: str) -> Tuple[bool, str]:
        """Verify that recovery completed successfully.
        
        Args:
            ssh_manager: SSH connection to target server
            database_name: Name of database to verify
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Check if PostgreSQL is accepting connections
            conn_test = ssh_manager.execute_command('sudo -u postgres psql -c "SELECT 1;" -d postgres')
            if conn_test['exit_code'] != 0:
                return False, "PostgreSQL is not accepting connections"
            
            # Check if target database exists (for database-specific restores)
            if database_name and database_name != 'postgres':
                db_check = ssh_manager.execute_command(f'sudo -u postgres psql -lqt | grep -qw {database_name}')
                if db_check['exit_code'] != 0:
                    return False, f"Database {database_name} not found after restore"
            
            # Check recovery status
            recovery_check = ssh_manager.execute_command('sudo -u postgres psql -c "SELECT pg_is_in_recovery();" -d postgres -t')
            if recovery_check['exit_code'] == 0:
                is_recovering = recovery_check['stdout'].strip()
                if 'f' in is_recovering.lower():
                    return True, "Recovery completed successfully - database is operational"
                else:
                    return True, "Recovery in progress - WAL replay continuing"
            
            return True, "Recovery verification completed"
            
        except Exception as e:
            return False, f"Recovery verification error: {str(e)}"