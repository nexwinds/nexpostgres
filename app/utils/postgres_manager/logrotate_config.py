"""Log rotation configuration for WAL-G logs.

This module provides utilities to set up log rotation for WAL-G
to prevent log files from growing indefinitely.
"""

import logging
from typing import Tuple


class LogRotateManager:
    """Manages log rotation configuration for WAL-G."""
    
    def __init__(self, ssh_manager, logger=None):
        """Initialize the log rotation manager.
        
        Args:
            ssh_manager: SSH connection manager
            logger: Logger instance
        """
        self.ssh = ssh_manager
        self.logger = logger or logging.getLogger(__name__)
    
    def setup_walg_logrotate(self) -> Tuple[bool, str]:
        """Set up logrotate configuration for WAL-G logs.
        
        Returns:
            tuple: (success, message)
        """
        try:
            # Create logrotate configuration for WAL-G
            logrotate_config = self._generate_logrotate_config()
            
            # Write logrotate configuration file
            config_path = '/etc/logrotate.d/walg'
            
            # Create a temporary file with the configuration content
            temp_path = '/tmp/walg_logrotate.conf'
            if not self.ssh.write_file_content(temp_path, logrotate_config):
                return False, "Failed to write temporary logrotate configuration file"
            
            # Move the temporary file to the final location with proper permissions
            move_cmd = f'sudo mv {temp_path} {config_path} && sudo chmod 644 {config_path}'
            result = self.ssh.execute_command(move_cmd)
            
            if result['exit_code'] != 0:
                return False, f"Failed to create logrotate configuration: {result.get('stderr', 'Unknown error')}"
            
            # Test logrotate configuration
            test_cmd = f'sudo logrotate -d {config_path}'
            test_result = self.ssh.execute_command(test_cmd)
            
            if test_result['exit_code'] != 0:
                self.logger.warning(f"Logrotate configuration test failed: {test_result.get('stderr', '')}")
                return True, "Logrotate configuration created but test failed - please verify manually"
            
            self.logger.info("WAL-G logrotate configuration set up successfully")
            return True, "Log rotation configured successfully for WAL-G"
            
        except Exception as e:
            error_msg = f"Error setting up log rotation: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def _generate_logrotate_config(self) -> str:
        """Generate logrotate configuration content for WAL-G.
        
        Returns:
            str: Logrotate configuration content
        """
        config = '''
# WAL-G log rotation configuration
# Prevents log files from growing indefinitely
/var/log/walg/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 640 postgres postgres
    sharedscripts
    postrotate
        # Send HUP signal to WAL-G processes if running
        /usr/bin/pkill -HUP wal-g 2>/dev/null || true
    endscript
}

# WAL-G error logs
/var/log/walg/*.err {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 640 postgres postgres
    sharedscripts
}
'''
        return config.strip()
    
    def check_log_directory(self) -> Tuple[bool, str]:
        """Check and create WAL-G log directory if needed.
        
        Returns:
            tuple: (success, message)
        """
        try:
            log_dir = '/var/log/walg'
            
            # Check if directory exists
            check_cmd = f'test -d {log_dir}'
            result = self.ssh.execute_command(check_cmd)
            
            if result['exit_code'] == 0:
                return True, f"Log directory {log_dir} already exists"
            
            # Create directory with proper permissions
            create_cmd = f'sudo mkdir -p {log_dir} && sudo chown postgres:postgres {log_dir} && sudo chmod 755 {log_dir}'
            create_result = self.ssh.execute_command(create_cmd)
            
            if create_result['exit_code'] != 0:
                return False, f"Failed to create log directory: {create_result.get('stderr', 'Unknown error')}"
            
            return True, f"Log directory {log_dir} created successfully"
            
        except Exception as e:
            error_msg = f"Error checking/creating log directory: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def get_log_status(self) -> Tuple[bool, dict]:
        """Get current status of WAL-G logs.
        
        Returns:
            tuple: (success, log_status_dict)
        """
        try:
            log_dir = '/var/log/walg'
            
            # Get log file sizes and counts
            size_cmd = f'find {log_dir} -name "*.log" -exec ls -lh {{}} \; 2>/dev/null | wc -l'
            size_result = self.ssh.execute_command(size_cmd)
            
            total_size_cmd = f'du -sh {log_dir} 2>/dev/null || echo "0K {log_dir}"'
            total_result = self.ssh.execute_command(total_size_cmd)
            
            status = {
                'log_count': int(size_result.get('stdout', '0').strip()) if size_result['exit_code'] == 0 else 0,
                'total_size': total_result.get('stdout', '0K').strip().split()[0] if total_result['exit_code'] == 0 else '0K',
                'directory': log_dir
            }
            
            return True, status
            
        except Exception as e:
            self.logger.error(f"Error getting log status: {str(e)}")
            return False, {}