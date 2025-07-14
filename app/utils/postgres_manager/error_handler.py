"""Error handling utilities for PostgreSQL management operations."""

import logging
from typing import Dict, Tuple, Optional
from .constants import PostgresConstants

class PostgresErrorHandler:
    """Centralized error handling for PostgreSQL operations."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
    
    def handle_command_failure(self, command: str, result: Dict, 
                             context: str = "") -> Tuple[bool, str]:
        """Handle command execution failures with consistent logging.
        
        Args:
            command: The command that failed
            result: Command execution result
            context: Additional context for the error
            
        Returns:
            tuple: (False, error_message)
        """
        error_msg = result.get('stderr', result.get('stdout', 'Unknown error'))
        full_context = f"{context}: " if context else ""
        
        self.logger.error(f"{full_context}Command failed: {command}")
        self.logger.error(f"Error details: {error_msg}")
        
        return False, f"{full_context}Command execution failed: {error_msg}"
    
    def handle_service_failure(self, service_name: str, operation: str, 
                             result: Dict) -> Tuple[bool, str]:
        """Handle service operation failures.
        
        Args:
            service_name: Name of the service
            operation: Operation that failed (start, stop, restart)
            result: Command execution result
            
        Returns:
            tuple: (False, error_message)
        """
        error_msg = result.get('stderr', 'Unknown error')
        message = f"Failed to {operation} {service_name}: {error_msg}"
        
        self.logger.error(message)
        return False, message
    
    def handle_installation_failure(self, package: str, version: str = None, 
                                  result: Dict = None) -> Tuple[bool, str]:
        """Handle package installation failures.
        
        Args:
            package: Package name that failed to install
            version: Package version (if applicable)
            result: Command execution result
            
        Returns:
            tuple: (False, error_message)
        """
        version_str = f" version {version}" if version else ""
        error_details = ""
        
        if result:
            error_details = f": {result.get('stderr', 'Unknown error')}"
        
        message = f"Failed to install {package}{version_str}{error_details}"
        self.logger.error(message)
        
        return False, message
    
    def handle_backup_failure(self, operation: str, db_name: str, 
                            result: Dict = None) -> Tuple[bool, str]:
        """Handle backup operation failures.
        
        Args:
            operation: Backup operation (backup, restore, check)
            db_name: Database name
            result: Command execution result
            
        Returns:
            tuple: (False, error_message)
        """
        error_details = ""
        if result:
            error_details = f": {result.get('stderr', 'Unknown error')}"
        
        message = f"Backup {operation} failed for database {db_name}{error_details}"
        self.logger.error(message)
        
        return False, message
    
    def handle_config_failure(self, config_file: str, operation: str, 
                            result: Dict = None) -> Tuple[bool, str]:
        """Handle configuration file operation failures.
        
        Args:
            config_file: Configuration file name
            operation: Operation that failed
            result: Command execution result
            
        Returns:
            tuple: (False, error_message)
        """
        error_details = ""
        if result:
            error_details = f": {result.get('stderr', 'Unknown error')}"
        
        message = f"Failed to {operation} {config_file}{error_details}"
        self.logger.error(message)
        
        return False, message
    
    def handle_user_operation_failure(self, operation: str, username: str, 
                                    db_name: str = None, 
                                    result: Dict = None) -> Tuple[bool, str]:
        """Handle user management operation failures.
        
        Args:
            operation: User operation (create, delete, grant, revoke)
            username: Username
            db_name: Database name (if applicable)
            result: Command execution result
            
        Returns:
            tuple: (False, error_message)
        """
        db_context = f" on database {db_name}" if db_name else ""
        error_details = ""
        
        if result:
            error_details = f": {result.get('stderr', 'Unknown error')}"
        
        message = f"Failed to {operation} user {username}{db_context}{error_details}"
        self.logger.error(message)
        
        return False, message
    
    def log_warning_with_context(self, message: str, context: str = "", 
                               result: Dict = None) -> None:
        """Log warning with additional context and command result.
        
        Args:
            message: Warning message
            context: Additional context
            result: Command execution result (optional)
        """
        full_message = f"{context}: {message}" if context else message
        
        if result and result.get('stderr'):
            full_message += f" - {result['stderr']}"
        
        self.logger.warning(full_message)
    
    def log_retry_attempt(self, operation: str, attempt: int, max_attempts: int, 
                         error: str = "") -> None:
        """Log retry attempt information.
        
        Args:
            operation: Operation being retried
            attempt: Current attempt number
            max_attempts: Maximum number of attempts
            error: Error that caused the retry
        """
        message = f"Retrying {operation} (attempt {attempt}/{max_attempts})"
        if error:
            message += f" - Previous error: {error}"
        
        self.logger.warning(message)
    
    def get_standard_error_message(self, error_key: str) -> str:
        """Get standardized error message from constants.
        
        Args:
            error_key: Key from PostgresConstants.ERROR_MESSAGES
            
        Returns:
            str: Error message
        """
        return PostgresConstants.ERROR_MESSAGES.get(
            error_key, 
            f"Unknown error: {error_key}"
        )
    
    def validate_and_log_version_warning(self, version: str) -> None:
        """Validate version and log appropriate warnings.
        
        Args:
            version: PostgreSQL version to validate
        """
        if version not in PostgresConstants.VERSION_SPECIFIC:
            self.logger.warning(f"PostgreSQL version {version} is not officially supported")
            return
        
        version_info = PostgresConstants.VERSION_SPECIFIC[version]
        status = version_info.get('status', 'unknown')
        eol_date = version_info.get('eol_date', 'unknown')
        
        if status == 'EOL':
            recommended = PostgresConstants.SUPPORTED_VERSIONS['recommended']
            self.logger.warning(
                f"PostgreSQL {version} has reached end-of-life on {eol_date}. "
                f"Consider upgrading to version {recommended}."
            )
        elif status == 'deprecated':
            recommended = PostgresConstants.SUPPORTED_VERSIONS['recommended']
            self.logger.warning(
                f"PostgreSQL {version} is deprecated and will reach end-of-life on {eol_date}. "
                f"Consider upgrading to version {recommended}."
            )