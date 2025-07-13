"""Validation service for database operations.

This module provides validation utilities for database-related operations,
reducing code duplication and ensuring consistent validation logic.
"""

import re
from typing import List, Tuple, Optional
from flask import flash


class ValidationService:
    """Service class for validation operations."""
    
    # Common validation patterns
    USERNAME_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]{2,62}$')
    DATABASE_NAME_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]{2,62}$')
    
    @staticmethod
    def validate_required_fields(data: dict, required_fields: List[str]) -> List[str]:
        """Validate that required fields are present and not empty.
        
        Args:
            data: Dictionary containing form data
            required_fields: List of required field names
            
        Returns:
            List of error messages
        """
        errors = []
        for field in required_fields:
            if not data.get(field) or not data[field].strip():
                errors.append(f'{field.replace("_", " ").title()} is required')
        return errors
    
    @staticmethod
    def validate_database_name(name: str) -> Tuple[bool, Optional[str]]:
        """Validate database name format.
        
        Args:
            name: Database name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not name:
            return False, 'Database name is required'
        
        if len(name) < 3 or len(name) > 63:
            return False, 'Database name must be between 3 and 63 characters'
        
        if not ValidationService.DATABASE_NAME_PATTERN.match(name):
            return False, 'Database name must start with a letter and contain only letters, numbers, and underscores'
        
        # Check for PostgreSQL reserved words (basic list)
        reserved_words = {
            'postgres', 'template0', 'template1', 'user', 'public', 'information_schema',
            'pg_catalog', 'pg_toast', 'pg_temp', 'pg_toast_temp'
        }
        if name.lower() in reserved_words:
            return False, f'Database name "{name}" is reserved and cannot be used'
        
        return True, None
    
    @staticmethod
    def validate_username(username: str) -> Tuple[bool, Optional[str]]:
        """Validate username format.
        
        Args:
            username: Username to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not username:
            return False, 'Username is required'
        
        if len(username) < 3 or len(username) > 63:
            return False, 'Username must be between 3 and 63 characters'
        
        if not ValidationService.USERNAME_PATTERN.match(username):
            return False, 'Username must start with a letter and contain only letters, numbers, and underscores'
        
        # Check for PostgreSQL reserved usernames
        reserved_usernames = {
            'postgres', 'root', 'admin', 'administrator', 'sa', 'dba', 'superuser',
            'pg_monitor', 'pg_read_all_settings', 'pg_read_all_stats', 'pg_stat_scan_tables',
            'pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program'
        }
        if username.lower() in reserved_usernames:
            return False, f'Username "{username}" is reserved and cannot be used'
        
        return True, None
    
    @staticmethod
    def validate_password(password: str, min_length: int = 8) -> Tuple[bool, Optional[str]]:
        """Validate password strength.
        
        Args:
            password: Password to validate
            min_length: Minimum password length
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not password:
            return False, 'Password is required'
        
        if len(password) < min_length:
            return False, f'Password must be at least {min_length} characters long'
        
        # Check for basic password requirements
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        
        if not (has_upper and has_lower and has_digit):
            return False, 'Password must contain at least one uppercase letter, one lowercase letter, and one digit'
        
        return True, None
    
    @staticmethod
    def validate_permission_level(permission_level: str) -> Tuple[bool, Optional[str]]:
        """Validate permission level.
        
        Args:
            permission_level: Permission level to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        valid_permissions = {'read_only', 'read_write', 'admin'}
        
        if not permission_level:
            return False, 'Permission level is required'
        
        if permission_level not in valid_permissions:
            return False, f'Invalid permission level. Must be one of: {", ".join(valid_permissions)}'
        
        return True, None
    
    @staticmethod
    def validate_connection_string(connection_string: str) -> Tuple[bool, Optional[str]]:
        """Validate PostgreSQL connection string format.
        
        Args:
            connection_string: Connection string to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not connection_string:
            return False, 'Connection string is required'
        
        # Basic PostgreSQL URL format validation
        if not connection_string.startswith('postgresql://'):
            return False, 'Connection string must start with "postgresql://"'
        
        # Check for basic structure: postgresql://user:pass@host:port/database
        pattern = r'^postgresql://[^:]+:[^@]+@[^:/]+:\d+/[^/]+$'
        if not re.match(pattern, connection_string):
            return False, 'Invalid connection string format. Expected: postgresql://user:pass@host:port/database'
        
        return True, None
    
    @staticmethod
    def validate_and_flash_errors(validation_results: List[Tuple[bool, Optional[str]]]) -> bool:
        """Validate multiple fields and flash error messages.
        
        Args:
            validation_results: List of (is_valid, error_message) tuples
            
        Returns:
            True if all validations passed, False otherwise
        """
        all_valid = True
        
        for is_valid, error_message in validation_results:
            if not is_valid and error_message:
                flash(error_message, 'error')
                all_valid = False
        
        return all_valid
    
    @staticmethod
    def generate_username(base_name: str, existing_usernames: List[str]) -> str:
        """Generate a unique username based on database name.
        
        Args:
            base_name: Base name (usually database name)
            existing_usernames: List of existing usernames to avoid
            
        Returns:
            Generated unique username
        """
        # Clean base name
        clean_base = re.sub(r'[^a-zA-Z0-9_]', '', base_name.lower())
        if not clean_base or not clean_base[0].isalpha():
            clean_base = 'user_' + clean_base
        
        # Ensure it starts with a letter
        if not clean_base[0].isalpha():
            clean_base = 'u' + clean_base
        
        # Truncate if too long
        if len(clean_base) > 60:  # Leave room for suffix
            clean_base = clean_base[:60]
        
        # Check if base name is available
        if clean_base not in existing_usernames:
            return clean_base
        
        # Generate with suffix
        counter = 1
        while True:
            candidate = f"{clean_base}_{counter}"
            if len(candidate) > 63:
                # Truncate base name to make room for suffix
                max_base_len = 63 - len(f"_{counter}")
                candidate = f"{clean_base[:max_base_len]}_{counter}"
            
            if candidate not in existing_usernames:
                return candidate
            
            counter += 1
            if counter > 999:  # Safety break
                break
        
        # Fallback
        import time
        return f"user_{int(time.time())}"