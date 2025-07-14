"""Unified validation service for all application operations.

This module provides a comprehensive validation framework that consolidates
validation logic from across the application, reducing code duplication and
ensuring consistent validation patterns.
"""

import re
from typing import List, Tuple, Optional, Dict, Any
from flask import flash
from app.models.database import BackupJob, PostgresDatabase, S3Storage
from datetime import datetime


class UnifiedValidationService:
    """Unified service class for all validation operations."""
    
    # Common validation patterns
    USERNAME_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]{2,62}$')
    DATABASE_NAME_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]{2,62}$')
    BACKUP_JOB_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9\s\-_]+$')
    
    # Reserved words and names
    POSTGRES_RESERVED_WORDS = {
        'postgres', 'template0', 'template1', 'user', 'public', 'information_schema',
        'pg_catalog', 'pg_toast', 'pg_temp', 'pg_toast_temp'
    }
    
    POSTGRES_RESERVED_USERNAMES = {
        'postgres', 'root', 'admin', 'administrator', 'sa', 'dba', 'superuser',
        'pg_monitor', 'pg_read_all_settings', 'pg_read_all_stats', 'pg_stat_scan_tables',
        'pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program'
    }
    
    @staticmethod
    def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> Tuple[bool, List[str]]:
        """Validate that required fields are present and not empty.
        
        Args:
            data: Dictionary containing form data
            required_fields: List of required field names
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        for field in required_fields:
            value = data.get(field)
            if not value or (isinstance(value, str) and not value.strip()):
                errors.append(f'{field.replace("_", " ").title()} is required')
        return len(errors) == 0, errors
    
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
        
        if not UnifiedValidationService.DATABASE_NAME_PATTERN.match(name):
            return False, 'Database name must start with a letter and contain only letters, numbers, and underscores'
        
        if name.lower() in UnifiedValidationService.POSTGRES_RESERVED_WORDS:
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
        
        if not UnifiedValidationService.USERNAME_PATTERN.match(username):
            return False, 'Username must start with a letter and contain only letters, numbers, and underscores'
        
        if username.lower() in UnifiedValidationService.POSTGRES_RESERVED_USERNAMES:
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
    def validate_backup_job_name(name: str) -> Tuple[bool, Optional[str]]:
        """Validate backup job name.
        
        Args:
            name: Backup job name
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not name or not name.strip():
            return False, "Backup job name is required"
        
        name = name.strip()
        
        if len(name) < 3:
            return False, "Backup job name must be at least 3 characters long"
        
        if len(name) > 100:
            return False, "Backup job name must be less than 100 characters"
        
        if not UnifiedValidationService.BACKUP_JOB_NAME_PATTERN.match(name):
            return False, "Backup job name can only contain letters, numbers, spaces, hyphens, and underscores"
        
        return True, None
    
    @staticmethod
    def validate_backup_type(backup_type: str) -> Tuple[bool, Optional[str]]:
        """Validate backup type.
        
        Args:
            backup_type: Type of backup
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        valid_types = ['full', 'incr']
        
        if backup_type not in valid_types:
            return False, f"Invalid backup type. Must be one of: {', '.join(valid_types)}"
        
        return True, None
    
    @staticmethod
    def validate_cron_expression(cron_expression: str) -> Tuple[bool, Optional[str]]:
        """Validate cron expression format.
        
        Args:
            cron_expression: Cron schedule expression
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not cron_expression or not cron_expression.strip():
            return False, "Cron expression is required"
        
        cron_expression = cron_expression.strip()
        
        # Basic cron validation - should have 5 parts
        parts = cron_expression.split()
        if len(parts) != 5:
            return False, "Cron expression must have exactly 5 parts (minute hour day month weekday)"
        
        # Validate each part
        minute, hour, day, month, weekday = parts
        
        # Validate minute (0-59)
        if not UnifiedValidationService._validate_cron_field(minute, 0, 59):
            return False, "Invalid minute in cron expression (must be 0-59 or * or */n)"
        
        # Validate hour (0-23)
        if not UnifiedValidationService._validate_cron_field(hour, 0, 23):
            return False, "Invalid hour in cron expression (must be 0-23 or * or */n)"
        
        # Validate day (1-31)
        if not UnifiedValidationService._validate_cron_field(day, 1, 31):
            return False, "Invalid day in cron expression (must be 1-31 or * or */n)"
        
        # Validate month (1-12)
        if not UnifiedValidationService._validate_cron_field(month, 1, 12):
            return False, "Invalid month in cron expression (must be 1-12 or * or */n)"
        
        # Validate weekday (0-7, where 0 and 7 are Sunday)
        if not UnifiedValidationService._validate_cron_field(weekday, 0, 7):
            return False, "Invalid weekday in cron expression (must be 0-7 or * or */n)"
        
        return True, None
    
    @staticmethod
    def _validate_cron_field(field: str, min_val: int, max_val: int) -> bool:
        """Validate individual cron field.
        
        Args:
            field: Cron field value
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            bool: True if valid
        """
        # Allow wildcard
        if field == '*':
            return True
        
        # Allow step values (*/n)
        if field.startswith('*/'):
            try:
                step = int(field[2:])
                return step > 0
            except ValueError:
                return False
        
        # Allow ranges (n-m)
        if '-' in field:
            try:
                start, end = field.split('-')
                start_val = int(start)
                end_val = int(end)
                return min_val <= start_val <= max_val and min_val <= end_val <= max_val and start_val <= end_val
            except ValueError:
                return False
        
        # Allow comma-separated values (n,m,o)
        if ',' in field:
            try:
                values = [int(v.strip()) for v in field.split(',')]
                return all(min_val <= v <= max_val for v in values)
            except ValueError:
                return False
        
        # Single value
        try:
            value = int(field)
            return min_val <= value <= max_val
        except ValueError:
            return False
    
    @staticmethod
    def validate_retention_count(retention_count: Any) -> Tuple[bool, Optional[str]]:
        """Validate retention count.
        
        Args:
            retention_count: Number of backups to retain
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if retention_count is None:
            return False, "Retention count is required"
        
        try:
            count = int(retention_count)
        except (ValueError, TypeError):
            return False, "Retention count must be a number"
        
        if count < 1:
            return False, "Retention count must be at least 1"
        
        if count > 365:
            return False, "Retention count cannot exceed 365"
        
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
    def validate_database_exists(database_id: Any) -> Tuple[bool, Optional[str], Optional[PostgresDatabase]]:
        """Validate that database exists.
        
        Args:
            database_id: Database ID to check
            
        Returns:
            Tuple of (is_valid, error_message, database_object)
        """
        if not database_id:
            return False, "Database ID is required", None
        
        try:
            db_id = int(database_id)
        except (ValueError, TypeError):
            return False, "Invalid database ID format", None
        
        database = PostgresDatabase.query.get(db_id)
        if not database:
            return False, "Selected database does not exist", None
        
        return True, None, database
    
    @staticmethod
    def validate_database_exists_by_name(database_name: str, server_id: int) -> bool:
        """Check if a database with the given name already exists on the specified server.
        
        Args:
            database_name: Name of the database to check
            server_id: ID of the server to check on
            
        Returns:
            True if database exists, False otherwise
        """
        existing_database = PostgresDatabase.query.filter_by(
            name=database_name,
            vps_server_id=server_id
        ).first()
        return existing_database is not None
    
    @staticmethod
    def validate_s3_storage_exists(s3_storage_id: Any) -> Tuple[bool, Optional[str], Optional[S3Storage]]:
        """Validate that S3 storage configuration exists.
        
        Args:
            s3_storage_id: S3 storage ID to check
            
        Returns:
            Tuple of (is_valid, error_message, s3_storage_object)
        """
        if not s3_storage_id:
            return False, "S3 storage ID is required", None
        
        try:
            storage_id = int(s3_storage_id)
        except (ValueError, TypeError):
            return False, "Invalid S3 storage ID format", None
        
        s3_storage = S3Storage.query.get(storage_id)
        if not s3_storage:
            return False, "Selected S3 storage configuration does not exist", None
        
        return True, None, s3_storage
    
    @staticmethod
    def validate_backup_job_exists(backup_job_id: Any) -> Tuple[bool, Optional[str], Optional[BackupJob]]:
        """Validate that backup job exists.
        
        Args:
            backup_job_id: Backup job ID to check
            
        Returns:
            Tuple of (is_valid, error_message, backup_job_object)
        """
        if not backup_job_id:
            return False, "Backup job ID is required", None
        
        try:
            job_id = int(backup_job_id)
        except (ValueError, TypeError):
            return False, "Invalid backup job ID format", None
        
        backup_job = BackupJob.query.get(job_id)
        if not backup_job:
            return False, "Selected backup job does not exist", None
        
        return True, None, backup_job
    
    @staticmethod
    def validate_one_to_one_backup_relationship(database_id: int, backup_job_id: Optional[int] = None) -> Tuple[bool, Optional[str]]:
        """Validate one-to-one relationship between database and backup job.
        
        Args:
            database_id: Database ID to check
            backup_job_id: Existing backup job ID (for updates, None for new jobs)
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check if database already has a backup job
        existing_backup_job = BackupJob.query.filter_by(database_id=database_id).first()
        
        if existing_backup_job:
            # If we're updating an existing backup job, allow it
            if backup_job_id and existing_backup_job.id == backup_job_id:
                return True, None
            # Otherwise, this database already has a backup job
            return False, f"Database already has a backup job ('{existing_backup_job.name}'). Each database can have only one backup job."
        
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
    def flash_validation_errors(errors: List[str], category: str = 'danger') -> None:
        """Flash multiple validation error messages.
        
        Args:
            errors: List of error messages
            category: Flash message category
        """
        for error in errors:
            flash(error, category)
    
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
        
        # Ensure it's within length limits
        if len(clean_base) > 60:
            clean_base = clean_base[:60]
        
        # Check if it's unique
        username = clean_base
        counter = 1
        while username in existing_usernames:
            username = f"{clean_base}_{counter}"
            counter += 1
            # Prevent infinite loop
            if counter > 1000:
                break
        
        return username
    
    @classmethod
    def validate_backup_form_data(cls, form_data: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any]]:
        """Comprehensive validation for backup form data.
        
        Args:
            form_data: Form data dictionary
            
        Returns:
            Tuple of (is_valid, errors, validated_data)
        """
        errors = []
        validated_data = {}
        
        # Required fields validation
        required_fields = ['name', 'database_id', 'backup_type', 'cron_expression', 's3_storage_id', 'retention_count']
        is_valid, field_errors = cls.validate_required_fields(form_data, required_fields)
        if not is_valid:
            errors.extend(field_errors)
        
        # Backup job name validation
        if form_data.get('name'):
            is_valid, error = cls.validate_backup_job_name(form_data['name'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['name'] = form_data['name'].strip()
        
        # Backup type validation
        if form_data.get('backup_type'):
            is_valid, error = cls.validate_backup_type(form_data['backup_type'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['backup_type'] = form_data['backup_type']
        
        # Cron expression validation
        if form_data.get('cron_expression'):
            is_valid, error = cls.validate_cron_expression(form_data['cron_expression'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['cron_expression'] = form_data['cron_expression'].strip()
        
        # Retention count validation
        if form_data.get('retention_count'):
            is_valid, error = cls.validate_retention_count(form_data['retention_count'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['retention_count'] = int(form_data['retention_count'])
        
        # Database existence validation
        if form_data.get('database_id'):
            is_valid, error, database = cls.validate_database_exists(form_data['database_id'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['database'] = database
                validated_data['database_id'] = database.id
                
                # Validate one-to-one relationship (for new backup jobs)
                backup_job_id = form_data.get('backup_job_id')  # Will be None for new jobs
                is_valid, error = cls.validate_one_to_one_backup_relationship(database.id, backup_job_id)
                if not is_valid:
                    errors.append(error)
        
        # S3 storage existence validation
        if form_data.get('s3_storage_id'):
            is_valid, error, s3_storage = cls.validate_s3_storage_exists(form_data['s3_storage_id'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['s3_storage'] = s3_storage
                validated_data['s3_storage_id'] = s3_storage.id
        
        # Enabled flag
        validated_data['enabled'] = form_data.get('enabled') == 'true'
        
        return len(errors) == 0, errors, validated_data
    
    @classmethod
    def validate_restore_form_data(cls, form_data: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any]]:
        """Comprehensive validation for restore form data.
        
        Args:
            form_data: Form data dictionary
            
        Returns:
            Tuple of (is_valid, errors, validated_data)
        """
        errors = []
        validated_data = {}
        
        # Required fields validation
        required_fields = ['backup_job_id', 'database_id']
        is_valid, field_errors = cls.validate_required_fields(form_data, required_fields)
        if not is_valid:
            errors.extend(field_errors)
        
        # Backup job existence validation
        if form_data.get('backup_job_id'):
            is_valid, error, backup_job = cls.validate_backup_job_exists(form_data['backup_job_id'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['backup_job'] = backup_job
                validated_data['backup_job_id'] = backup_job.id
        
        # Database existence validation
        if form_data.get('database_id'):
            is_valid, error, database = cls.validate_database_exists(form_data['database_id'])
            if not is_valid:
                errors.append(error)
            else:
                validated_data['database'] = database
                validated_data['database_id'] = database.id
        
        # Optional fields
        validated_data['backup_log_id'] = form_data.get('backup_log_id')
        validated_data['target_database_id'] = form_data.get('target_database_id')
        validated_data['restore_to_same'] = form_data.get('restore_to_same') == 'on'
        validated_data['use_recovery_time'] = form_data.get('use_recovery_time') == 'on'
        validated_data['recovery_time'] = form_data.get('recovery_time')
        validated_data['stop_target_database'] = form_data.get('stop_target_database') == 'on'
        validated_data['create_restore_point'] = form_data.get('create_restore_point') == 'on'
        
        return len(errors) == 0, errors, validated_data