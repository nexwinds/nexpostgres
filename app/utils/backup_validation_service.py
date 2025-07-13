from app.models.database import BackupJob, BackupLog, PostgresDatabase, S3Storage
from datetime import datetime
import re


class BackupValidationService:
    """Service class for backup-related validations."""
    
    @staticmethod
    def validate_required_fields(fields_dict):
        """Validate that required fields are present and not empty.
        
        Args:
            fields_dict: Dictionary of field_name: value pairs
            
        Returns:
            tuple: (is_valid, error_message)
        """
        missing_fields = []
        
        for field_name, value in fields_dict.items():
            if not value or (isinstance(value, str) and not value.strip()):
                missing_fields.append(field_name.replace('_', ' ').title())
        
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
        
        return True, "All required fields provided"
    
    @staticmethod
    def validate_backup_job_name(name):
        """Validate backup job name.
        
        Args:
            name: Backup job name
            
        Returns:
            tuple: (is_valid, error_message)
        """
        if not name or not name.strip():
            return False, "Backup job name is required"
        
        name = name.strip()
        
        if len(name) < 3:
            return False, "Backup job name must be at least 3 characters long"
        
        if len(name) > 100:
            return False, "Backup job name must be less than 100 characters"
        
        # Allow alphanumeric, spaces, hyphens, and underscores
        if not re.match(r'^[a-zA-Z0-9\s\-_]+$', name):
            return False, "Backup job name can only contain letters, numbers, spaces, hyphens, and underscores"
        
        return True, "Backup job name is valid"
    
    @staticmethod
    def validate_backup_type(backup_type):
        """Validate backup type.
        
        Args:
            backup_type: Type of backup
            
        Returns:
            tuple: (is_valid, error_message)
        """
        valid_types = ['full', 'incr']
        
        if backup_type not in valid_types:
            return False, f"Invalid backup type. Must be one of: {', '.join(valid_types)}"
        
        return True, "Backup type is valid"
    
    @staticmethod
    def validate_cron_expression(cron_expression):
        """Validate cron expression format.
        
        Args:
            cron_expression: Cron schedule expression
            
        Returns:
            tuple: (is_valid, error_message)
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
        if not BackupValidationService._validate_cron_field(minute, 0, 59):
            return False, "Invalid minute in cron expression (must be 0-59 or * or */n)"
        
        # Validate hour (0-23)
        if not BackupValidationService._validate_cron_field(hour, 0, 23):
            return False, "Invalid hour in cron expression (must be 0-23 or * or */n)"
        
        # Validate day (1-31)
        if not BackupValidationService._validate_cron_field(day, 1, 31):
            return False, "Invalid day in cron expression (must be 1-31 or * or */n)"
        
        # Validate month (1-12)
        if not BackupValidationService._validate_cron_field(month, 1, 12):
            return False, "Invalid month in cron expression (must be 1-12 or * or */n)"
        
        # Validate weekday (0-7, where 0 and 7 are Sunday)
        if not BackupValidationService._validate_cron_field(weekday, 0, 7):
            return False, "Invalid weekday in cron expression (must be 0-7 or * or */n)"
        
        return True, "Cron expression is valid"
    
    @staticmethod
    def _validate_cron_field(field, min_val, max_val):
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
    def validate_retention_count(retention_count):
        """Validate retention count.
        
        Args:
            retention_count: Number of backups to retain
            
        Returns:
            tuple: (is_valid, error_message)
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
        
        return True, "Retention count is valid"
    
    @staticmethod
    def validate_database_exists(database_id):
        """Validate that database exists.
        
        Args:
            database_id: Database ID to check
            
        Returns:
            tuple: (is_valid, error_message, database_object)
        """
        if not database_id:
            return False, "Database ID is required", None
        
        try:
            database_id = int(database_id)
        except (ValueError, TypeError):
            return False, "Invalid database ID", None
        
        database = PostgresDatabase.query.get(database_id)
        if not database:
            return False, "Selected database does not exist", None
        
        return True, "Database exists", database
    
    @staticmethod
    def validate_s3_storage_exists(s3_storage_id):
        """Validate that S3 storage configuration exists.
        
        Args:
            s3_storage_id: S3 storage ID to check
            
        Returns:
            tuple: (is_valid, error_message, s3_storage_object)
        """
        if not s3_storage_id:
            return False, "S3 storage ID is required", None
        
        try:
            s3_storage_id = int(s3_storage_id)
        except (ValueError, TypeError):
            return False, "Invalid S3 storage ID", None
        
        s3_storage = S3Storage.query.get(s3_storage_id)
        if not s3_storage:
            return False, "Selected S3 storage configuration does not exist", None
        
        return True, "S3 storage configuration exists", s3_storage
    
    @staticmethod
    def validate_backup_job_exists(backup_job_id):
        """Validate that backup job exists.
        
        Args:
            backup_job_id: Backup job ID to check
            
        Returns:
            tuple: (is_valid, error_message, backup_job_object)
        """
        if not backup_job_id:
            return False, "Backup job ID is required", None
        
        try:
            backup_job_id = int(backup_job_id)
        except (ValueError, TypeError):
            return False, "Invalid backup job ID", None
        
        backup_job = BackupJob.query.get(backup_job_id)
        if not backup_job:
            return False, "Selected backup job does not exist", None
        
        return True, "Backup job exists", backup_job
    
    @staticmethod
    def validate_backup_log_exists(backup_log_id):
        """Validate that backup log exists.
        
        Args:
            backup_log_id: Backup log ID to check
            
        Returns:
            tuple: (is_valid, error_message, backup_log_object)
        """
        if not backup_log_id:
            return False, "Backup log ID is required", None
        
        try:
            backup_log_id = int(backup_log_id)
        except (ValueError, TypeError):
            return False, "Invalid backup log ID", None
        
        backup_log = BackupLog.query.get(backup_log_id)
        if not backup_log:
            return False, "Selected backup log does not exist", None
        
        return True, "Backup log exists", backup_log
    
    @staticmethod
    def validate_recovery_time(recovery_time):
        """Validate recovery time format.
        
        Args:
            recovery_time: Recovery time string
            
        Returns:
            tuple: (is_valid, error_message, parsed_datetime)
        """
        if not recovery_time or not recovery_time.strip():
            return False, "Recovery time is required", None
        
        recovery_time = recovery_time.strip()
        
        try:
            # Try to parse as ISO format
            parsed_time = datetime.fromisoformat(recovery_time)
            
            # Check if time is not in the future
            if parsed_time > datetime.utcnow():
                return False, "Recovery time cannot be in the future", None
            
            # Check if time is not too old (more than 1 year)
            one_year_ago = datetime.utcnow().replace(year=datetime.utcnow().year - 1)
            if parsed_time < one_year_ago:
                return False, "Recovery time cannot be more than 1 year ago", None
            
            return True, "Recovery time is valid", parsed_time
            
        except ValueError:
            return False, "Invalid recovery time format. Use YYYY-MM-DDTHH:MM:SS format", None
    
    @staticmethod
    def validate_s3_credentials(bucket, region, access_key, secret_key):
        """Validate S3 credentials format.
        
        Args:
            bucket: S3 bucket name
            region: AWS region
            access_key: AWS access key
            secret_key: AWS secret key
            
        Returns:
            tuple: (is_valid, error_message)
        """
        # Validate bucket name
        if not bucket or not bucket.strip():
            return False, "S3 bucket name is required"
        
        bucket = bucket.strip()
        if len(bucket) < 3 or len(bucket) > 63:
            return False, "S3 bucket name must be between 3 and 63 characters"
        
        if not re.match(r'^[a-z0-9][a-z0-9\-]*[a-z0-9]$', bucket):
            return False, "S3 bucket name must contain only lowercase letters, numbers, and hyphens"
        
        # Validate region
        if not region or not region.strip():
            return False, "AWS region is required"
        
        region = region.strip()
        # Basic AWS region format validation
        if not re.match(r'^[a-z0-9\-]+$', region):
            return False, "Invalid AWS region format"
        
        # Validate access key
        if not access_key or not access_key.strip():
            return False, "AWS access key is required"
        
        access_key = access_key.strip()
        if len(access_key) < 16 or len(access_key) > 32:
            return False, "AWS access key must be between 16 and 32 characters"
        
        if not re.match(r'^[A-Z0-9]+$', access_key):
            return False, "AWS access key must contain only uppercase letters and numbers"
        
        # Validate secret key
        if not secret_key or not secret_key.strip():
            return False, "AWS secret key is required"
        
        secret_key = secret_key.strip()
        if len(secret_key) < 40:
            return False, "AWS secret key must be at least 40 characters"
        
        return True, "S3 credentials format is valid"
    
    @staticmethod
    def validate_backup_form_data(form_data):
        """Validate complete backup job form data.
        
        Args:
            form_data: Dictionary containing form data
            
        Returns:
            tuple: (is_valid, errors_list, validated_data)
        """
        errors = []
        validated_data = {}
        
        # Extract form data
        name = form_data.get('name', '').strip()
        database_id = form_data.get('database_id')
        backup_type = form_data.get('backup_type')
        cron_expression = form_data.get('cron_expression', '').strip()
        s3_storage_id = form_data.get('s3_storage_id')
        retention_count = form_data.get('retention_count')
        
        # Validate name
        is_valid, error = BackupValidationService.validate_backup_job_name(name)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['name'] = name
        
        # Validate database
        is_valid, error, database = BackupValidationService.validate_database_exists(database_id)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['database_id'] = database_id
            validated_data['database'] = database
        
        # Validate backup type
        is_valid, error = BackupValidationService.validate_backup_type(backup_type)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['backup_type'] = backup_type
        
        # Validate cron expression
        is_valid, error = BackupValidationService.validate_cron_expression(cron_expression)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['cron_expression'] = cron_expression
        
        # Validate S3 storage
        is_valid, error, s3_storage = BackupValidationService.validate_s3_storage_exists(s3_storage_id)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['s3_storage_id'] = s3_storage_id
            validated_data['s3_storage'] = s3_storage
        
        # Validate retention count
        is_valid, error = BackupValidationService.validate_retention_count(retention_count)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['retention_count'] = int(retention_count)
        
        return len(errors) == 0, errors, validated_data
    
    @staticmethod
    def validate_restore_form_data(form_data):
        """Validate complete restore form data.
        
        Args:
            form_data: Dictionary containing form data
            
        Returns:
            tuple: (is_valid, errors_list, validated_data)
        """
        errors = []
        validated_data = {}
        
        # Extract form data
        backup_job_id = form_data.get('backup_job_id')
        database_id = form_data.get('database_id')
        backup_log_id = form_data.get('backup_log_id')
        recovery_time = form_data.get('recovery_time', '').strip()
        restore_to_same = form_data.get('restore_to_same') == 'true'
        use_recovery_time = form_data.get('use_recovery_time') == 'true'
        target_database_id = form_data.get('target_database_id')
        
        # Validate backup job
        is_valid, error, backup_job = BackupValidationService.validate_backup_job_exists(backup_job_id)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['backup_job_id'] = backup_job_id
            validated_data['backup_job'] = backup_job
        
        # Determine target database
        if not restore_to_same and target_database_id:
            database_id = target_database_id
        elif not database_id and backup_job:
            database_id = backup_job.database_id
        
        # Validate target database
        is_valid, error, database = BackupValidationService.validate_database_exists(database_id)
        if not is_valid:
            errors.append(error)
        else:
            validated_data['database_id'] = database_id
            validated_data['database'] = database
        
        # Validate backup log if provided
        if backup_log_id:
            is_valid, error, backup_log = BackupValidationService.validate_backup_log_exists(backup_log_id)
            if not is_valid:
                errors.append(error)
            else:
                validated_data['backup_log_id'] = backup_log_id
                validated_data['backup_log'] = backup_log
        
        # Validate recovery time if using point-in-time recovery
        if use_recovery_time:
            if recovery_time:
                is_valid, error, parsed_time = BackupValidationService.validate_recovery_time(recovery_time)
                if not is_valid:
                    errors.append(error)
                else:
                    validated_data['recovery_time'] = recovery_time
                    validated_data['parsed_recovery_time'] = parsed_time
            else:
                errors.append("Recovery time is required for point-in-time recovery")
        
        validated_data['restore_to_same'] = restore_to_same
        validated_data['use_recovery_time'] = use_recovery_time
        
        return len(errors) == 0, errors, validated_data
    
    @staticmethod
    def flash_validation_errors(errors):
        """Flash validation errors to user.
        
        Args:
            errors: List of error messages
        """
        from flask import flash
        
        for error in errors:
            flash(error, 'danger')