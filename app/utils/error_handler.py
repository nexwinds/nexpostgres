"""Error handling utilities for consistent error management across routes.

This module provides standardized error handling functions to reduce code duplication
and ensure consistent error messaging throughout the application.
"""

from flask import flash, redirect, url_for
from typing import Optional, Any


class ErrorHandler:
    """Utility class for standardized error handling."""
    
    @staticmethod
    def flash_error(message: str, category: str = 'error') -> None:
        """Flash an error message with consistent formatting.
        
        Args:
            message: Error message to display
            category: Flash message category ('error', 'danger', 'warning')
        """
        flash(message, category)
    
    @staticmethod
    def flash_and_redirect(message: str, endpoint: str, category: str = 'error', **kwargs) -> Any:
        """Flash an error message and redirect to specified endpoint.
        
        Args:
            message: Error message to display
            endpoint: Flask endpoint to redirect to
            category: Flash message category
            **kwargs: Additional arguments for url_for
            
        Returns:
            Flask redirect response
        """
        flash(message, category)
        return redirect(url_for(endpoint, **kwargs))
    
    @staticmethod
    def handle_validation_error(field_name: str, error_type: str = 'required') -> str:
        """Generate standardized validation error messages.
        
        Args:
            field_name: Name of the field that failed validation
            error_type: Type of validation error ('required', 'invalid', 'exists')
            
        Returns:
            Formatted error message
        """
        error_messages = {
            'required': f'{field_name} is required',
            'invalid': f'Invalid {field_name} provided',
            'exists': f'{field_name} already exists',
            'not_found': f'{field_name} not found',
            'empty': f'{field_name} cannot be empty'
        }
        
        return error_messages.get(error_type, f'Error with {field_name}')
    
    @staticmethod
    def handle_connection_error(service_name: str = 'service') -> str:
        """Generate standardized connection error messages.
        
        Args:
            service_name: Name of the service that failed to connect
            
        Returns:
            Formatted error message
        """
        return f'Failed to connect to {service_name}. Please check your settings.'
    
    @staticmethod
    def handle_operation_error(operation: str, details: Optional[str] = None) -> str:
        """Generate standardized operation error messages.
        
        Args:
            operation: Name of the operation that failed
            details: Optional additional error details
            
        Returns:
            Formatted error message
        """
        base_message = f'{operation} failed'
        if details:
            return f'{base_message}: {details}'
        return base_message