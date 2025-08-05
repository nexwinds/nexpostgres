import logging
import traceback
from flask import jsonify, render_template, request, current_app
from werkzeug.exceptions import HTTPException
from sqlalchemy.exc import SQLAlchemyError
from paramiko import SSHException

logger = logging.getLogger(__name__)

class ErrorHandler:
    """Centralized error handling middleware for the application."""
    
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize error handlers for the Flask app."""
        app.register_error_handler(Exception, self.handle_generic_exception)
        app.register_error_handler(HTTPException, self.handle_http_exception)
        app.register_error_handler(SQLAlchemyError, self.handle_database_exception)
        app.register_error_handler(SSHException, self.handle_ssh_exception)
        app.register_error_handler(404, self.handle_not_found)
        app.register_error_handler(403, self.handle_forbidden)
        app.register_error_handler(500, self.handle_internal_error)
        
        # Add security headers to all responses
        app.after_request(self.add_security_headers)
    
    def add_security_headers(self, response):
        """Add security headers to all responses."""
        security_headers = current_app.config.get('SECURITY_HEADERS', {})
        for header, value in security_headers.items():
            response.headers[header] = value
        return response
    
    def handle_generic_exception(self, error):
        """Handle generic exceptions."""
        # Check if it's a template error and provide more specific information
        if 'jinja2' in str(type(error)).lower() or 'template' in str(error).lower():
            error_type = 'Template Error'
            error_id = self._log_error(error, error_type)
            
            if request.is_json:
                return jsonify({
                    'error': 'Template rendering error',
                    'error_id': error_id,
                    'message': f'Template error: {str(error)}'
                }), 500
            
            return render_template('errors/500.html', 
                                 error_id=error_id, 
                                 error_details=f'Template Error: {str(error)}'), 500
        
        # Handle other generic exceptions
        error_id = self._log_error(error, 'Generic Exception')
        
        if request.is_json:
            return jsonify({
                'error': 'Internal server error',
                'error_id': error_id,
                'message': 'An unexpected error occurred'
            }), 500
        
        return render_template('errors/500.html', error_id=error_id), 500
    
    def handle_http_exception(self, error):
        """Handle HTTP exceptions."""
        error_id = self._log_error(error, f'HTTP {error.code} Error')
        
        if request.is_json:
            return jsonify({
                'error': error.name,
                'error_id': error_id,
                'message': error.description
            }), error.code
        
        # Try to render specific error template, fallback to generic
        try:
            return render_template(f'errors/{error.code}.html', error=error, error_id=error_id), error.code
        except Exception:
            return render_template('errors/generic.html', error=error, error_id=error_id), error.code
    
    def handle_database_exception(self, error):
        """Handle database-related exceptions."""
        error_id = self._log_error(error, 'Database Error')
        
        if request.is_json:
            return jsonify({
                'error': 'Database error',
                'error_id': error_id,
                'message': 'A database error occurred. Please try again later.'
            }), 500
        
        return render_template('errors/database.html', error_id=error_id), 500
    
    def handle_ssh_exception(self, error):
        """Handle SSH-related exceptions."""
        error_id = self._log_error(error, 'SSH Error')
        
        if request.is_json:
            return jsonify({
                'error': 'SSH connection error',
                'error_id': error_id,
                'message': 'Failed to establish SSH connection. Please check your server configuration.'
            }), 500
        
        return render_template('errors/ssh.html', error_id=error_id), 500
    
    def handle_not_found(self, error):
        """Handle 404 errors."""
        error_id = self._log_error(error, '404 Not Found', level='warning')
        
        if request.is_json:
            return jsonify({
                'error': 'Not found',
                'error_id': error_id,
                'message': 'The requested resource was not found'
            }), 404
        
        return render_template('errors/404.html', error_id=error_id), 404
    
    def handle_forbidden(self, error):
        """Handle 403 errors."""
        error_id = self._log_error(error, '403 Forbidden', level='warning')
        
        if request.is_json:
            return jsonify({
                'error': 'Forbidden',
                'error_id': error_id,
                'message': 'You do not have permission to access this resource'
            }), 403
        
        return render_template('errors/403.html', error_id=error_id), 403
    
    def handle_internal_error(self, error):
        """Handle 500 errors."""
        error_id = self._log_error(error, 'Internal Server Error')
        
        if request.is_json:
            return jsonify({
                'error': 'Internal server error',
                'error_id': error_id,
                'message': 'An internal server error occurred'
            }), 500
        
        return render_template('errors/500.html', error_id=error_id), 500
    
    def _log_error(self, error, error_type, level='error'):
        """Log error with context information and return error ID."""
        import uuid
        error_id = str(uuid.uuid4())[:8]
        
        context = {
            'error_id': error_id,
            'error_type': error_type,
            'url': request.url if request else 'N/A',
            'method': request.method if request else 'N/A',
            'user_agent': request.headers.get('User-Agent', 'N/A') if request else 'N/A',
            'remote_addr': request.remote_addr if request else 'N/A',
            'error_message': str(error),
            'traceback': traceback.format_exc()
        }
        
        log_message = f"{error_type} [{error_id}]: {str(error)}"
        
        if level == 'error':
            logger.error(log_message, extra=context)
        elif level == 'warning':
            logger.warning(log_message, extra=context)
        else:
            logger.info(log_message, extra=context)
        
        return error_id

# Global error handler instance
error_handler = ErrorHandler()