"""PostgreSQL Manager - Modular PostgreSQL management system.

This module provides a comprehensive PostgreSQL management interface
using a modular architecture for better maintainability and testing.
"""

# Import the main class from the modular structure
from .postgres_manager.core import PostgresManager

# Import individual components for direct access if needed
from .postgres_manager.config_manager import PostgresConfigManager

from .postgres_manager.user_manager import PostgresUserManager
from .postgres_manager.system_utils import SystemUtils
from .postgres_manager.constants import PostgresConstants

# Maintain backward compatibility by exposing the main class
__all__ = [
    'PostgresManager',
    'PostgresConfigManager', 
    'PostgresUserManager',
    'SystemUtils',
    'PostgresConstants'
]