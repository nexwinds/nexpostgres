"""PostgreSQL version resolution utilities.

This module provides centralized version resolution logic for PostgreSQL installations,
ensuring DRY principles and maintainability."""

import logging
from typing import Dict, List, Optional, Tuple
from .constants import PostgresConstants
from .error_handler import PostgresErrorHandler


class PostgresVersionResolver:
    """Handles PostgreSQL version resolution and validation."""
    
    def __init__(self, ssh_manager, system_utils, logger=None):
        """Initialize the version resolver.
        
        Args:
            ssh_manager: SSH connection manager
            system_utils: System utilities instance
            logger: Logger instance
        """
        self.ssh = ssh_manager
        self.system_utils = system_utils
        self.logger = logger or logging.getLogger(__name__)
        self.error_handler = PostgresErrorHandler(self.logger)
    
    def resolve_version(self, requested_version: str = None) -> Tuple[bool, str, Dict]:
        """Resolve a PostgreSQL version to install.
        
        This is the main entry point for version resolution. It handles:
        - Version validation
        - Fallback logic
        - EOL/deprecation warnings
        - Latest patch version resolution
        
        Args:
            requested_version: Requested major version (e.g., '15', '16')
                             If None, uses recommended version
        
        Returns:
            tuple: (success, resolved_version, metadata)
                  metadata contains: status, eol_date, warnings, etc.
        """
        self.logger.info(f"Resolving PostgreSQL version: {requested_version or 'recommended'}")
        
        # Step 1: Determine target version
        target_version = requested_version or PostgresConstants.SUPPORTED_VERSIONS['recommended']
        
        # Step 2: Validate and get metadata - strict enforcement, no fallbacks
        is_valid, metadata = self._validate_and_get_metadata(target_version)
        
        if not is_valid:
            return False, f"PostgreSQL version {target_version} is not supported or available", metadata
        
        # Step 3: Get latest patch version
        success, resolved_version = self._get_latest_patch_version(target_version)
        if not success:
            return False, f"Failed to resolve patch version for {target_version}", metadata
        
        # Step 4: Add warnings for EOL/deprecated versions
        self._add_version_warnings(target_version, metadata)
        
        self.logger.info(f"Successfully resolved version {requested_version or 'recommended'} to {resolved_version}")
        return True, resolved_version, metadata
    
    def _validate_and_get_metadata(self, version: str) -> Tuple[bool, Dict]:
        """Validate version and get metadata.
        
        Args:
            version: Major version to validate
            
        Returns:
            tuple: (is_valid, metadata_dict)
        """
        # Only support versions 17, 16, and 15
        supported_versions = ['17', '16', '15']
        
        metadata = {
            'requested_version': version,
            'is_supported': version in supported_versions,
            'is_available': False,
            'warnings': []
        }
        
        # Check if version is in our supported list (17, 16, 15 only)
        if not metadata['is_supported']:
            self.logger.error(f"Version {version} is not supported. Only versions 17, 16, and 15 are supported.")
            return False, metadata
        
        # Get version info from constants
        if version in PostgresConstants.VERSION_SPECIFIC:
            version_info = PostgresConstants.VERSION_SPECIFIC[version]
            metadata.update({
                'status': version_info['status'],
                'eol_date': version_info['eol_date']
            })
        
        # Check if version is available in package repositories
        if self.system_utils.validate_postgres_version(version):
            metadata['is_available'] = True
            return True, metadata
        
        # If not available, try setting up the official PostgreSQL repository
        self.logger.info(f"PostgreSQL version {version} not found in default repositories, setting up official repository...")
        repo_success, repo_message = self.system_utils.setup_postgres_repository()
        
        if repo_success:
            self.logger.info(f"Repository setup successful: {repo_message}")
            # Check availability again after repository setup
            if self.system_utils.validate_postgres_version(version):
                metadata['is_available'] = True
                return True, metadata
            else:
                self.logger.warning(f"PostgreSQL version {version} is still not available after repository setup")
                metadata['warnings'].append(f"Version {version} is not available even after repository setup")
                return False, metadata
        else:
            self.error_handler.log_warning_with_context(
                f"Failed to setup PostgreSQL repository: {repo_message}",
                "Repository Setup"
            )
            metadata['warnings'].append(f"Repository setup failed: {repo_message}")
            return False, metadata
    
    def _get_latest_patch_version(self, major_version: str) -> Tuple[bool, str]:
        """Get the latest patch version for a major version.
        
        Args:
            major_version: Major version (e.g., '15', '16')
            
        Returns:
            tuple: (success, version_string)
        """
        self.logger.debug(f"Finding latest patch version for PostgreSQL {major_version}")
        
        os_type = self.system_utils.detect_os()
        pkg_commands = self.system_utils.get_package_manager_commands()
        
        if not pkg_commands:
            self.logger.warning("No package manager commands available")
            return True, major_version  # Return major version as fallback
        
        if os_type == 'debian':
            return self._get_debian_patch_version(major_version, pkg_commands)
        elif os_type == 'rhel':
            return self._get_rhel_patch_version(major_version, pkg_commands)
        else:
            self.logger.warning(f"Unsupported OS type: {os_type}")
            return True, major_version
    
    def _get_debian_patch_version(self, major_version: str, pkg_commands: Dict) -> Tuple[bool, str]:
        """Get latest patch version for Debian/Ubuntu systems.
        
        Args:
            major_version: Major version
            pkg_commands: Package manager commands
            
        Returns:
            tuple: (success, version_string)
        """
        # Use apt-cache madison to get exact version
        search_cmd = f"apt-cache madison postgresql-{major_version} | head -1"
        result = self.ssh.execute_command(search_cmd)
        
        if result['exit_code'] == 0 and result['stdout'].strip():
            # Extract version from madison output: "postgresql-16 | 16.1-1.pgdg20.04+1 | ..."
            parts = result['stdout'].strip().split('|')
            if len(parts) >= 2:
                version_info = parts[1].strip()
                # Extract just the PostgreSQL version part (e.g., "16.1" from "16.1-1.pgdg20.04+1")
                version_match = version_info.split('-')[0]
                if version_match.startswith(major_version):
                    self.logger.debug(f"Found exact version: {version_match}")
                    return True, major_version  # Return major version for consistency
        
        # Fallback: check if the package exists at all
        check_cmd = f"apt-cache show postgresql-{major_version} > /dev/null 2>&1"
        check_result = self.ssh.execute_command(check_cmd)
        if check_result['exit_code'] == 0:
            return True, major_version
        
        return False, f"PostgreSQL {major_version} not available"
    
    def _get_rhel_patch_version(self, major_version: str, pkg_commands: Dict) -> Tuple[bool, str]:
        """Get latest patch version for RHEL/CentOS systems.
        
        Args:
            major_version: Major version
            pkg_commands: Package manager commands
            
        Returns:
            tuple: (success, version_string)
        """
        if 'list_available' in pkg_commands:
            search_cmd = f"{pkg_commands['list_available']} postgresql{major_version}-server"
            result = self.ssh.execute_command(search_cmd)
            
            if result['exit_code'] == 0 and f"postgresql{major_version}-server" in result['stdout']:
                return True, major_version
        
        return False, f"PostgreSQL {major_version} not available"
    
    def _add_version_warnings(self, version: str, metadata: Dict) -> None:
        """Add warnings for EOL or deprecated versions.
        
        Args:
            version: Version to check
            metadata: Metadata dictionary to update
        """
        if version not in PostgresConstants.VERSION_SPECIFIC:
            return
        
        # Use error handler for consistent warning logging
        self.error_handler.validate_and_log_version_warning(version)
        
        version_info = PostgresConstants.VERSION_SPECIFIC[version]
        status = version_info['status']
        eol_date = version_info['eol_date']
        
        if status == 'EOL':
            warning = f"PostgreSQL {version} has reached end-of-life on {eol_date}. Consider upgrading to version {PostgresConstants.SUPPORTED_VERSIONS['recommended']}."
            metadata['warnings'].append(warning)
        elif status == 'deprecated':
            warning = f"PostgreSQL {version} is deprecated and will reach end-of-life on {eol_date}. Consider upgrading to version {PostgresConstants.SUPPORTED_VERSIONS['recommended']}."
            metadata['warnings'].append(warning)
    
    def get_version_specific_packages(self, version: str) -> List[str]:
        """Get version-specific package names.
        
        Args:
            version: PostgreSQL major version
            
        Returns:
            list: Package names for the version
        """
        return self.system_utils.get_postgres_package_names(version)
    
    def get_available_versions(self) -> List[str]:
        """Get list of available PostgreSQL versions.
        
        Returns:
            list: Available major versions
        """
        return self.system_utils.get_available_postgres_versions()
    
    def is_version_supported(self, version: str) -> bool:
        """Check if a version is supported.
        
        Args:
            version: Major version to check
            
        Returns:
            bool: True if supported
        """
        return version in PostgresConstants.VERSION_SPECIFIC
    
    def get_version_info(self, version: str) -> Optional[Dict]:
        """Get detailed information about a version.
        
        Args:
            version: Major version
            
        Returns:
            dict: Version information or None
        """
        return PostgresConstants.VERSION_SPECIFIC.get(version)
    
    def get_recommended_version(self) -> str:
        """Get the recommended PostgreSQL version.
        
        Returns:
            str: Recommended major version
        """
        return PostgresConstants.SUPPORTED_VERSIONS['recommended']
    
    def get_latest_stable_version(self) -> str:
        """Get the latest stable PostgreSQL version.
        
        Returns:
            str: Latest stable major version
        """
        return PostgresConstants.SUPPORTED_VERSIONS['latest_stable']
    
    def get_current_lts_version(self) -> str:
        """Get the current LTS PostgreSQL version.
        
        Returns:
            str: Current LTS major version
        """
        return PostgresConstants.SUPPORTED_VERSIONS['current_lts']