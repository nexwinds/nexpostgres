from typing import Dict, List, Tuple, Optional
from enum import Enum

class PermissionCombination(Enum):
    """Predefined permission combinations for database users."""
    ALL_PERMISSIONS = "all_permissions"
    NO_PERMISSIONS = "no_permissions"
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"
    CUSTOM = "custom"

class PermissionManager:
    """Manages database user permissions with both predefined combinations and individual permissions."""
    
    # Define individual permission metadata
    INDIVIDUAL_PERMISSIONS = {
        'connect': {
            'label': 'Connect',
            'description': 'Allow user to connect to the database',
            'required': True  # Connect is always required
        },
        'select': {
            'label': 'Select (Read)',
            'description': 'Allow user to read data from tables',
            'required': False
        },
        'insert': {
            'label': 'Insert',
            'description': 'Allow user to add new data to tables',
            'required': False
        },
        'update': {
            'label': 'Update',
            'description': 'Allow user to modify existing data in tables',
            'required': False
        },
        'delete': {
            'label': 'Delete',
            'description': 'Allow user to remove data from tables',
            'required': False
        },
        'create': {
            'label': 'Create',
            'description': 'Allow user to create new tables and database objects',
            'required': False
        }
    }
    
    # Define permission combinations
    PERMISSION_COMBINATIONS = {
        PermissionCombination.ALL_PERMISSIONS: {
            'connect': True,
            'select': True,
            'insert': True,
            'update': True,
            'delete': True,
            'create': True
        },
        PermissionCombination.NO_PERMISSIONS: {
            'connect': True,
            'select': False,
            'insert': False,
            'update': False,
            'delete': False,
            'create': False
        },
        PermissionCombination.READ_ONLY: {
            'connect': True,
            'select': True,
            'insert': False,
            'update': False,
            'delete': False,
            'create': False
        },
        PermissionCombination.READ_WRITE: {
            'connect': True,
            'select': True,
            'insert': True,
            'update': True,
            'delete': True,
            'create': False
        },

    }
    
    # Human-readable labels for UI
    COMBINATION_LABELS = {
        PermissionCombination.ALL_PERMISSIONS: "All Permissions Granted",
        PermissionCombination.NO_PERMISSIONS: "Deactivate",
        PermissionCombination.READ_ONLY: "Read Only Access",
        PermissionCombination.READ_WRITE: "Read & Write Access",
        PermissionCombination.CUSTOM: "Custom Permissions",
    }
    
    # Descriptions for each combination
    COMBINATION_DESCRIPTIONS = {
        PermissionCombination.ALL_PERMISSIONS: "Full database access including creating tables, schemas, and managing database structure",
        PermissionCombination.NO_PERMISSIONS: "Can connect to database but has no other permissions - user is deactivated",
        PermissionCombination.READ_ONLY: "Can connect and read data from existing tables",
        PermissionCombination.READ_WRITE: "Can read, insert, update, and delete data but cannot create new tables",
        PermissionCombination.CUSTOM: "Custom permission configuration with individually selected permissions",
    }
    
    @classmethod
    def get_permission_combinations(cls, include_custom: bool = False) -> List[Dict[str, str]]:
        """Get all available permission combinations for UI display.
        
        Args:
            include_custom: Whether to include the custom permissions option
        
        Returns:
            List of dictionaries with combination info for UI
        """
        combinations = []
        for combo in PermissionCombination:
            if combo == PermissionCombination.CUSTOM and not include_custom:
                continue
                
            combo_data = {
                'value': combo.value,
                'label': cls.COMBINATION_LABELS[combo],
                'description': cls.COMBINATION_DESCRIPTIONS[combo]
            }
            
            # Add permissions for non-custom combinations
            if combo != PermissionCombination.CUSTOM:
                combo_data['permissions'] = cls.PERMISSION_COMBINATIONS[combo]
            
            combinations.append(combo_data)
        return combinations
    
    @classmethod
    def get_permissions_for_combination(cls, combination: str) -> Dict[str, bool]:
        """Get permission flags for a specific combination.
        
        Args:
            combination: The combination key (e.g., 'read_only')
            
        Returns:
            Dictionary of permission flags
        """
        try:
            combo_enum = PermissionCombination(combination)
            return cls.PERMISSION_COMBINATIONS[combo_enum].copy()
        except ValueError:
            # Return no permissions for invalid combination
            return cls.PERMISSION_COMBINATIONS[PermissionCombination.NO_PERMISSIONS].copy()
    
    @classmethod
    def detect_combination_from_permissions(cls, permissions: Dict[str, bool]) -> str:
        """Detect which predefined combination matches the given permissions.
        
        Args:
            permissions: Dictionary of current permission flags
            
        Returns:
            String describing the role/permission level
        """
        # Convert permission dict to set of permission names
        permission_set = set()
        
        if permissions.get('connect', False):
            permission_set.add('Connect')
        if permissions.get('select', False):
            permission_set.add('Select')
        if permissions.get('insert', False):
            permission_set.add('Insert')
        if permissions.get('update', False):
            permission_set.add('Update')
        if permissions.get('delete', False):
            permission_set.add('Delete')
        if permissions.get('create', False):
            permission_set.add('Create')
        
        # Direct permission set matching
        if permission_set == {"Connect", "Select", "Insert", "Update", "Delete", "Create"}:
            return "All Permissions Granted"
        elif permission_set == {"Connect"}:
            return "Deactivate"
        elif permission_set == {"Connect", "Select"}:
            return "Read Only Access"
        elif permission_set == {"Connect", "Select", "Insert", "Update", "Delete"}:
            return "Read & Write Access"
        else:
            return "Custom Permissions"
    
    @classmethod
    def get_combination_label(cls, combination: str) -> str:
        """Get human-readable label for a combination.
        
        Args:
            combination: The combination key
            
        Returns:
            Human-readable label
        """
        try:
            combo_enum = PermissionCombination(combination)
            return cls.COMBINATION_LABELS[combo_enum]
        except ValueError:
            return "Custom Permissions"
    
    @classmethod
    def map_legacy_permission_to_combination(cls, legacy_permission: str) -> str:
        """Map legacy permission levels to new combinations.
        
        Args:
            legacy_permission: Legacy permission level (read_only, read_write, no_access)
            
        Returns:
            Corresponding combination key
        """
        mapping = {
            'read_only': PermissionCombination.READ_ONLY.value,
            'read_write': PermissionCombination.READ_WRITE.value,
            'no_access': PermissionCombination.NO_PERMISSIONS.value,
            'all_permissions': PermissionCombination.ALL_PERMISSIONS.value
        }
        return mapping.get(legacy_permission, PermissionCombination.NO_PERMISSIONS.value)
    
    @classmethod
    def get_individual_permissions(cls) -> Dict[str, Dict[str, any]]:
        """Get all individual permission definitions.
        
        Returns:
            Dictionary of individual permissions with metadata
        """
        return cls.INDIVIDUAL_PERMISSIONS.copy()
    
    @classmethod
    def validate_individual_permissions(cls, permissions: Dict[str, bool]) -> Tuple[bool, List[str]]:
        """Validate individual permission settings.
        
        Args:
            permissions: Dictionary of permission flags
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check that connect permission is always enabled
        if not permissions.get('connect', False):
            errors.append('Connect permission is required and cannot be disabled')
        
        # Check for unknown permissions
        valid_permissions = set(cls.INDIVIDUAL_PERMISSIONS.keys())
        provided_permissions = set(permissions.keys())
        unknown_permissions = provided_permissions - valid_permissions
        
        if unknown_permissions:
            errors.append(f'Unknown permissions: {", ".join(unknown_permissions)}')
        
        return len(errors) == 0, errors
    
    @classmethod
    def apply_individual_permissions(cls, base_permissions: Dict[str, bool], 
                                   individual_permissions: Dict[str, bool]) -> Dict[str, bool]:
        """Apply individual permission changes to base permissions.
        
        Args:
            base_permissions: Current permission state
            individual_permissions: Individual permissions to apply
            
        Returns:
            Updated permission dictionary
        """
        result = base_permissions.copy()
        
        # Ensure connect is always True
        result['connect'] = True
        
        # Apply individual permission changes
        for perm, enabled in individual_permissions.items():
            if perm in cls.INDIVIDUAL_PERMISSIONS:
                if perm == 'connect':
                    # Connect must always be True
                    result[perm] = True
                else:
                    result[perm] = enabled
        
        return result
    
    @classmethod
    def detect_combination_from_permissions_enhanced(cls, permissions: Dict[str, bool]) -> Tuple[Optional[str], bool, str]:
        """Enhanced detection that returns combination, exact match status, and description.
        
        Args:
            permissions: Dictionary of current permission flags
            
        Returns:
            Tuple of (combination_key, is_exact_match, description)
        """
        # Check for exact matches with predefined combinations
        for combo in PermissionCombination:
            if combo == PermissionCombination.CUSTOM:
                continue
                
            combo_permissions = cls.PERMISSION_COMBINATIONS[combo]
            if permissions == combo_permissions:
                return combo.value, True, cls.COMBINATION_LABELS[combo]
        
        # No exact match found - this is a custom permission set
        return PermissionCombination.CUSTOM.value, False, "Custom Permissions"