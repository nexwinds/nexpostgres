from typing import Dict, List, Tuple
from enum import Enum

class PermissionCombination(Enum):
    """Predefined permission combinations for database users."""
    ALL_PERMISSIONS = "all_permissions"
    NO_PERMISSIONS = "no_permissions"
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"

class PermissionManager:
    """Manages database user permissions with predefined combinations."""
    
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
            'connect': False,
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
        PermissionCombination.NO_PERMISSIONS: "No Permissions / Deactivate",
        PermissionCombination.READ_ONLY: "Read Only Access",
        PermissionCombination.READ_WRITE: "Read & Write Access",

    }
    
    # Descriptions for each combination
    COMBINATION_DESCRIPTIONS = {
        PermissionCombination.ALL_PERMISSIONS: "Full database access including creating tables, schemas, and managing database structure",
        PermissionCombination.NO_PERMISSIONS: "No database access - user is effectively deactivated",
        PermissionCombination.READ_ONLY: "Can connect and read data from existing tables",
        PermissionCombination.READ_WRITE: "Can read, insert, update, and delete data but cannot create new tables",

    }
    
    @classmethod
    def get_permission_combinations(cls) -> List[Dict[str, str]]:
        """Get all available permission combinations for UI display.
        
        Returns:
            List of dictionaries with combination info for UI
        """
        combinations = []
        for combo in PermissionCombination:
            combinations.append({
                'value': combo.value,
                'label': cls.COMBINATION_LABELS[combo],
                'description': cls.COMBINATION_DESCRIPTIONS[combo],
                'permissions': cls.PERMISSION_COMBINATIONS[combo]
            })
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
    def detect_combination_from_permissions(cls, permissions: Dict[str, bool]) -> Tuple[str, bool]:
        """Detect which predefined combination matches the given permissions.
        
        Args:
            permissions: Dictionary of current permission flags
            
        Returns:
            Tuple of (combination_key, is_exact_match)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"DEBUG: Detecting combination for permissions: {permissions}")
        
        # Check for exact matches, prioritizing more specific combinations first
        # Order matters: check from most restrictive to most permissive
        priority_order = [
            PermissionCombination.NO_PERMISSIONS,
            PermissionCombination.READ_ONLY,
            PermissionCombination.READ_WRITE,
            PermissionCombination.ALL_PERMISSIONS
        ]
        
        for combo in priority_order:
            combo_perms = cls.PERMISSION_COMBINATIONS[combo]
            logger.info(f"DEBUG: Comparing with {combo.value}: {combo_perms}")
            if permissions == combo_perms:
                logger.info(f"DEBUG: Exact match found: {combo.value}")
                return combo.value, True
        
        # If no exact match, return closest match or custom
        logger.info("DEBUG: No exact match found, returning custom")
        return "custom", False
    
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