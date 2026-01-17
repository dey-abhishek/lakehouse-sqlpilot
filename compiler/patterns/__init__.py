"""
Pattern Factory - Creates pattern instances based on type
"""

from typing import Dict, Any
from .base_pattern import BasePattern
from .incremental_append import IncrementalAppendPattern
from .scd2 import SCD2Pattern
from .merge_upsert import MergeUpsertPattern
from .full_replace import FullReplacePattern


class PatternFactory:
    """Factory for creating pattern instances"""
    
    # Registry of available patterns
    _patterns = {
        'INCREMENTAL_APPEND': IncrementalAppendPattern,
        'SCD2': SCD2Pattern,
        'MERGE_UPSERT': MergeUpsertPattern,
        'FULL_REPLACE': FullReplacePattern,
    }
    
    @classmethod
    def create_pattern(cls, plan: Dict[str, Any]) -> BasePattern:
        """
        Create pattern instance from plan
        
        Args:
            plan: Validated plan dictionary
            
        Returns:
            Pattern instance
            
        Raises:
            ValueError: If pattern type not supported
        """
        pattern_type = plan.get('pattern', {}).get('type')
        
        if pattern_type not in cls._patterns:
            supported = ', '.join(cls._patterns.keys())
            raise ValueError(
                f"Unsupported pattern type: {pattern_type}. "
                f"Supported patterns: {supported}"
            )
        
        pattern_class = cls._patterns[pattern_type]
        return pattern_class(plan)
    
    @classmethod
    def get_supported_patterns(cls) -> list:
        """Get list of supported pattern types"""
        return list(cls._patterns.keys())


# Export all patterns
__all__ = [
    'PatternFactory',
    'BasePattern',
    'IncrementalAppendPattern',
    'SCD2Pattern',
    'MergeUpsertPattern',
    'FullReplacePattern',
]
