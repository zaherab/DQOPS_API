"""ODPS (Open Data Product Specification) integration module.

Provides mapping between DQ Platform check categories and ODPS 4.1
standardized data quality dimensions.
"""

from dq_platform.odps.dimension_mapping import (
    ALL_DIMENSIONS,
    CATEGORY_TO_DIMENSION,
    SEVERITY_WEIGHTS,
    ODPSDimension,
    get_all_check_types_for_dimension,
    get_dimension_for_category,
    get_dimension_for_check_type,
)

__all__ = [
    "ODPSDimension",
    "CATEGORY_TO_DIMENSION",
    "SEVERITY_WEIGHTS",
    "ALL_DIMENSIONS",
    "get_dimension_for_check_type",
    "get_dimension_for_category",
    "get_all_check_types_for_dimension",
]
