"""
BT Engine - Backtesting Engine Package
=====================================

A comprehensive backtesting framework for financial strategies.
"""

# Import core data structures
from .engine import (
    Order,
    Signal,
)

# Import helper classes
from .helpers import (
    PolygonBarLoader,
)

# Package metadata
__version__ = "0.1.0"
__author__ = "melgazar9"

# Define what gets imported with "from bt_engine import *"
__all__ = [
    "Signal",
    "Order",
    "PolygonBarLoader",
]
