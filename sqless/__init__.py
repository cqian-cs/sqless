"""
sqless - A schema-flexible, zero-abstraction SQLite interface supporting Relational tables, JSON tables, Full-Text Search, and Semantic Search.
"""

__version__ = "3.1.0"
__author__ = "cqian.cs"
__email__ = "cqian.cs@qq.com"

from .database import DB, DBS


def __getattr__(name):
    """Lazy loading for optional modules."""
    if name == "VecTable":
        from .vec_table import VecTable
        return VecTable
    if name == "JsonTable":
        from .json_table import JsonTable
        return JsonTable
    if name == "FtsTable":
        from .fts_table import FtsTable
        return FtsTable
    if name == "RelTable":
        from .rel_table import RelTable
        return RelTable
    if name == "run_server":
        from .server import run_server
        return run_server
    if name == "api":
        from .server import api
        return api
    if name == "RDB":
        from .client import RDB
        return RDB
    raise AttributeError(f"module 'sqless' has no attribute {name!r}")
