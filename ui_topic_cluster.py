"""
Thin wrapper for backward compatible imports.

Keep:
  from ui_topic_cluster import show_topic_cluster_admin
"""

from __future__ import annotations

from ui_topic_cluster_admin import show_topic_cluster_admin

__all__ = ["show_topic_cluster_admin"]

