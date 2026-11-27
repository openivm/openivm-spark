"""Stdlib-only HTML rendering for OpenIVM CREATE and REFRESH telemetry."""

from .display import show
from .renderer import render_live, render_report

__all__ = ["render_live", "render_report", "show"]
