"""Optional IPython display; requires the ``ipython`` extra only when called."""

from .renderer import render_live, render_report


def show(report, live=False, handle=None):
    """Display a report; pass the returned handle back to replace the same output."""
    from IPython.display import HTML, display

    content = HTML(render_live(report) if live else render_report(report))
    if handle is None:
        return display(content, display_id=True)
    handle.update(content)
    return handle
