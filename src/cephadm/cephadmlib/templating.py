# templating.py - functions to wrap string/file templating libs


from typing import Any

import jinja2

from .context import CephadmContext


def template_str(ctx: CephadmContext, template: str, **kwargs: Any) -> str:
    loader = jinja2.BaseLoader()
    return (
        jinja2.Environment(loader=loader)
        .from_string(template)
        .render(
            ctx=ctx,
            **kwargs,
        )
    )
