# templating.py - functions to wrap string/file templating libs

import enum

from typing import Any, Optional, IO

import jinja2

from .context import CephadmContext

_PKG = __name__.rsplit('.', 1)[0]
_DIR = 'templates'


class Templates(str, enum.Enum):
    """Known template files."""

    ceph_service = 'ceph.service.j2'
    agent_service = 'agent.service.j2'

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return repr(self.value)


class Templater:
    """Cephadm's generic templater class. Based on jinja2."""

    # defaults that can be overridden for testing purposes
    # and are lazily acquired
    _jinja2_loader: Optional[jinja2.BaseLoader] = None
    _jinja2_env: Optional[jinja2.Environment] = None
    _pkg = _PKG
    _dir = _DIR

    @property
    def _env(self) -> jinja2.Environment:
        if self._jinja2_env is None:
            self._jinja2_env = jinja2.Environment(loader=self._loader)
        return self._jinja2_env

    @property
    def _loader(self) -> jinja2.BaseLoader:
        if self._jinja2_loader is None:
            self._jinja2_loader = jinja2.PackageLoader(self._pkg, self._dir)
        return self._jinja2_loader

    def render_str(
        self, ctx: CephadmContext, template: str, **kwargs: Any
    ) -> str:
        return self._env.from_string(template).render(ctx=ctx, **kwargs)

    def render(self, ctx: CephadmContext, name: str, **kwargs: Any) -> str:
        return self._env.get_template(str(name)).render(ctx=ctx, **kwargs)

    def render_to_file(
        self, fp: IO, ctx: CephadmContext, name: str, **kwargs: Any
    ) -> None:
        self._env.get_template(str(name)).stream(ctx=ctx, **kwargs).dump(fp)


# create a defaultTemplater instace from the Templater class that will
# be used to provide a simple set of methods
defaultTemplater = Templater()

# (temporary alias) template_str is currently used by the cephadm code
template_str = defaultTemplater.render_str

# alias methods as module level functions for convenience. most callers do
# not need to care that these are implemented via a class
render_str = defaultTemplater.render_str
render = defaultTemplater.render
render_to_file = defaultTemplater.render_to_file
