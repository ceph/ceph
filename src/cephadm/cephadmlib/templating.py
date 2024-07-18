# templating.py - functions to wrap string/file templating libs

import enum
import os
import posixpath
import shlex
import zipimport

from typing import Any, Optional, IO, Tuple, Callable, cast

import jinja2
import jinja2.loaders

from .context import CephadmContext

_PKG = __name__.rsplit('.', 1)[0]
_DIR = 'templates'


class Templates(str, enum.Enum):
    """Known template files."""

    ceph_service = 'ceph.service.j2'
    agent_service = 'agent.service.j2'
    dropin_service = 'dropin.service.j2'
    init_ctr_service = 'init_ctr.service.j2'
    sidecar_service = 'sidecar.service.j2'
    cluster_logrotate_config = 'cluster.logrotate.config.j2'
    cephadm_logrotate_config = 'cephadm.logrotate.config.j2'
    sidecar_run = 'sidecar.run.j2'
    init_ctr_run = 'init_containers.run.j2'

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return repr(self.value)


class TemplateNotFoundInZipApp(jinja2.TemplateNotFound):
    def __init__(
        self,
        template: str,
        *,
        relative_path: str = '',
        archive_path: str = '',
    ) -> None:
        super().__init__(template)
        self.relative_path = relative_path
        self.archive_path = archive_path

    def __str__(self) -> str:
        return (
            f'{self.message}: path {self.relative_path!r}'
            f' not found in {self.archive_path!r}'
        )


class _PackageLoader(jinja2.PackageLoader):
    """Workaround for PackageLoader when using cephadm with relative paths.

    It was found that running the cephadm zipapp from a local dir (like:
    `./cephadm`) instead of an absolute path (like: `/usr/sbin/cephadm`) caused
    the PackageLoader to fail to load the template.  After investigation it was
    found to relate to how the PackageLoader tries to normalize paths and yet
    the zipimporter type did not have a normalized path (/home/foo/./cephadm
    and /home/foo/cephadm respectively).  When a full absolute path is passed
    to zipimporter's get_data method it uses the (non normalized) .archive
    property to strip the prefix from the argument. When the argument is a
    normalized path - the prefix fails to match and is not stripped and then
    the full path fails to match any value in the archive.

    This shim subclass of jinja2.PackageLoader customizes the code path used to
    load files from the zipimporter so that we try to do the prefix handling
    all with normalized paths and only path the relative paths to the
    zipimporter function.
    """

    def get_source(
        self, environment: jinja2.Environment, template: str
    ) -> Tuple[str, str, Optional[Callable[[], bool]]]:
        if isinstance(self._loader, zipimport.zipimporter):
            return self._get_archive_source(template)
        return super().get_source(environment, template)

    def _get_archive_source(self, template: str) -> Tuple[str, str, None]:
        assert isinstance(self._loader, zipimport.zipimporter)
        arelpath = posixpath.join(
            self.package_name, self.package_path, template
        )
        if any(p == '.' or p == '..' for p in arelpath.split(posixpath.sep)):
            raise ValueError('template path contains invalid components')
        try:
            source = cast(bytes, self._loader.get_data(arelpath))
        except OSError as e:
            not_found = TemplateNotFoundInZipApp(
                template,
                relative_path=arelpath,
                archive_path=self._loader.archive,
            )
            raise not_found from e
        path = os.path.normpath(
            posixpath.join(self._loader.archive, arelpath)
        )
        return source.decode(self.encoding), path, None


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
            self._jinja2_env.filters['shellquote'] = shlex.quote
        return self._jinja2_env

    @property
    def _loader(self) -> jinja2.BaseLoader:
        if self._jinja2_loader is None:
            self._jinja2_loader = _PackageLoader(self._pkg, self._dir)
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

# alias methods as module level functions for convenience. most callers do
# not need to care that these are implemented via a class
render_str = defaultTemplater.render_str
render = defaultTemplater.render
render_to_file = defaultTemplater.render_to_file
