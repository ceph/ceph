import logging
from typing import Optional, Any

from rados import TimedOut, ObjectNotFound, Rados
from mgr_module import NFS_POOL_NAME as POOL_NAME
from .ganesha_conf import format_block
from .utils import USER_CONF_PREFIX
from .ganesha_raw_conf import RawBlock

log = logging.getLogger(__name__)


def _check_rados_notify(ioctx: Any, obj: str) -> None:
    try:
        ioctx.notify(obj)
    except TimedOut:
        log.exception("Ganesha timed out")


class NFSRados:
    def __init__(self, rados: 'Rados', namespace: str) -> None:
        self.rados = rados
        self.pool = POOL_NAME
        self.namespace = namespace

    def _make_rados_url(self, obj: str) -> str:
        return "rados://{}/{}/{}".format(self.pool, self.namespace, obj)

    def _create_url_block(self, obj_name: str) -> RawBlock:
        return RawBlock('%url', values={'value': self._make_rados_url(obj_name)})

    def write_obj(self,
                  conf_block: str,
                  obj: str,
                  config_obj: str = '',
                  should_notify: Optional[bool] = True) -> None:
        with self.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            ioctx.write_full(obj, conf_block.encode('utf-8'))
            if not config_obj:
                # Return after creating empty common config object
                return
            log.debug("write configuration into rados object %s/%s/%s",
                      self.pool, self.namespace, obj)

            # Add created obj url to common config obj
            ioctx.append(config_obj, format_block(
                         self._create_url_block(obj)).encode('utf-8'))
            if should_notify:
                _check_rados_notify(ioctx, config_obj)
            log.debug("Added %s url to %s", obj, config_obj)

    def read_obj(self, obj: str) -> Optional[str]:
        with self.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            try:
                return ioctx.read(obj, 1048576).decode()
            except ObjectNotFound:
                return None

    def update_obj(self, conf_block: str, obj: str, config_obj: str,
                   should_notify: Optional[bool] = True) -> None:
        with self.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            ioctx.write_full(obj, conf_block.encode('utf-8'))
            log.debug("write configuration into rados object %s/%s/%s",
                      self.pool, self.namespace, obj)
            if should_notify:
                _check_rados_notify(ioctx, config_obj)
            log.debug("Update export %s in %s", obj, config_obj)

    def remove_obj(self, obj: str, config_obj: str, should_notify: Optional[bool] = True) -> None:
        with self.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            export_urls = ioctx.read(config_obj)
            url = '%url "{}"\n\n'.format(self._make_rados_url(obj))
            export_urls = export_urls.replace(url.encode('utf-8'), b'')
            ioctx.remove_object(obj)
            ioctx.write_full(config_obj, export_urls)
            if should_notify:
                _check_rados_notify(ioctx, config_obj)
            log.debug("Object deleted: %s", url)

    def remove_all_obj(self) -> None:
        with self.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            for obj in ioctx.list_objects():
                obj.remove()

    def check_config(self, config: str = USER_CONF_PREFIX) -> bool:
        with self.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            for obj in ioctx.list_objects():
                if obj.key.startswith(config):
                    return True
        return False
