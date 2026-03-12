import logging
from typing import Any

from mgr_module import MgrModule
import rbd

logger = logging.getLogger(__name__)

POOL_NAME = ".nvmeof"


class NVMeoF(MgrModule):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(NVMeoF, self).__init__(*args, **kwargs)

    def _pool_exists(self, pool_name: str) -> bool:
        logger.info(f"checking if pool {pool_name} exists")
        pool_exists = self.pool_exists(pool_name)
        if pool_exists:
            logger.info(f"pool {pool_name} already exists")
        else:
            logger.info(f"pool {pool_name} doesn't exist")
        return pool_exists

    def _create_pool(self, pool_name: str) -> None:
        try:
            self.create_pool(pool_name)
            logger.info(f"Pool '{pool_name}' created.")
        except Exception:
            logger.error(f"Error creating pool '{pool_name}", exc_info=True)
            raise

    def _enable_rbd_application(self, pool_name: str) -> None:
        try:
            self.appify_pool(pool_name, 'rbd')
            logger.info(f"'rbd' application enabled on pool '{pool_name}'.")
        except Exception:
            logger.error(
                f"Failed to enable 'rbd' application on '{pool_name}'",
                exc_info=True
            )
            raise

    def _rbd_pool_init(self, pool_name: str) -> None:
        try:
            with self.rados.open_ioctx(pool_name) as ioctx:
                rbd.RBD().pool_init(ioctx, False)
            logger.info(f"RBD pool_init completed on '{pool_name}'.")
        except Exception:
            logger.error(f"Failed to initialize RBD pool '{pool_name}'", exc_info=True)
            raise

    def create_pool_if_not_exists(self) -> None:
        if not self._pool_exists(POOL_NAME):
            self._create_pool(POOL_NAME)
        self._enable_rbd_application(POOL_NAME)
        self._rbd_pool_init(POOL_NAME)
