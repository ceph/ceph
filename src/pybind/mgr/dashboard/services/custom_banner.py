import logging
from typing import Optional

from mgr_module import _get_localized_key

from .. import mgr

logger = logging.getLogger(__name__)


def set_login_banner_mgr(inbuf: str, mgr_id: Optional[str] = None):
    item_key = 'custom_login_banner'
    if mgr_id is not None:
        mgr.set_store(_get_localized_key(mgr_id, item_key), inbuf)
    else:
        mgr.set_store(item_key, inbuf)


def get_login_banner_mgr():
    banner_text = mgr.get_store('custom_login_banner')
    logger.info('Reading custom login banner: %s', banner_text)
    return banner_text


def unset_login_banner_mgr():
    mgr.set_store('custom_login_banner', None)
    logger.info('Removing custom login banner')
