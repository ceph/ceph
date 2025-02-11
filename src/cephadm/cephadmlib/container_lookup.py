# container_lookup.py - high-level functions for getting container info

from typing import Optional

import logging

from .container_engines import ContainerInfo, parsed_container_image_stats
from .container_types import get_container_stats
from .context import CephadmContext
from .daemon_identity import DaemonIdentity
from .listing import daemons_matching
from .listing_updaters import CoreStatusUpdater


logger = logging.getLogger()


def get_container_info(
    ctx: CephadmContext, daemon_filter: str, by_name: bool
) -> Optional[ContainerInfo]:
    """
    :param ctx: Cephadm context
    :param daemon_filter: daemon name or type
    :param by_name: must be set to True if daemon name is provided
    :return: Container information or None
    """
    if by_name and '.' not in daemon_filter:
        logger.warning(
            f'Trying to get container info using invalid daemon name {daemon_filter}'
        )
        return None

    # configure filters: fsid and (daemon name or daemon type)
    kwargs = {
        'fsid': ctx.fsid,
        ('daemon_name' if by_name else 'daemon_type'): daemon_filter,
    }
    # use keep_container_info to cache the ContainerInfo generated
    # during the loop and hopefully avoid having to perform the same
    # lookup right away.
    _cinfo_key = '_container_info'
    _updater = CoreStatusUpdater(keep_container_info=_cinfo_key)
    matching_daemons = [
        _updater.expand(ctx, entry)
        for entry in daemons_matching(ctx, **kwargs)
    ]

    if not matching_daemons:
        # no matches at all
        logger.debug(
            'no daemons match: daemon_filter=%r, by_name=%r',
            daemon_filter,
            by_name,
        )
        return None
    if by_name and len(matching_daemons) > 1:
        # too many matches while searching by name
        logger.warning(
            f'Found multiple daemons sharing same name: {daemon_filter}'
        )
        # Prefer to take the first daemon we find that is actually running, or
        # just the first in the list if none are running
        # (key reminder: false (0) sorts before true (1))
        matching_daemons = sorted(
            matching_daemons, key=lambda d: d.get('state') != 'running'
        )

    matched_deamon = matching_daemons[0]
    is_running = matched_deamon.get('state') == 'running'
    image_name = matched_deamon.get('container_image_name', '')
    if is_running:
        cinfo = matched_deamon.get(_cinfo_key)
        if cinfo:
            # found a cached ContainerInfo while getting daemon statuses
            return cinfo
        return get_container_stats(
            ctx,
            DaemonIdentity.from_name(
                matched_deamon['fsid'], matched_deamon['name']
            ),
        )
    elif image_name:
        # this daemon's container is not running. the regular container inspect
        # command will not work. Fall back to inspecting the container image
        assert isinstance(image_name, str)
        return parsed_container_image_stats(ctx, image_name)
    # not running, but no image name to look up!
    logger.debug(
        'bad daemon state: no image, not running: %r', matched_deamon
    )
    return None
