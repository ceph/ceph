# container_lookup.py - high-level functions for getting container info

from operator import itemgetter
from typing import Optional, Tuple

import fnmatch
import logging

from .container_engines import (
    ContainerInfo,
    ImageInfo,
    parsed_container_image_list,
    parsed_container_image_stats,
)
from .container_types import get_container_stats
from .context import CephadmContext
from .daemon_identity import DaemonIdentity
from .daemons.ceph import ceph_daemons
from .exceptions import Error
from .listing import LegacyDaemonEntry, daemons_matching
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


def infer_local_ceph_image(
    ctx: CephadmContext, container_path: str = ''
) -> Optional[str]:
    """Infer the best ceph image to use based on the following criteria:
    Out of all images labeled as ceph that are non-dangling, prefer
    1. the same image as the daemon container specified by -name arg (if provided).
    2. the image used by any ceph container running on the host
    3. the most ceph recent image on the host

    :return: An image name or none
    """
    # enumerate ceph images on the system
    images = parsed_container_image_list(
        ctx,
        filters=['dangling=false', 'label=ceph=True'],
        container_path=container_path,
    )
    if not images:
        logger.warning('No non-dangling ceph images found')
        return None  # no images at all cached on host

    # find running ceph daemons
    _daemons = ceph_daemons()
    daemon_name = getattr(ctx, 'name', '')
    _cinfo_key = '_container_info'
    _updater = CoreStatusUpdater(keep_container_info=_cinfo_key)
    matching_daemons = [
        itemgetter(_cinfo_key, 'name')(_updater.expand(ctx, entry))
        for entry in daemons_matching(
            ctx, fsid=ctx.fsid, daemon_type_predicate=lambda t: t in _daemons
        )
    ]
    # collect the running ceph daemon image ids
    images_in_use_by_daemon = set(
        d.image_id
        for d, n in matching_daemons
        if (n == daemon_name and d is not None)
    )
    images_in_use = set(
        d.image_id for d, _ in matching_daemons if d is not None
    )

    # prioritize images
    def _keyfunc(image: ImageInfo) -> Tuple[bool, bool, str]:
        return (
            bool(
                image.digest
                and any(
                    v.startswith(image.image_id)
                    for v in images_in_use_by_daemon
                )
            ),
            bool(
                image.digest
                and any(v.startswith(image.image_id) for v in images_in_use)
            ),
            image.created,
        )

    images.sort(key=_keyfunc, reverse=True)
    best_image = images[0]
    name_match, ceph_match, _ = _keyfunc(best_image)
    reason = 'not in the list of non-dangling images with ceph=True label'
    if images_in_use_by_daemon and not name_match:
        expected = list(images_in_use_by_daemon)[0]
        logger.warning(
            'Not using image %r of named daemon: %s',
            expected,
            reason,
        )
    if images_in_use and not ceph_match:
        expected = list(images_in_use)[0]
        logger.warning(
            'Not using image %r of ceph daemon: %s',
            expected,
            reason,
        )
    return best_image.name


def infer_daemon_identity(
    ctx: CephadmContext, partial_name: str
) -> DaemonIdentity:
    """Given a partial daemon/service name, infer the identity of the
    daemon.
    """

    if not partial_name:
        raise Error('a daemon type is required to infer a service name')
    if '.' in partial_name:
        _type, _name = partial_name.split('.', 1)
    else:
        _type, _name = partial_name, ''
    # allow searching for a name with just the beginning without having
    # to expliclity supply a trailing asterisk
    _name += '*'
    matches = []
    for d in daemons_matching(ctx, fsid=ctx.fsid, daemon_type=_type):
        if isinstance(d, LegacyDaemonEntry):
            logger.info('Ignoring legacy daemon %s', d.name)
            continue  # ignore legacy daemons
        if fnmatch.fnmatch(d.identity.daemon_id, _name):
            matches.append(d)

    if not matches:
        raise Error(f'no daemons match {partial_name!r}')
    if len(matches) > 1:
        excess = ', '.join(d.identity.daemon_name for d in matches)
        raise Error(f'too many daemons match {partial_name!r} ({excess})')
    ident = matches[0].identity
    logger.info('Inferring daemon %s', ident.daemon_name)
    return ident


def identify(ctx: CephadmContext) -> DaemonIdentity:
    """Given a context try and determine a specific daemon identity to use
    based on the --name CLI option (exact) or --infer-name (partial match)
    option.
    """
    if not ctx.fsid:
        raise Error('must pass --fsid to specify cluster')
    name = getattr(ctx, 'name', '')
    if name:
        return DaemonIdentity.from_name(ctx.fsid, name)
    iname = getattr(ctx, 'infer_name', '')
    if iname:
        return infer_daemon_identity(ctx, iname)
    raise Error(
        'must specify a daemon name'
        ' (use --name/-n or --infer-name/-i for example)'
    )
