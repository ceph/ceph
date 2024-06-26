from typing import Dict, Optional

import posixpath
import re

# Initially, this regex is pretty restrictive.  But I (JJM) find that
# starting out more restricitive is better than not because it's generally
# easier to relax strict rules then discover someone relies on lax rules
# that fail somewhere and now you've got backwards-compatibilty issues
# to worry about.
#
# The names are valid dns fragments as well since the cluster_id can be
# combined with the hostname for a virtual hostname for the container.
_name_re = re.compile('^[a-zA-Z0-9]($|[a-zA-Z0-9-]{,16}[a-zA-Z0-9]$)')

# We might want to open up share names to non-special unicode chars too.
# but as above it's easier to start strict.
_share_re = re.compile('^[a-zA-Z0-9_][a-zA-Z0-9. _-]{,63}$')


def valid_id(value: str) -> bool:
    """Return true if value is a valid (cluster|share|etc) ID."""
    return bool(_name_re.match(value))


def check_id(value: str) -> None:
    """Raise ValueError if value is not a valid ID."""
    if not valid_id(value):
        raise ValueError(f"{value:!r} is not a valid ID")


def valid_share_name(value: str) -> bool:
    """Return true if value is a valid share name."""
    return bool(_share_re.match(value))


def check_share_name(value: str) -> None:
    """Raise ValueError if value is not a valid share name."""
    if not valid_share_name(value):
        raise ValueError(f"{value:!r} is not a valid share name")


# alias for normpath so other smb libs can just import validation module
normalize_path = posixpath.normpath


def valid_path(value: str) -> bool:
    """Return true if value is a valid path for a share."""
    path = normalize_path(value)
    # ensure that post-normalization there are no relative path elements or
    # empty segments
    if path == '/':
        # special case / to refer to the root of the volume/subvolume.
        # it is always valid.
        return True
    return not any(
        p in ('.', '..', '') for p in path.lstrip('/').split(posixpath.sep)
    )


def check_path(value: str) -> None:
    """Raise ValueError if value is not a valid share path."""
    if not valid_path(value):
        raise ValueError(f'{value!r} is not a valid share path')


CUSTOM_CAUTION_KEY = '_allow_customization'
CUSTOM_CAUTION_VALUE = (
    'i-take-responsibility-for-all-samba-configuration-errors'
)


def check_custom_options(opts: Optional[Dict[str, str]]) -> None:
    """Raise ValueError if a custom configuration options dict is not valid."""
    if opts is None:
        return
    if opts.get(CUSTOM_CAUTION_KEY) != CUSTOM_CAUTION_VALUE:
        raise ValueError(
            'options lack custom override permission key and value'
            f' (review documentation pertaining to {CUSTOM_CAUTION_KEY})'
        )
    for key, value in opts.items():
        if '[' in key or ']' in key:
            raise ValueError(
                f'custom option key may not contain square brackets: {key!r}'
            )
        if '\n' in key:
            raise ValueError(
                f'custom option key may not contain newlines: {key!r}'
            )
        if '\n' in value:
            raise ValueError(
                f'custom option value may not contain newlines: {key!r}'
            )


def clean_custom_options(
    opts: Optional[Dict[str, str]]
) -> Optional[Dict[str, str]]:
    """Return a version of the custom options dictionary cleaned of special
    validation parameters.
    """
    if opts is None:
        return None
    return {k: v for k, v in opts.items() if k != CUSTOM_CAUTION_KEY}


def check_access_name(name: str) -> None:
    if ' ' in name or '\t' in name or '\n' in name:
        raise ValueError(
            'login name may not contain spaces, tabs, or newlines'
        )
    if len(name) > 128:
        raise ValueError('login name may not exceed 128 characters')
