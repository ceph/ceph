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
