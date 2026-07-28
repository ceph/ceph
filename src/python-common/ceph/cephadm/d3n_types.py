import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Union, Tuple
from ceph.utils import size_to_bytes


class D3NCacheError(ValueError):
    pass


@dataclass(frozen=True)
class D3NCacheSpec:
    """
    RGW spec's d3n_cache section:
      d3n_cache:
        filesystem: xfs|ext4           (optional, default xfs)
        size: 10G|512M|...             (required)
        devices:
          host1: [/dev/nvme0n1, ...]   (required)
          host2: [/dev/nvme1n1, ...]
    """

    filesystem: str
    size: Union[str, int]
    devices: Dict[str, list[str]]

    _SUPPORTED_FILESYSTEMS = ("xfs", "ext4")
    _KNOWN_KEYS = {"filesystem", "size", "devices"}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> "D3NCacheSpec":
        if not isinstance(obj, Mapping):
            raise D3NCacheError(
                f"d3n_cache must be a dict, got {type(obj).__name__}"
            )

        unknown = set(obj.keys()) - cls._KNOWN_KEYS
        if unknown:
            raise D3NCacheError(
                f"Unknown d3n_cache field(s): {sorted(unknown)!r}"
            )

        filesystem = str(obj.get("filesystem", "xfs")).strip()
        size = obj.get("size", None)
        if not isinstance(size, (str, int)) or (
            isinstance(size, str) and not size.strip()
        ):
            raise D3NCacheError(
                '"d3n_cache.size" must be an int (bytes) or a string like "10G"'
            )

        devices_raw = obj.get("devices", None)

        if not isinstance(devices_raw, Mapping):
            raise D3NCacheError(
                '"d3n_cache.devices" must be a mapping of host -> list of device paths'
            )

        devices: Dict[str, list[str]] = {}
        for host, devs in devices_raw.items():
            if not isinstance(host, str) or not host.strip():
                raise D3NCacheError(
                    "Invalid host key in d3n_cache.devices (must be non-empty string)"
                )
            if not isinstance(devs, list) or not devs:
                raise D3NCacheError(
                    f'"d3n_cache.devices[{host}]" must be a non-empty list of device paths'
                )

            norm: list[str] = []
            for d in devs:
                if not isinstance(d, str) or not d.startswith("/dev/"):
                    raise D3NCacheError(
                        f'Invalid device path "{d}" in d3n_cache.devices[{host}]'
                    )
                norm.append(d)

            devices[host] = sorted(set(norm))

        spec = cls(filesystem=filesystem, size=size, devices=devices)
        spec.validate()
        return spec

    def validate(self) -> None:
        if self.filesystem not in self._SUPPORTED_FILESYSTEMS:
            raise D3NCacheError(
                f'Invalid filesystem "{self.filesystem}" in d3n_cache '
                f'(supported: {", ".join(self._SUPPORTED_FILESYSTEMS)})'
            )
        if self.size is None or self.size == "":
            raise D3NCacheError('"d3n_cache.size" is required')

    def devices_for_host(self, host: str) -> list[str]:
        devs = self.devices.get(host)
        if not devs:
            raise D3NCacheError(
                f'no devices found for host "{host}" in d3n_cache.devices'
            )
        return devs


@dataclass(frozen=True)
class D3NCache:
    device: str
    filesystem: str
    mountpoint: str
    cache_path: str
    size_bytes: Optional[int] = None

    _SUPPORTED_FILESYSTEMS = ("xfs", "ext4")
    _KNOWN_KEYS = {
        "device",
        "filesystem",
        "mountpoint",
        "cache_path",
        "size_bytes",
    }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> "D3NCache":
        if not isinstance(obj, Mapping):
            raise D3NCacheError(
                f"d3n_cache must be a dict, got {type(obj).__name__}"
            )

        unknown = set(obj.keys()) - cls._KNOWN_KEYS
        if unknown:
            raise D3NCacheError(
                f"Unknown d3n_cache field(s): {sorted(unknown)!r}"
            )

        d = cls(
            device=str(obj.get("device", "")).strip(),
            filesystem=str(obj.get("filesystem", "xfs")).strip(),
            mountpoint=str(obj.get("mountpoint", "")).strip(),
            cache_path=str(obj.get("cache_path", "")).strip(),
            size_bytes=obj.get("size_bytes"),
        )
        d.validate()
        return d

    def to_json(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "device": self.device,
            "filesystem": self.filesystem,
            "mountpoint": self.mountpoint,
            "cache_path": self.cache_path,
        }
        if self.size_bytes is not None:
            out["size_bytes"] = self.size_bytes
        return out

    def validate(self) -> None:
        if not self.device:
            raise D3NCacheError("d3n_cache.device must be a non-empty string")
        if not self.device.startswith("/"):
            raise D3NCacheError(
                f"d3n_cache.device must be an absolute path, got {self.device!r}"
            )

        if self.filesystem not in self._SUPPORTED_FILESYSTEMS:
            raise D3NCacheError(
                f"Invalid filesystem {self.filesystem!r} "
                f"(supported: {', '.join(self._SUPPORTED_FILESYSTEMS)})"
            )

        if not self.mountpoint:
            raise D3NCacheError(
                "d3n_cache.mountpoint must be a non-empty string"
            )
        if not self.mountpoint.startswith("/"):
            raise D3NCacheError(
                f"d3n_cache.mountpoint must be an absolute path, got {self.mountpoint!r}"
            )

        if not self.cache_path:
            raise D3NCacheError(
                "d3n_cache.cache_path must be a non-empty string"
            )
        if not self.cache_path.startswith("/"):
            raise D3NCacheError(
                f"d3n_cache.cache_path must be an absolute path, got {self.cache_path!r}"
            )

        if self.size_bytes is not None:
            if not isinstance(self.size_bytes, int) or self.size_bytes <= 0:
                raise D3NCacheError(
                    "d3n_cache.size_bytes must be a positive integer"
                )


def d3n_get_host_devs(
    d3n: D3NCacheSpec, host: str
) -> Tuple[str, int, list[str]]:

    fs_type = d3n.filesystem
    devs = d3n.devices_for_host(host)
    try:
        size_bytes = size_to_bytes(d3n.size)
    except Exception as e:
        raise D3NCacheError(
            f'invalid d3n_cache.size {d3n.size!r}: {e}'
        ) from e

    return fs_type, size_bytes, devs


def d3n_parse_dev_from_path(p: str) -> Optional[str]:
    # expected: /mnt/ceph-d3n/<fsid>/<dev>/rgw_datacache/...

    if not p:
        return None

    parts = [x for x in p.split('/') if x]
    try:
        i = parts.index('ceph-d3n')
    except ValueError:
        return None
    if len(parts) < i + 3:
        return None
    dev = parts[i + 2]
    return f'/dev/{dev}' if dev else None


def d3n_mountpoint(fsid: str, dev: str) -> str:
    dev_key = os.path.basename(dev)
    return f"/mnt/ceph-d3n/{fsid}/{dev_key}"


def d3n_cache_path(mountpoint: str, daemon_id: str) -> str:
    daemon_entity = f"client.rgw.{daemon_id}"
    return os.path.join(mountpoint, "rgw_datacache", daemon_entity)


def d3n_paths(fsid: str, device: str, daemon_id: str) -> tuple[str, str]:
    mp = d3n_mountpoint(fsid, device)
    return mp, d3n_cache_path(mp, daemon_id)
