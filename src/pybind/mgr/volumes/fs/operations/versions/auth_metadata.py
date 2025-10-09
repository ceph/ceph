from contextlib import contextmanager
import errno
import os
import fcntl
import json
import logging
import struct
import uuid

import cephfs

from ..group import Group
from ...exception import VolumeException

log = logging.getLogger(__name__)


FILE_NAME_MAX = 255


class AuthMetadataError(Exception):
    pass


class AuthMetadataManager(object):

    # Current version
    version = 6

    # Filename extensions for meta files.
    META_FILE_EXT = ".meta"
    DEFAULT_VOL_PREFIX = "/volumes"

    def __init__(self, fs):
        self.fs = fs
        self._id = struct.unpack(">Q", uuid.uuid1().bytes[0:8])[0]
        self.volume_prefix = self.DEFAULT_VOL_PREFIX

    def _to_bytes(self, param):
        '''
        Helper method that returns byte representation of the given parameter.
        '''
        if isinstance(param, str):
            return param.encode('utf-8')
        elif param is None:
            return param
        else:
            return str(param).encode('utf-8')

    def _subvolume_metadata_path(self, group_name, subvol_name):
        group_name = group_name if group_name != Group.NO_GROUP_NAME else ""
        metadata_filename = f'_{group_name}:{subvol_name}{self.META_FILE_EXT}'
        # in order to allow creation of this metadata file, 5 chars for ".meta"
        # extension and 2 chars for "_" and ":" respectively. so that combina-
        # -tion of group and subvolname should be shorter than 248 chars.
        if len(metadata_filename) > FILE_NAME_MAX:
            raise VolumeException(-errno.ENAMETOOLONG,
                                 'use shorter group or subvol name, '
                                 'combination of both should be less '
                                 'than 249 characters')
        return os.path.join(self.volume_prefix, metadata_filename)

    def _check_compat_version(self, compat_version):
        if self.version < compat_version:
            msg = ("The current version of AuthMetadataManager, version {0} "
                   "does not support the required feature. Need version {1} "
                   "or greater".format(self.version, compat_version)
                   )
            log.error(msg)
            raise AuthMetadataError(msg)

    def _metadata_get(self, path):
        """
        Return a deserialized JSON object, or None
        """
        fd = self.fs.open(path, "r")
        # TODO iterate instead of assuming file < 4MB
        read_bytes = self.fs.read(fd, 0, 4096 * 1024)
        self.fs.close(fd)
        if read_bytes:
            return json.loads(read_bytes.decode())
        else:
            return None

    def _metadata_set(self, path, data):
        serialized = json.dumps(data)
        fd = self.fs.open(path, "w")
        try:
            self.fs.write(fd, self._to_bytes(serialized), 0)
            self.fs.fsync(fd, 0)
        finally:
            self.fs.close(fd)

    def _lock(self, path):
        @contextmanager
        def fn():
            while(1):
                fd = self.fs.open(path, os.O_CREAT, 0o755)
                self.fs.flock(fd, fcntl.LOCK_EX, self._id)

                # The locked file will be cleaned up sometime. It could be
                # unlinked by consumer e.g., an another manila-share service
                # instance, before lock was applied on it. Perform checks to
                # ensure that this does not happen.
                try:
                    statbuf = self.fs.stat(path)
                except cephfs.ObjectNotFound:
                    self.fs.close(fd)
                    continue

                fstatbuf = self.fs.fstat(fd)
                if statbuf.st_ino == fstatbuf.st_ino:
                    break

            try:
                yield
            finally:
                self.fs.flock(fd, fcntl.LOCK_UN, self._id)
                self.fs.close(fd)

        return fn()

    def _auth_metadata_path(self, auth_id):
        return os.path.join(self.volume_prefix, "${0}{1}".format(
            auth_id, self.META_FILE_EXT))

    def auth_lock(self, auth_id):
        return self._lock(self._auth_metadata_path(auth_id))

    def auth_metadata_get(self, auth_id):
        """
        Call me with the metadata locked!

        Check whether a auth metadata structure can be decoded by the current
        version of AuthMetadataManager.

        Return auth metadata that the current version of AuthMetadataManager
        can decode.
        """
        auth_metadata = self._metadata_get(self._auth_metadata_path(auth_id))

        if auth_metadata:
            self._check_compat_version(auth_metadata['compat_version'])

        return auth_metadata

    def auth_metadata_set(self, auth_id, data):
        """
        Call me with the metadata locked!

        Fsync the auth metadata.

        Add two version attributes to the auth metadata,
        'compat_version', the minimum AuthMetadataManager version that can
        decode the metadata, and 'version', the AuthMetadataManager version
        that encoded the metadata.
        """
        data['compat_version'] = 6
        data['version'] = self.version
        return self._metadata_set(self._auth_metadata_path(auth_id), data)

    def create_subvolume_metadata_file(self, group_name, subvol_name):
        """
        Create a subvolume metadata file, if it does not already exist, to store
        data about auth ids having access to the subvolume
        """
        fd = self.fs.open(self._subvolume_metadata_path(group_name, subvol_name),
                          os.O_CREAT, 0o755)
        self.fs.close(fd)

    def delete_subvolume_metadata_file(self, group_name, subvol_name):
        vol_meta_path = self._subvolume_metadata_path(group_name, subvol_name)
        try:
            self.fs.unlink(vol_meta_path)
        except cephfs.ObjectNotFound:
            pass

    def subvol_metadata_lock(self, group_name, subvol_name):
        """
        Return a ContextManager which locks the authorization metadata for
        a particular subvolume, and persists a flag to the metadata indicating
        that it is currently locked, so that we can detect dirty situations
        during recovery.

        This lock isn't just to make access to the metadata safe: it's also
        designed to be used over the two-step process of checking the
        metadata and then responding to an authorization request, to
        ensure that at the point we respond the metadata hasn't changed
        in the background.  It's key to how we avoid security holes
        resulting from races during that problem ,
        """
        return self._lock(self._subvolume_metadata_path(group_name, subvol_name))

    def subvol_metadata_get(self, group_name, subvol_name):
        """
        Call me with the metadata locked!

        Check whether a subvolume metadata structure can be decoded by the current
        version of AuthMetadataManager.

        Return a subvolume_metadata structure that the current version of
        AuthMetadataManager can decode.
        """
        subvolume_metadata = self._metadata_get(self._subvolume_metadata_path(group_name, subvol_name))

        if subvolume_metadata:
            self._check_compat_version(subvolume_metadata['compat_version'])

        return subvolume_metadata

    def subvol_metadata_set(self, group_name, subvol_name, data):
        """
        Call me with the metadata locked!

        Add two version attributes to the subvolume metadata,
        'compat_version', the minimum AuthMetadataManager version that can
        decode the metadata and 'version', the AuthMetadataManager version
        that encoded the metadata.
        """
        data['compat_version'] = 1
        data['version'] = self.version
        return self._metadata_set(self._subvolume_metadata_path(group_name, subvol_name), data)
