import os
import tempfile
import uuid

from py_tests.internal import exception
from py_tests.internal import utils
from py_tests.internal import rbd_image
from py_tests.internal import unittest_base


class TestImportExport(unittest_base.TestBase):
    _image_size = 32 << 20
    _image_prefix = 'cephImportTest-'

    @classmethod
    def _generate_img_name(cls):
        return cls._image_prefix + str(uuid.uuid4()).split('-')[-1]

    def test_bdev_export_import(self):
        fd, import_path = tempfile.mkstemp(prefix=self._image_prefix)
        os.close(fd)
        self.addCleanup(os.remove, import_path)
        utils.generate_random_file(import_path, self._image_size)

        # Map the rbd image and then import the resulting block device
        img = rbd_image.RbdImage.import_image(self._generate_img_name(), import_path)
        self.addCleanup(img.cleanup)
        img.map()

        bdev_imported_img = rbd_image.RbdImage.import_image(
            self._generate_img_name(), img.path)
        self.addCleanup(bdev_imported_img.cleanup)

        # Create a new image, map it and then export our image there.
        bdev_exported_img = rbd_image.RbdImage.create(
            self._generate_img_name(),
            size_mb=self._image_size >> 20)
        self.addCleanup(bdev_exported_img.cleanup)
        bdev_exported_img.map()

        bdev_imported_img.export_image(bdev_exported_img.path)
        bdev_imported_img.map()

        # validate mapped disk size
        self.assertEqual(self._image_size, img.get_disk_size())
        self.assertEqual(self._image_size, bdev_imported_img.get_disk_size())
        self.assertEqual(self._image_size, bdev_exported_img.get_disk_size())

        # validate contents
        file_checksum = utils.checksum(import_path)
        img_checksum = utils.checksum(img.path)
        bdev_exported_checksum = utils.checksum(bdev_exported_img.path)
        bdev_imported_checksum = utils.checksum(bdev_imported_img.path)

        self.assertEqual(file_checksum, img_checksum)
        self.assertEqual(file_checksum, bdev_exported_checksum)
        self.assertEqual(file_checksum, bdev_imported_checksum)

    def test_bdev_v2_import_export(self):
        # v2 exports are not allowed with Windows block devices
        img = rbd_image.RbdImage.create(
            self._generate_img_name(),
            size_mb=self._image_size >> 20)
        self.addCleanup(img.cleanup)
        img.map()

        self.assertRaises(
            exception.CommandFailed,
            rbd_image.RbdImage.import_image,
            self._generate_img_name(),
            img.path,
            export_format=2)

        img2 = rbd_image.RbdImage.create(
            self._generate_img_name(),
            size_mb=self._image_size >> 20)
        self.addCleanup(img2.cleanup)
        img2.map()

        self.assertRaises(
            exception.CommandFailed,
            img.export_image,
            img2.path,
            export_format=2)
