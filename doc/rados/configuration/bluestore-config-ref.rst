==========================
BlueStore Config Reference
==========================

Inline Compression
==================

BlueStore supports inline compression using snappy, zlib, or LZ4. Please note,
the lz4 compression plugin is not distributed in the official release.

``bluestore compression algorithm``

:Description: The default compressor to use (if any) if the per-pool property
              ``compression_algorithm`` is not set. Note that zstd is *not*
              recommended for bluestore due to high CPU overhead when
              compressing small amounts of data.
:Type: String
:Required: No
:Valid Settings: ``lz4``, ``snappy``, ``zlib``, ``zstd``
:Default: ``snappy``

``bluestore compression mode``

:Description: The default policy for using compression if the per-pool property
              ``compression_mode`` is not set. ``none`` means never use
              compression.  ``passive`` means use compression when
              `clients hint`_ that data is compressible.  ``aggressive`` means
              use compression unless clients hint that data is not compressible.
              ``force`` means use compression under all circumstances even if
              the clients hint that the data is not compressible.
:Type: String
:Required: No
:Valid Settings: ``none``, ``passive``, ``aggressive``, ``force``
:Default: ``none``

``bluestore compression min blob size``

:Description: Chunks smaller than this are never compressed.
              The per-pool property ``compression_min_blob_size`` overrides
              this setting.

:Type: Unsigned Integer
:Required: No
:Default: 0

``bluestore compression min blob size hdd``

:Description: Default value of ``bluestore compression min blob size``
              for rotational media.

:Type: Unsigned Integer
:Required: No
:Default: 128K

``bluestore compression min blob size ssd``

:Description: Default value of ``bluestore compression min blob size``
              for non-rotational (solid state) media.

:Type: Unsigned Integer
:Required: No
:Default: 8K

``bluestore compression max blob size``

:Description: Chunks larger than this are broken into smaller blobs sizing
              ``bluestore compression max blob size`` before being compressed.
              The per-pool property ``compression_max_blob_size`` overrides
              this setting.

:Type: Unsigned Integer
:Required: No
:Default: 0

``bluestore compression max blob size hdd``

:Description: Default value of ``bluestore compression max blob size``
              for rotational media.

:Type: Unsigned Integer
:Required: No
:Default: 512K

``bluestore compression max blob size ssd``

:Description: Default value of ``bluestore compression max blob size``
              for non-rotational (solid state) media.

:Type: Unsigned Integer
:Required: No
:Default: 64K

.. _clients hint: ../../api/librados/#rados_set_alloc_hint
