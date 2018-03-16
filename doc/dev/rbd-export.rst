RBD Export & Import
===================

This is a file format of an RBD image or snapshot. It's a sparse format
for the full image. There are three recording sections in the file.

(1) Header.
(2) Metadata.
(3) Diffs.

Header
~~~~~~

"rbd image v2\\n"

Metadata records
~~~~~~~~~~~~~~~~

Every record has a one byte "tag" that identifies the record type,
followed by length of data, and then some other data.

Metadata records come in the first part of the image.  Order is not
important, as long as all the metadata records come before the data
records.

In v2, we have the following metadata in each section:
(1 Bytes) tag.
(8 Bytes) length.
(n Bytes) data.

In this way, we can skip the unrecognized tag.

Image order
-----------

- u8: 'O'
- le64: length of appending data (8)
- le64: image order

Image format
------------

- u8: 'F'
- le64: length of appending data (8)
- le64: image format

Image Features
--------------

- u8: 'T'
- le64: length of appending data (8)
- le64: image features

Image Stripe unit
-----------------

- u8: 'U'
- le64: length of appending data (8)
- le64: image striping unit

Image Stripe count
------------------

- u8: 'C'
- le64: length of appending data (8)
- le64: image striping count

ImageMeta Key and Value
-----------------------

- u8: 'M'
- le64: length of appending data (length of key + length of value + 4 * 2)
- string: image-meta key
- string: image-meta value

Final Record
~~~~~~~~~~~~

End
---

- u8: 'E'


Diffs records
~~~~~~~~~~~~~

Record the all snapshots and the HEAD in this section. 

Snap Protection status
----------------------

Record the snapshot's protection status if `--export-format=2`.
- u8: 'p'
- le64: length of appending data (8)
- u8: snap protection status (0 for false, 1 for true)

Others
------

- le64: number of diffs
- Diffs ...

Detail please refer to rbd-diff.rst
