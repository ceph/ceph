Datum
=====

Datum is a powerful foundation for building clear, composable data-handling code in Ceph.
It provides shared operations for recurring jobs: gathering fragmented payloads, selecting byte
ranges, ordering records, and choosing fields for encoding or diagnostics.

Its standard-library core brings together byte geometry, interval queries, record projections,
comparisons, and traversal. These operations compose naturally with C++ Ranges, while domain
vocabularies connect them to native Ceph buffers and wire formats.

Gateway services, object classes, and storage integrations can build on the same vocabulary and
processing logic, reducing the separate helpers each component needs to maintain. Views,
projections, and explicit ownership help components pass data between operations and avoid
unnecessary copies while retaining control of I/O, transactions, and storage policy.

.. datum-read-plan

.. rubric:: One selection, two ways

With ``<array>`` and ``<ceph-datum.h>`` included, and ``namespace datum = ceph::salad::datum;``,
select bytes from an ordinary Ceph ``buffer::list``:

.. code-block:: cpp

   ceph::buffer::list stored;
   stored.append(ceph::buffer::copy("Miso: m", 7));
   stored.append(ceph::buffer::copy("eow", 3));

   const datum::byte_extent meow {6, 4};
   std::array<char, 4> output {};
   datum::copy(stored, meow, output);
   const ceph::buffer::list retained = datum::slice(stored, meow);

Both operations select ``"meow"`` across the two buffers. ``copy`` writes into the array;
``slice`` returns another Ceph list sharing ownership of the selected payloads, so those bytes
remain alive even after ``stored`` is cleared. An ordinary ``std::span`` would borrow storage
without keeping it alive.

.. rubric:: What can you build?

* **Bytes and buffers:** :doc:`read fragmented payloads <01-hello-fragments>`, select byte ranges,
  and retain storage without first assembling a contiguous buffer.
* **Records and codecs:** :doc:`project record fields <08-toy-basket>` and
  :doc:`encode selected fields <12-postcard>` using shared record declarations.
* **Keys and ordering:** :doc:`build ordered keys <04-numeric-keys>` and compose comparisons
  for standard Ranges algorithms, from a single field to a compound ordering.
* **Traversal and budgets:** :doc:`continue across pages <14-turn-page>` and
  :doc:`account for work <13-three-tickets>` while the application controls fetching and scheduling.

.. rubric:: Domain Vocabulary: Ceph

The shared ``ceph-datum.h`` header connects Datum's byte extents, mappings, and operations to
existing Ceph buffers. :doc:`Bytes with a Ceph accent <18-ceph-bytes>` shows how to map object
positions into buffer storage, copy a selection, and retain native slices.

For worked examples and before-and-after comparisons, explore
:doc:`Datum in practice <in-practice>`: fragmented bytes, missing cache ranges, key selections,
and native Ceph integration.

.. toctree::
   :maxdepth: 1

   Tutorial <tutorial>
   Datum in practice <in-practice>
   Design and reference <design>
   Build and run <build-and-run>
   Complete examples <examples>
   Index <lookup>
   References <references>
