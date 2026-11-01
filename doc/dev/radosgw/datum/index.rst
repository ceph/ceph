Datum
=====

Datum provides C++26 operations for selecting and observing data, from fragmented
bytes and interval sets to record fields and page traversal. Start with small,
runnable examples, then consult the design and reference for the interfaces and
their boundaries. All lessons and examples are provisional.

See :doc:`Psst... wanna buy a codec? <in-practice>` for key ranges, split buffers, and
bucket-index bindings, with code comparisons.

.. toctree::
   :maxdepth: 1

   Tutorial <tutorial>
   Sales pitch <in-practice>
   Design and reference <design>
   Build and run <build-and-run>
   Complete examples <examples>
   Index <lookup>
   References <references>

The RGW lessons are a separate integration preview. Their native adapters are
outside the foundation PR, and their local dependency is explained in
:doc:`build-and-run`.
