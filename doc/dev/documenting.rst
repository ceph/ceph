==================
 Documenting Ceph
==================

Drawing diagrams
================

Graphviz
--------

You can use Graphviz_, as explained in the `Graphviz extension documentation`_.

.. _Graphviz: http://graphviz.org/
.. _`Graphviz extension documentation`: http://sphinx.pocoo.org/ext/graphviz.html

.. graphviz::

   digraph "example" {
     foo -> bar;
     bar -> baz;
     bar -> thud;
   }

Most of the time, you'll want to put the actual DOT source in a
separate file, like this::

  .. graphviz:: myfile.dot


Ditaa
-----

You can use Ditaa_:

.. _Ditaa: http://ditaa.sourceforge.net/

.. ditaa::

   +--------------+   /=----\
   | hello, world |-->| hi! |
   +--------------+   \-----/


Blockdiag
---------

If a use arises, we can integrate Blockdiag_. It is a Graphviz-style
declarative language for drawing things, and includes:

- `block diagrams`_: boxes and arrows (automatic layout, as opposed to
  Ditaa_)
- `sequence diagrams`_: timelines and messages between them
- `activity diagrams`_: subsystems and activities in them
- `network diagrams`_: hosts, LANs, IP addresses etc (with `Cisco
  icons`_ if wanted)

.. _Blockdiag: http://blockdiag.com/
.. _`Cisco icons`: http://pypi.python.org/pypi/blockdiagcontrib-cisco/
.. _`block diagrams`: http://blockdiag.com/en/blockdiag/
.. _`sequence diagrams`: http://blockdiag.com/en/seqdiag/index.html
.. _`activity diagrams`: http://blockdiag.com/en/actdiag/index.html
.. _`network diagrams`: http://blockdiag.com/en/nwdiag/
