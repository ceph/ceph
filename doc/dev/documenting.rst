==================
 Documenting Ceph
==================

User documentation
==================

The documentation on docs.ceph.com is generated from the restructuredText
sources in ``/doc/`` in the Ceph git repository.

Please make sure that your changes are written in a way that is intended
for end users of the software, unless you are making additions in
``/doc/dev/``, which is the section for developers.

All pull requests that modify user-facing functionality must
include corresponding updates to documentation: see 
`Submitting Patches`_ for more detail.

Check your .rst syntax is working as expected by using the "View"
button in the github user interface when looking at a diff on
an .rst file, or build the docs locally using the ``admin/build-doc``
script.

For more information about the Ceph documentation, see 
:doc:`/start/documenting-ceph`.

Code Documentation
==================

C and C++ can be documented with Doxygen_, using the subset of Doxygen
markup supported by Breathe_.

.. _Doxygen: http://www.doxygen.nl/
.. _Breathe: https://github.com/michaeljones/breathe

The general format for function documentation is::

  /**
   * Short description
   *
   * Detailed description when necessary
   *
   * preconditons, postconditions, warnings, bugs or other notes
   *
   * parameter reference
   * return value (if non-void)
   */

This should be in the header where the function is declared, and
functions should be grouped into logical categories. The `librados C
API`_ provides a complete example. It is pulled into Sphinx by
`librados.rst`_, which is rendered at :doc:`/rados/api/librados`.

.. _`librados C API`: https://github.com/ceph/ceph/blob/master/src/include/rados/librados.h
.. _`librados.rst`: https://github.com/ceph/ceph/raw/master/doc/rados/api/librados.rst

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
     bar -> th
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

.. _Blockdiag: http://blockdiag.com/en/
.. _`Cisco icons`: https://pypi.org/project/blockdiagcontrib-cisco/
.. _`block diagrams`: http://blockdiag.com/en/blockdiag/
.. _`sequence diagrams`: http://blockdiag.com/en/seqdiag/index.html
.. _`activity diagrams`: http://blockdiag.com/en/actdiag/index.html
.. _`network diagrams`: http://blockdiag.com/en/nwdiag/


Inkscape
--------

You can use Inkscape to generate scalable vector graphics.
https://inkscape.org/en/ for restructedText documents.

If you generate diagrams with Inkscape, you should
commit both the Scalable Vector Graphics (SVG) file and export a
Portable Network Graphic (PNG) file. Reference the PNG file.

By committing the SVG file, others will be able to update the
SVG diagrams using Inkscape.

HTML5 will support SVG inline.

.. _`Submitting Patches`: https://github.com/ceph/ceph/blob/master/SubmittingPatches.rst
