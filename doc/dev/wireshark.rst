=====================
 Wireshark Dissector
=====================

Wireshark has support for the Ceph protocol and it will be shipped in the 1.12.1
release.

Using
=====

To use the Wireshark dissector you must build it from `git`__, the process is
outlined in great detail in the `Building and Installing`__ section of the
`Wireshark Users Guide`__.

__ `Wireshark git`_
__ WSUG_BI_
__ WSUG_

Developing
==========

The Ceph dissector lives in `Wireshark git`_ at
``epan/dissectors/packet-ceph.c``.  At the top of that file there are some
comments explaining how to insert new functionality or to update the encoding
of existing types.

Before you start hacking on Wireshark code you should look at the
``doc/README.developer`` and ``doc/README.dissector`` documents as they explain
the basics of writing dissectors.  After reading those two documents you should
be prepared to work on the Ceph dissector.  `The Wireshark
developers guide`__ also contains a lot of useful information but it is less
directed and is more useful as a reference then an introduction.

__ WSDG_

.. _WSUG: https://www.wireshark.org/docs/wsug_html_chunked/
.. _WSDG: https://www.wireshark.org/docs/wsdg_html_chunked/
.. _WSUG_BI: https://www.wireshark.org/docs/wsug_html_chunked/ChapterBuildInstall.html
.. _Wireshark git: https://www.wireshark.org/develop.html

.. vi: textwidth=80 noexpandtab
