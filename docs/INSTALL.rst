.. _installation_and_setup:

Installation and setup
======================

Ubuntu, Fedora & SUSE/openSUSE
------------------------------

A bootstrap script is provided that will do everything for you assuming
you have ``sudo``::

    ./bootstrap

MacOS X
-------

The ``bootstrap`` script was recently updated to support MacOS X using `homebrew <http://brew.sh/>`_::

    ./bootstrap

**Note**: Certain features might not work properly on MacOS X. Patches are
encouraged, but it has never been a goal of ours to run a full ``teuthology``
setup on a Mac.

Other operating systems
-----------------------

Patches are welcomed to add ``bootstrap`` support for other operating systems. Until then, manual installs are possible

First install the non-PyPI dependencies::

    python-dev python-pip python-virtualenv libevent-dev python-libvirt

Next, clone its `git repository <https://github.com/ceph/teuthology/>`__,
create a `virtualenv <http://virtualenv.readthedocs.org/en/latest/>`__, and
install dependencies::

    git clone https://github.com/ceph/teuthology/
    cd teuthology
    virtualenv ./virtualenv
    source virtualenv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    python setup.py develop


Teuthology in PyPI
------------------

However if you prefer, you may install ``teuthology`` from `PyPI <http://pypi.python.org>`__::

    pip install teuthology


**Note**: The version in PyPI can be (*far*) behind the development version.

