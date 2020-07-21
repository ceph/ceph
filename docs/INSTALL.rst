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
install dependencies. The instructions are given below::

    git clone https://github.com/ceph/teuthology/
    cd teuthology
    virtualenv --python python3 ./virtualenv
    source virtualenv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    python setup.py develop


Teuthology in PyPI
------------------

However if you prefer, you may install ``teuthology`` from `PyPI <http://pypi.python.org>`__::

    pip install teuthology


**Note**: The version in PyPI can be (*far*) behind the development version.

Or from GitHub::

    pip install git+https://github.com/ceph/teuthology#egg=teuthology[orchestra]

where the dependencies for orchestrating are installed. They are used for
interacting with the services to schedule tests and to report the test results.


Update Dependencies
-------------------

We track the dependencies using ``requirements.txt``. These packages are
tested, and should work with teuthology. But if you want to bump up the
versions of them, please use the following command to update these files::

  ./update-requirements.sh -P <package-name>

Please upgrade pip-tool using following command ::

  pip install pip-tools --upgrade

if the command above fails like::

  Traceback (most recent call last):
  File "/home/kchai/teuthology/virtualenv/bin/pip-compile", line 5, in <module>
    from piptools.scripts.compile import cli
  File "/home/kchai/teuthology/virtualenv/local/lib/python2.7/site-packages/piptools/scripts/compile.py", line 11, in <module>
    from pip.req import InstallRequirement, parse_requirements
  ImportError: No module named req

Add Dependencies
----------------

td,dr: please add the new dependencies in both ``setup.py`` and
``requirements.in``.

We also use ``pip install <URL>`` to install teuthology in some Ceph's unit
tests. To cater their needs, some requirements are listed in ``setup.py`` as
well, so that ``pip install`` can pick them up. We could just avoid duplicating
the packages specifications in two places by putting::

  -e .[orchestra,test]

in ``requirements.in``. But dependabot includes::

  -e file:///home/dependabot/dependabot-updater/tmp/dependabot_20200617-72-1n8af4b  # via -r requirements.in

in the generated ``requirements.txt``. This renders the created pull request
useless without human intervention. To appease dependabot, a full-blown
``requirements.in`` collecting all direct dependencies listed by ``setup.py``
is used instead.
