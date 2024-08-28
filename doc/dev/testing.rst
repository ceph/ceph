Testing notes
=============


build-integration-branch
------------------------

Setup
^^^^^

#. Create a github token at `<https://github.com/settings/tokens>`_
   and put it in ``~/.github_token``.  Note that only the
   ``public_repo`` under the ``repo`` section needs to be checked.

#. Create a ceph repo label `wip-yourname-testing` if you don't
   already have one at `<https://github.com/ceph/ceph/labels>`_.

#. Create the ``ci`` remote::

     git remote add ci git@github.com:ceph/ceph-ci

Using
^^^^^

#. Tag some subset of `needs-qa` commits with your label (usually `wip-yourname-testing`).

#. Create the integration branch::

     git checkout master
     git pull
     ../src/script/build-integration-branch wip-yourname-testing

#. Smoke test::

     ./run-make-check.sh

#. Push to ceph-ci::

     git push ci $(git rev-parse --abbrev-ref HEAD)

