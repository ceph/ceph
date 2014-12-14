==========================
Teuthology Lab Setup Notes
==========================

Introduction
============

We recently set up a new lab for Ceph testing and decided to document the parts of the process that are most relevant to teuthology. This is the result.

We started by setting aside two of the test machines: one as the 'teuthology node', and another as the 'paddles/pulpito node'. These would be used to orchestrate automated testing and to store and serve the results on our intranet.

paddles/pulpito node
====================

We're currently running both paddles and pulpito on the same node. We have a proxy server up front listening on port 80 that forwards to the proper service based on which hostname is used. Feel free to modify our `paddles <_static/nginx_paddles>`_ and `pulpito <_static/nginx_pulpito>`_ configurations for your use.

Do the following as root or as another user with sudo access::

    sudo apt-get install git python-dev python-virtualenv postgresql postgresql-contrib postgresql-server-dev-all supervisor
    sudo -u postgres createuser paddles -P
    sudo -u postgres createdb paddles

Create a separate user for paddles and puplito. We used 'paddles' and 'pulpito'.


paddles
-------
Follow instructions at https://github.com/ceph/paddles/blob/master/README.rst


pulpito
-------
Follow instructions at https://github.com/ceph/pulpito/blob/master/README.rst


Starting up
-----------

Back as the 'root or sudo' user::

    sudo cp ~paddles/paddles/supervisord_paddles.conf /etc/supervisor/conf.d/paddles.conf
    sudo supervisorctl reread && sudo supervisorctl update paddles && sudo supervisorctl start paddles
    sudo cp ~pulpito/pulpito/supervisord_pulpito.conf /etc/supervisor/conf.d/pulpito.conf
    sudo supervisorctl reread && sudo supervisorctl update pulpito && sudo supervisorctl start pulpito


Test Nodes
==========

Each node needs to have a user named 'ubuntu' with passwordless sudo access.

It's also necessary to generate an ssh key pair that will be used to provide
passwordless authentication to all the test nodes, and put the public key in
``~/.ssh/authorized_keys`` on all the test nodes.


Teuthology Node
===============

Create an ``/etc/teuthology.yaml`` that looks like::

    lab_domain: example.com
    lock_server: http://pulpito.example.com:8080
    results_server: http://pulpito.example.com:8080
    queue_host: localhost
    queue_port: 11300
    results_email: you@example.com
    archive_base: /home/teuthworker/archive

Do the following as root or as another user with sudo access:

Create two additional users: one that simply submits jobs to the queue, and
another that picks them up from the queue and executes them. We use
'teuthology' and 'teuthworker', respectively.

Give both users passwordless sudo access.

Copy the ssh key pair that you created to access the test nodes into each of
these users' ``~/.ssh`` directory.

Install these packages::

    sudo apt-get -y install git python-dev python-pip python-virtualenv libevent-dev python-libvirt beanstalkd

Now, set up the two users you just created:


Scheduler
---------
As 'teuthology', do the following::

    mkdir ~/src
    git clone https://github.com/ceph/teuthology.git src/teuthology_master
    pushd src/teuthology_master/
    ./bootstrap
    popd


Worker
------
As 'teuthworker', do the following::

    mkdir ~/src
    git clone https://github.com/ceph/teuthology.git src/teuthology_master
    pushd src/teuthology_master/
    ./bootstrap
    popd
    mkdir ~/bin
    wget -O ~/bin/worker_start https://raw.githubusercontent.com/ceph/teuthology/master/docs/_static/worker_start.sh
    echo 'PATH="$HOME/src/teuthology_master/virtualenv/bin:$PATH"' >> ~/.profile
    source ~/.profile
    mkdir -p ~/archive/worker_logs
    worker_start magna 1


Submitting Nodes
================

First::

    wget https://raw.githubusercontent.com/ceph/teuthology/master/docs/_static/create_nodes.py

Edit ``create_nodes.py`` to generate the hostnames of the machines you want to submit to paddles.

Now to do the work::

    python create_nodes.py
    teuthology-lock --owner initial@setup --list-targets > /tmp/targets
    teuthology --owner initial@setup /tmp/targets
    teuthology-lock --owner initial@setup --unlock -t /tmp/targets


Serving Test Logs
=================

pulpito tries to provide links to test logs. Out-of-the-box, those links will be broken, but are easy to fix. 

First, install your favorite web server on the teuthology node. If you use nginx, you may use `our configuration <_static/nginx_test_logs>`_ as a template.

Once you've got log files being served, edit paddles' ``config.py`` and update the ``job_log_href_templ`` value. Restart paddles when you're done.
