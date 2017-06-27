
=========================
ceph-mgr dashboard module
=========================

Dependencies
============

On Ubuntu Xenial:

::

    apt-get install python-cherrypy3

On Fedora:

::

    dnf install -y python-cherrypy

    
If you had already enabled the module, restart ceph-mgr after installing dependencies to reload the module.

Enabling
========

Enable the module with::

  ceph mgr module enable dashboard

You can see currently enabled modules with::

  ceph mgr module ls

If you use any other ceph-mgr modules, make sure they're in the list too.

An address where the dashboard will listen on needs to be configured as well, set this to ``::`` to listen on all
IPv4 and IPv6 addresses.

::

    ceph config-key put mgr/dashboard/server_addr ::

Restart the ceph-mgr daemon after modifying the setting to load the module.

Accessing
=========

Point your browser at port 7000 on the server where ceph-mgr is running.

FAQs
====

Q: Aargh there's no authentication!  Are you crazy?
A: At present this module only serves read-only status information, and it is disabled by default.  Administrators can make a decision
   at the point of enabling the module whether they want to limit access to the port, and/or put it behind an authenticating HTTP proxy
   such as Apache+digest auth.
   A motivated person could totally build in some authentication stuff though: that would probably only be worth the effort at the point
   that non-readonly features were added.

Q: Aargh there's no SSL!  Are you crazy?
A: See the authentication question.  You can always configure your own SSL gateway on top of this if you need it.

Q: Why CherryPy?
A: ceph-mgr is a pure CPython environment, which means that anything gevent based doesn't work (gevent relies on C-level hacks).  CherryPy
   includes a web server based on standard python threads, making it a convenient choice.  There are other web frameworks of course.

Q: Does this use the `restful` module?
A: No.  This module loads everything it needs directly via the interfaces available to ceph-mgr modules, and sends it straight to your browser.

Q: My browser says "connection refused"
A: Check that you have a running ceph-mgr daemon.  Look in the ceph-mgr log (/var/log/ceph/ceph-mgr...) for errors loading modules.  Check
   that you have enabled the module (see "Enabling" above)

Q: My log contains an error like "ImportError: No module named cherrypy"
A: See "Dependencies" above.  Make sure you installed a 3.x version of cherrypy, many distros have multiple cherrypy packages.

Q: I want to add something to the UI, how do I do it?
A: See HACKING.rst

