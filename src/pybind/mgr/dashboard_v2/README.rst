Dashboard and Administration Module for Ceph Manager (aka "Dashboard v2")
=========================================================================

Overview
--------

The original Ceph Manager Dashboard started out as a simple read-only view into
various run-time information and performance data of a Ceph cluster.

However, there is a `growing demand <http://pad.ceph.com/p/mimic-dashboard>`_
for adding more web-based management capabilities, to make it easier for
administrators that prefer a WebUI over the command line.

This module is an ongoing project to add a native web based monitoring and
administration application to Ceph Manager. It aims at becoming a successor of
the existing dashboard, which provides read-only functionality and uses a
simpler architecture to achieve the original goal.

The code and architecture of this module is derived from and inspired by the
`openATTIC Ceph management and monitoring tool <https://openattic.org/>`_ (both
the backend and WebUI). The development is actively driven by the team behind
openATTIC.

The intention is to reuse as much of the existing openATTIC code as possible,
while adapting it to the different environment. The current openATTIC backend
implementation is based on Django and the Django REST framework, the Manager
module's backend code will use the CherryPy framework and a custom REST API
implementation instead.

The WebUI implementation will be developed using Angular/TypeScript, merging
both functionality from the existing dashboard as well as adding new
functionality originally developed for the standalone version of openATTIC.

The porting and migration of the existing openATTIC and dashboard functionality
will be done in stages. The tasks are currently tracked in the `openATTIC team's
JIRA instance <https://tracker.openattic.org/browse/OP-3039>`_.

Enabling and Starting the Dashboard
-----------------------------------

The Python backend code of this module requires a number of Python modules to be
installed. They are listed in file ``requirements.txt``.  Using `pip
<https://pypi.python.org/pypi/pip>`_ you may install all required dependencies
by issuing ``pip -r requirements.txt``.

If you're using the `ceph-dev-docker development environment
<https://github.com/ricardoasmarques/ceph-dev-docker/>`_, simply run
``./install_deps.sh`` from the current directory to install them.

Start the Dashboard module by running::

  $ ceph mgr module enable dashboard_v2

You can see currently enabled modules with::

  $ ceph mgr module ls

Currently you will need to manually generate the frontend code.
Instructions can be found in `./frontend/README.md`.

In order to be able to log in, you need to define a username and password, which
will be stored in the MON's configuration database::

  $ ceph dashboard set-login-credentials <username> <password>

The password will be stored as a hash using ``bcrypt``.

The WebUI should then be reachable on TCP port 8080.

Unit Testing and Linting
------------------------

We included a ``tox`` configuration file that will run the unit tests under
Python 2 and 3, as well as linting tools to guarantee the uniformity of code.

You need to install ``tox`` before running it. To install ``tox`` in your
system, either install it via your operating system's package management
tools, e.g. by running ``dnf install python3-tox`` on Fedora Linux.

Alternatively, you can use Python's native package installation method::

  $ pip install tox

To run tox, run the following command in the root directory (where ``tox.ini``
is located)::

  $ tox


If you just want to run a single tox environment, for instance only run the
linting tools::

  $ tox -e lint

Developer Notes
---------------

How to add a new controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to add a new endpoint to the backend, you just need to add a
class derived from ``BaseController`` decorated with ``ApiController`` in a
Python file located under the ``controllers`` directory. The Dashboard module
will automatically load your new controller upon start.

For example create a file ``ping2.py`` under ``controllers`` directory with the
following code::

  import cherrypy
  from ..tools import ApiController, BaseController

  @ApiController('ping2')
  class Ping2(BaseController):
    @cherrypy.expose
    def default(self, *args):
      return "Hello"

Reload the Dashboard module and then you can access the above controller from
the web browser using the URL http://mgr_hostname:8080/api/ping2.

We also provide a simple mechanism to create REST based controllers using the
``RESTController`` class.

For example, we can adapt the above controller to return JSON when accessing
the endpoint with a GET request::

  import cherrypy
  from ..tools import ApiController, RESTController

  @ApiController('ping2')
  class Ping2(RESTController):
    def list(self):
      return {"msg": "Hello"}


How to restrict access to a controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you require that only authenticated users can access you controller, just
add the ``AuthRequired`` decorator to your controller class.

Example::

  import cherrypy
  from ..tools import ApiController, AuthRequired, RESTController

  @ApiController('ping2')
  @AuthRequired()
  class Ping2(RESTController):
    def list(self):
      return {"msg": "Hello"}

Now only authenticated users will be able to "ping" your controller.


How to access the manager module instance from a controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each controller class derived from ``BaseController``has a class property that
points to the manager module global instance. The property is named ``mgr``.
There is another class property called ``logger`` to easily add log messages.

Example::

  import cherrypy
  from ..tools import ApiController, RESTController

  @ApiController('servers')
  class Servers(RESTController):
    def list(self):
      self.logger.debug('Listing available servers')
      return {'servers': self.mgr.list_servers()}


How to write a unit test for a controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We provide a test helper class called ``ControllerTestCase`` to easily create
unit tests for your controller.

If we want to write a unit test for the above ``Ping2`` controller, create a
``test_ping2.py`` file under the ``tests`` directory with the following code::

  from .helper import ControllerTestCase
  from .controllers.ping2 import Ping2

  class Ping2Test(ControllerTestCase):
      @classmethod
      def setup_test(cls):
          Ping2._cp_config['tools.authentica.on'] = False

      def test_ping2(self):
          self._get("/api/ping2")
          self.assertStatus(200)
          self.assertJsonBody({'msg': 'Hello'})

The ``ControllerTestCase`` class will call the dashboard module code that loads
the controllers and initializes the CherryPy webserver. Then it will call the
``setup_test()`` class method to execute additional instructions that each test
case needs to add to the test.
In the example above we use the ``setup_test()`` method to disable the
authentication handler for the ``Ping2`` controller.


How to listen for manager notifications in a controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The manager notifies the modules of several types of cluster events, such
as cluster logging event, etc...

Each module has a "global" handler function called ``notify`` that the manager
calls to notify the module. But this handler function must not block or spend
too much time processing the event notification.
For this reason we provide a notification queue that controllers can register
themselves with to receive cluster notifications.

The example below represents a controller that implements a very simple live
log viewer page::

  from __future__ import absolute_import

  import collections

  import cherrypy

  from ..tools import ApiController, BaseController, NotificationQueue


  @ApiController('livelog')
  class LiveLog(BaseController):
      log_buffer = collections.deque(maxlen=1000)

      def __init__(self):
          super(LiveLog, self).__init__()
          NotificationQueue.register(self.log, 'clog')

      def log(self, log_struct):
          self.log_buffer.appendleft(log_struct)

      @cherrypy.expose
      def default(self):
          ret = '<html><meta http-equiv="refresh" content="2" /><body>'
          for l in self.log_buffer:
              ret += "{}<br>".format(l)
          ret += "</body></html>"
          return ret

As you can see above, the ``NotificationQueue`` class provides a register
method that receives the function as its first argument, and receives the
"notification type" as the second argument.
You can omit the second argument of the ``register`` method, and in that case
you are registering to listen all notifications of any type.

Here is an list of notification types (these might change in the future) that
can be used:

* ``clog``: cluster log notifications
* ``command``: notification when a command issued by ``MgrModule.send_command``
  completes
* ``perf_schema_update``: perf counters schema update
* ``mon_map``: monitor map update
* ``fs_map``: cephfs map update
* ``osd_map``: OSD map update
* ``service_map``: services (RGW, RBD-Mirror, etc.) map update
* ``mon_status``: monitor status regular update
* ``health``: health status regular update
* ``pg_summary``: regular update of PG status information

