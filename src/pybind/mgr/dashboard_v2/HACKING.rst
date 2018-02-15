Dashboard v2 Developer Documentation
====================================

Frontend Development
--------------------

Before you can start the dashboard from within a development environment,  you
will need to generate the frontend code and either use a compiled and running
Ceph cluster (e.g. started by ``vstart.sh``) or the standalone development web
server.

The build process is based on `Node.js <https://nodejs.org/>`_ and requires the
`Node Package Manager <https://www.npmjs.com/>`_ ``npm`` to be installed.

Prerequisites
~~~~~~~~~~~~~

Run ``npm install`` in directory ``src/pybind/mgr/dashboard_v2/frontend`` to
install the required packages locally.

.. note::

  If you do not have the `Angular CLI <https://github.com/angular/angular-cli>`_
  installed globally, then you need to execute ``ng`` commands with an
  additional ``npm run`` before it.

Setting up a Development Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the ``proxy.conf.json`` file based on ``proxy.conf.json.sample``.

Run ``npm start -- --proxy-config proxy.conf.json`` for a dev server.
Navigate to ``http://localhost:4200/``. The app will automatically
reload if you change any of the source files.

Code Scaffolding
~~~~~~~~~~~~~~~~

Run ``ng generate component component-name`` to generate a new
component. You can also use
``ng generate directive|pipe|service|class|guard|interface|enum|module``.

Build the Project
~~~~~~~~~~~~~~~~~

Run ``npm run build`` to build the project. The build artifacts will be
stored in the ``dist/`` directory. Use the ``-prod`` flag for a
production build. Navigate to ``http://localhost:8080``.

Running Unit Tests
~~~~~~~~~~~~~~~~~~

Run ``npm run test`` to execute the unit tests via `Karma
<https://karma-runner.github.io>`_.

Running End-to-End Tests
~~~~~~~~~~~~~~~~~~~~~~~~

Run ``npm run e2e`` to execute the end-to-end tests via
`Protractor <http://www.protractortest.org/>`__.

Further Help
~~~~~~~~~~~~

To get more help on the Angular CLI use ``ng help`` or go check out the
`Angular CLI
README <https://github.com/angular/angular-cli/blob/master/README.md>`__.

Example of a Generator
~~~~~~~~~~~~~~~~~~~~~~

::

    # Create module 'Core'
    src/app> ng generate module core -m=app --routing

    # Create module 'Auth' under module 'Core'
    src/app/core> ng generate module auth -m=core --routing
    or, alternatively:
    src/app> ng generate module core/auth -m=core --routing

    # Create component 'Login' under module 'Auth'
    src/app/core/auth> ng generate component login -m=core/auth
    or, alternatively:
    src/app> ng generate component core/auth/login -m=core/auth

Frontend Typescript Code Style Guide Recommendations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Group the imports based on its source and separate them with a blank
line.

The source groups can be either from Angular, external or internal.

Example:

.. code:: javascript

    import { Component } from '@angular/core';
    import { Router } from '@angular/router';

    import { ToastsManager } from 'ng2-toastr';

    import { Credentials } from '../../../shared/models/credentials.model';
    import { HostService } from './services/host.service';


Backend Development
-------------------

The Python backend code of this module requires a number of Python modules to be
installed. They are listed in file ``requirements.txt``. Using `pip
<https://pypi.python.org/pypi/pip>`_ you may install all required dependencies
by issuing ``pip install -r requirements.txt`` in directory
``src/pybind/mgr/dashboard_v2``.

If you're using the `ceph-dev-docker development environment
<https://github.com/ricardoasmarques/ceph-dev-docker/>`_, simply run
``./install_deps.sh`` from the toplevel directory to install them.

Unit Testing and Linting
~~~~~~~~~~~~~~~~~~~~~~~~

We included a ``tox`` configuration file that will run the unit tests under
Python 2 or 3, as well as linting tools to guarantee the uniformity of code.

You need to install ``tox`` and ``coverage`` before running it. To install the
packages in your system, either install it via your operating system's package
management tools, e.g. by running ``dnf install python-tox python-coverage`` on
Fedora Linux.

Alternatively, you can use Python's native package installation method::

  $ pip install tox
  $ pip install coverage

The unit tests must run against a real Ceph cluster (no mocks are used). This
has the advantage of catching bugs originated from changes in the internal Ceph
code.

Our ``tox.ini`` script will start a ``vstart`` Ceph cluster before running the
python unit tests, and then it stops the cluster after the tests are run. Of
course this implies that you have built/compiled Ceph previously.

To run tox, run the following command in the root directory (where ``tox.ini``
is located)::

  $ PATH=../../../../build/bin:$PATH tox

We also collect coverage information from the backend code. You can check the
coverage information provided by the tox output, or by running the following
command after tox has finished successfully::

  $ coverage html

This command will create a directory ``htmlcov`` with an HTML representation of
the code coverage of the backend.

You can also run a single step of the tox script (aka tox environment), for
instance if you only want to run the linting tools, do::

  $ PATH=../../../../build/bin:$PATH tox -e lint

How to run a single unit test without using ``tox``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When developing the code of a controller and respective test code, it's useful
to be able to run that single test file without going through the whole ``tox``
workflow.

Since the tests must run against a real Ceph cluster, the first thing is to have
a Ceph cluster running. For that we can leverage the tox environment that starts
a Ceph cluster::

  $ PATH=../../../../build/bin:$PATH tox -e ceph-cluster-start

The command above uses ``vstart.sh`` script to start a Ceph cluster and
automatically enables the ``dashboard_v2`` module, and configures its cherrypy
web server to listen in port ``9865``.

After starting the Ceph cluster we can run our test file using ``py.test`` like
this::

  DASHBOARD_V2_PORT=9865 UNITTEST=true py.test -s tests/test_mycontroller.py

You can run tests multiple times without having to start and stop the Ceph
cluster.

After you finish your tests, you can stop the Ceph cluster using another tox
environment::

  $ tox -e ceph-cluster-stop

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

Every path given in the ``ApiController`` decorator will automatically be
prefixed with ``api``. After reloading the Dashboard module you can access the
above mentioned controller by pointing your browser to
http://mgr_hostname:8080/api/ping2.

It is also possible to have nested controllers. The ``RgwController`` uses
this technique to make the daemons available through the URL
http://mgr_hostname:8080/api/rgw/daemon::

  @ApiController('rgw')
  @AuthRequired()
  class Rgw(RESTController):
    pass


  @ApiController('rgw/daemon')
  @AuthRequired()
  class RgwDaemon(RESTController):

    def list(self):
      pass


Note that paths must be unique and that a path like ``rgw/daemon`` has to have
a parent ``rgw``. Otherwise it won't work.

How does the RESTController work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We also provide a simple mechanism to create REST based controllers using the
``RESTController`` class. Any class which inherits from ``RESTController`` will,
by default, return JSON.

The ``RESTController`` is basically an additional abstraction layer which eases
and unifies the work with collections. A collection is just an array of objects
with a specific type. ``RESTController`` enables some default mappings of
request types and given parameters to specific method names. This may sound
complicated at first, but it's fairly easy. Lets have look at the following
example::

  import cherrypy
  from ..tools import ApiController, RESTController

  @ApiController('ping2')
  class Ping2(RESTController):
    def list(self):
      return {"msg": "Hello"}

    def get(self, id):
      return self.objects[id]

In this case, the ``list`` method is automatically used for all requests to
``api/ping2`` where no additional argument is given and where the request type
is ``GET``. If the request is given an additional argument, the ID in our
case, it won't map to ``list`` anymore but to ``get`` and return the element
with the given ID (assuming that ``self.objects`` has been filled before). The
same applies to other request types:

+--------------+------------+----------------+-------------+
| Request type | Arguments  | Method         | Status Code |
+==============+============+================+=============+
| GET          | No         | list           | 200         |
+--------------+------------+----------------+-------------+
| PUT          | No         | bulk_set       | 200         |
+--------------+------------+----------------+-------------+
| PATCH        | No         | bulk_set       | 200         |
+--------------+------------+----------------+-------------+
| POST         | No         | create         | 201         |
+--------------+------------+----------------+-------------+
| DELETE       | No         | bulk_delete    | 204         |
+--------------+------------+----------------+-------------+
| GET          | Yes        | get            | 200         |
+--------------+------------+----------------+-------------+
| PUT          | Yes        | set            | 200         |
+--------------+------------+----------------+-------------+
| PATCH        | Yes        | set            | 200         |
+--------------+------------+----------------+-------------+
| DELETE       | Yes        | delete         | 204         |
+--------------+------------+----------------+-------------+

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


How to write a unit test when a controller accesses a Ceph module?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Consider the following example that implements a controller that retrieves the
list of RBD images of the ``rbd`` pool::

  import rbd
  from ..tools import ApiController, RESTController


  @ApiController('rbdimages')
  class RbdImages(RESTController):
      def __init__(self):
          self.ioctx = self.mgr.rados.open_ioctx('rbd')
          self.rbd = rbd.RBD()

      def list(self):
          return [{'name': n} for n in self.rbd.list(self.ioctx)]

In the example above, we want to mock the return value of the ``rbd.list``
function, so that we can test the JSON response of the controller.

The unit test code will look like the following::

  import mock
  from .helper import ControllerTestCase


  class RbdImagesTest(ControllerTestCase):
      @mock.patch('rbd.RBD.list')
      def test_list(self, rbd_list_mock):
          rbd_list_mock.return_value = ['img1', 'img2']
          self._get('/api/rbdimages')
          self.assertJsonBody([{'name': 'img1'}, {'name': 'img2'}])
