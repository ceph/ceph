Dashboard Developer Documentation
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

 * Node 6.9.0 or higher
 * NPM 3 or higher

nodeenv:
  During Ceph's build we create a virtualenv with ``node`` and ``npm``
  installed, which can be used as an alternative to installing node/npm in your
  system.

  If you want to use the node installed in the virtualenv you just need to
  activate the virtualenv before you run any npm commands. To activate it run
  ``. build/src/pybind/mgr/dashboard/node-env/bin/activate``.

  Once you finish, you can simply run ``deactivate`` and exit the virtualenv.

Angular CLI:
  If you do not have the `Angular CLI <https://github.com/angular/angular-cli>`_
  installed globally, then you need to execute ``ng`` commands with an
  additional ``npm run`` before it.

Package installation
~~~~~~~~~~~~~~~~~~~~

Run ``npm install`` in directory ``src/pybind/mgr/dashboard/frontend`` to
install the required packages locally.

Setting up a Development Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the ``proxy.conf.json`` file based on ``proxy.conf.json.sample``.

Run ``npm start`` for a dev server.
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
production build. Navigate to ``https://localhost:8443``.

Running Unit Tests
~~~~~~~~~~~~~~~~~~

Run ``npm run test`` to execute the unit tests via `Karma
<https://karma-runner.github.io>`_.

Running End-to-End Tests
~~~~~~~~~~~~~~~~~~~~~~~~

We use `Protractor <http://www.protractortest.org/>`__ to run our frontend e2e
tests.

Our ``run-frontend-e2e-tests.sh`` script will check if Chrome or Docker is
installed and run the tests if either is found.

Start all frontend e2e tests by running::

  $ ./run-frontend-e2e-tests.sh

Device:
  You can force the script to use a specific device with the ``-d`` flag::

    $ ./run-frontend-e2e-tests.sh -d <chrome|docker>

Remote:
  If you want to run the tests outside the ceph environment, you will need to
  manually define the dashboard url using ``-r``::

    $ ./run-frontend-e2e-tests.sh -r <DASHBOARD_URL>

Note:
  When using docker, as your device, you might need to run the script with sudo
  permissions.

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

Frontend components
~~~~~~~~~~~~~~~~~~~

There are several components that can be reused on different pages.
This components are declared on the components module:
`src/pybind/mgr/dashboard/frontend/src/app/shared/components`.

Helper
......

This component should be used to provide additional information to the user.

Example:

.. code:: html

    <cd-helper>
      Some <strong>helper</strong> html text
    </cd-helper>

Backend Development
-------------------

The Python backend code of this module requires a number of Python modules to be
installed. They are listed in file ``requirements.txt``. Using `pip
<https://pypi.python.org/pypi/pip>`_ you may install all required dependencies
by issuing ``pip install -r requirements.txt`` in directory
``src/pybind/mgr/dashboard``.

If you're using the `ceph-dev-docker development environment
<https://github.com/ricardoasmarques/ceph-dev-docker/>`_, simply run
``./install_deps.sh`` from the toplevel directory to install them.

Unit Testing
~~~~~~~~~~~~

In dashboard we have two different kinds of backend tests:

1. Unit tests based on ``tox``
2. API tests based on Teuthology.

Unit tests based on tox
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

To run the tests, run ``tox`` in the dashboard directory (where ``tox.ini``
is located).

We also collect coverage information from the backend code. You can check the
coverage information provided by the tox output, or by running the following
command after tox has finished successfully::

  $ coverage html

This command will create a directory ``htmlcov`` with an HTML representation of
the code coverage of the backend.

You can also run a single step of the tox script (aka tox environment), for
instance if you only want to run the linting tools, do::

  $ tox -e lint

API tests based on Teuthology
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run our API tests against a real Ceph cluster, we leverage the Teuthology framework. This
has the advantage of catching bugs originated from changes in the internal Ceph
code.


Our ``run-backend-api-tests.sh`` script will start a ``vstart`` Ceph cluster before running the
Teuthology tests, and then it stops the cluster after the tests are run. Of
course this implies that you have built/compiled Ceph previously.

Start all dashboard tests by running::

  $ ./run-backend-api-tests.sh

Or, start one or multiple specific tests by specifying the test name::

  $ ./run-backend-api-tests.sh tasks.mgr.dashboard.test_pool.PoolTest

Or, ``source`` the script and run the tests manually::

  $ source run-backend-api-tests.sh
  $ run_teuthology_tests [tests]...
  $ cleanup_teuthology

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
https://mgr_hostname:8443/api/ping2.

It is also possible to have nested controllers. The ``RgwController`` uses
this technique to make the daemons available through the URL
https://mgr_hostname:8443/api/rgw/daemon::

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

We provide the manager module instance as a global variable that can be
imported in any module. We also provide a logger instance in the same way.

Example::

  import cherrypy
  from .. import logger, mgr
  from ..tools import ApiController, RESTController


  @ApiController('servers')
  class Servers(RESTController):
    def list(self):
      logger.debug('Listing available servers')
      return {'servers': mgr.list_servers()}


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
  from .. import mgr
  from ..tools import ApiController, RESTController


  @ApiController('rbdimages')
  class RbdImages(RESTController):
      def __init__(self):
          self.ioctx = mgr.rados.open_ioctx('rbd')
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



How to add a new configuration setting?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you need to store some configuration setting for a new feature, we already
provide an easy mechanism for you to specify/use the new config setting.

For instance, if you want to add a new configuration setting to hold the
email address of the dashboard admin, just add a setting name as a class
attribute to the ``Options`` class in the ``settings.py`` file::

  # ...
  class Options(object):
    # ...

    ADMIN_EMAIL_ADDRESS = ('admin@admin.com', str)

The value of the class attribute is a pair composed by the default value for that
setting, and the python type of the value.

By declaring the ``ADMIN_EMAIL_ADDRESS`` class attribute, when you restart the
dashboard plugin, you will atomatically gain two additional CLI commands to
get and set that setting::

  $ ceph dashboard get-admin-email-address
  $ ceph dashboard set-admin-email-address <value>

To access, or modify the config setting value from your Python code, either
inside a controller or anywhere else, you just need to import the ``Settings``
class and access it like this::

  from settings import Settings

  # ...
  tmp_var = Settings.ADMIN_EMAIL_ADDRESS

  # ....
  Settings.ADMIN_EMAIL_ADDRESS = 'myemail@admin.com'

The settings management implementation will make sure that if you change a
setting value from the Python code you will see that change when accessing
that setting from the CLI and vice-versa.


How to run a controller read-write operation asynchronously?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some controllers might need to execute operations that alter the state of the
Ceph cluster. These operations might take some time to execute and to maintain
a good user experience in the Web UI, we need to run those operations
asynchronously and return immediately to frontend some information that the
operations are running in the background.

To help in the development of the above scenario we added the support for
asynchronous tasks. To trigger the execution of an asynchronous task we must
use the following class method of the ``TaskManager`` class::

  from ..tools import TaskManager
  # ...
  TaskManager.run(name, metadata, func, args, kwargs)

* ``name`` is a string that can be used to group tasks. For instance
  for RBD image creation tasks we could specify ``"rbd/create"`` as the
  name, or similarly ``"rbd/remove"`` for RBD image removal tasks.

* ``metadata`` is a dictionary where we can store key-value pairs that
  characterize the task. For instance, when creating a task for creating
  RBD images we can specify the metadata argument as
  ``{'pool_name': "rbd", image_name': "test-img"}``.

* ``func`` is the python function that implements the operation code, which
  will be executed asynchronously.

* ``args`` and ``kwargs`` are the positional and named arguments that will be
  passed to ``func`` when the task manager starts its execution.

The ``TaskManager.run`` method triggers the asynchronous execution of function
``func`` and returns a ``Task`` object.
The ``Task`` provides the public method ``Task.wait(timeout)``, which can be
used to wait for the task to complete up to a timeout defined in seconds and
provided as an argument. If no argument is provided the ``wait`` method
blocks until the task is finished.

The ``Task.wait`` is very useful for tasks that usually are fast to execute but
that sometimes may take a long time to run.
The return value of the ``Task.wait`` method is a pair ``(state, value)``
where ``state`` is a string with following possible values:

* ``VALUE_DONE = "done"``
* ``VALUE_EXECUTING = "executing"``

The ``value`` will store the result of the execution of function ``func`` if
``state == VALUE_DONE``. If ``state == VALUE_EXECUTING`` then
``value == None``.

The pair ``(name, metadata)`` should unequivocally identify the task being
run, which means that if you try to trigger a new task that matches the same
``(name, metadata)`` pair of the currently running task, then the new task
is not created and you get the task object of the current running task.

For instance, consider the following example:

.. code-block:: python

  task1 = TaskManager.run("dummy/task", {'attr': 2}, func)
  task2 = TaskManager.run("dummy/task", {'attr': 2}, func)

If the second call to ``TaskManager.run`` executes while the first task is
still executing then it will return the same task object:
``assert task1 == task2``.


How to get the list of executing and finished asynchronous tasks?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The list of executing and finished tasks is included in the ``Summary``
controller, which is already polled every 5 seconds by the dashboard frontend.
But we also provide a dedicated controller to get the same list of executing
and finished tasks.

The ``Task`` controller exposes the ``/api/task`` endpoint that returns the
list of executing and finished tasks. This endpoint accepts the ``name``
parameter that accepts a glob expression as its value.
For instance, an HTTP GET request of the URL ``/api/task?name=rbd/*``
will return all executing and finished tasks which name starts with ``rbd/``.

To prevent the finished tasks list from growing unbounded, we will always
maintain the 10 most recent finished tasks, and the remaining older finished
tasks will be removed when reaching a TTL of 1 minute. The TTL is calculated
using the timestamp when the task finished its execution. After a minute, when
the finished task information is retrieved, either by the summary controller or
by the task controller, it is automatically deleted from the list and it will
not be included in further task queries.

Each executing task is represented by the following dictionary::

  {
    'name': "name",  # str
    'metadata': { },  # dict
    'begin_time': "2018-03-14T15:31:38.423605Z",  # str (ISO 8601 format)
    'progress': 0  # int (percentage)
  }

Each finished task is represented by the following dictionary::

  {
    'name': "name",  # str
    'metadata': { },  # dict
    'begin_time': "2018-03-14T15:31:38.423605Z",  # str (ISO 8601 format)
    'end_time': "2018-03-14T15:31:39.423605Z",  # str (ISO 8601 format)
    'duration': 0.0,  # float
    'progress': 0  # int (percentage)
    'success': True,  # bool
    'ret_value': None,  # object, populated only if 'success' == True
    'exception': None,  # str, populated only if 'success' == False
  }


How to use asynchronous APIs with asynchronous tasks?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``TaskManager.run`` method as described in a previous section, is well
suited for calling blocking functions, as it runs the function inside a newly
created thread. But sometimes we want to call some function of an API that is
already asynchronous by nature.

For these cases we want to avoid creating a new thread for just running a
non-blocking function, and want to leverage the asynchronous nature of the
function. The ``TaskManager.run`` is already prepared to be used with
non-blocking functions by passing an object of the type ``TaskExecutor`` as an
additional parameter called ``executor``. The full method signature of
``TaskManager.run``::

  TaskManager.run(name, metadata, func, args=None, kwargs=None, executor=None)


The ``TaskExecutor`` class is responsible for code that executes a given task
function, and defines three methods that can be overriden by
subclasses::

  def init(self, task)
  def start(self)
  def finish(self, ret_value, exception)

The ``init`` method is called before the running the task function, and
receives the task object (of class ``Task``).

The ``start`` method runs the task function. The default implementation is to
run the task function in the current thread context.

The ``finish`` method should be called when the task function finishes with
either the ``ret_value`` populated with the result of the execution, or with
an exception object in the case that execution raised an exception.

To leverage the asynchronous nature of a non-blocking function, the developer
should implement a custom executor by creating a subclass of the
``TaskExecutor`` class, and provide an instance of the custom executor class
as the ``executor`` parameter of the ``TaskManager.run``.

To better understand the expressive power of executors, we write a full example
of use a custom executor to execute the ``MgrModule.send_command`` asynchronous
function:

.. code-block:: python

  import json
  from mgr_module import CommandResult
  from .. import mgr
  from ..tools import ApiController, RESTController, NotificationQueue, \
                      TaskManager, TaskExecutor


  class SendCommandExecutor(TaskExecutor):
      def __init__(self):
          super(SendCommandExecutor, self).__init__()
          self.tag = None
          self.result = None

      def init(self, task):
          super(SendCommandExecutor, self).init(task)

          # we need to listen for 'command' events to know when the command
          # finishes
          NotificationQueue.register(self._handler, 'command')

          # store the CommandResult object to retrieve the results
          self.result = self.task.fn_args[0]
          if len(self.task.fn_args) > 4:
              # the user specified a tag for the command, so let's use it
              self.tag = self.task.fn_args[4]
          else:
              # let's generate a unique tag for the command
              self.tag = 'send_command_{}'.format(id(self))
              self.task.fn_args.append(self.tag)

      def _handler(self, data):
          if data == self.tag:
              # the command has finished, notifying the task with the result
              self.finish(self.result.wait(), None)
              # deregister listener to avoid memory leaks
              NotificationQueue.deregister(self._handler, 'command')


  @ApiController('test')
  class Test(RESTController):

      def _run_task(self, osd_id):
          task = TaskManager.run("test/task", {}, mgr.send_command,
                                 [CommandResult(''), 'osd', osd_id,
                                  json.dumps({'prefix': 'perf histogram dump'})],
                                 executor=SendCommandExecutor())
          return task.wait(1.0)

      def get(self, osd_id):
          status, value = self._run_task(osd_id)
          return {'status': status, 'value': value}


The above ``SendCommandExecutor`` executor class can be used for any call to
``MgrModule.send_command``. This means that we should need just one custom
executor class implementation for each non-blocking API that we use in our
controllers.

The default executor, used when no executor object is passed to
``TaskManager.run``, is the ``ThreadedExecutor``. You can check its
implementation in the ``tools.py`` file.


How to update the execution progress of an asynchronous task?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The asynchronous tasks infrastructure provides support for updating the
execution progress of an executing task.
The progress can be updated from within the code the task is executing, which
usually is the place where we have the progress information available.

To update the progress from within the task code, the ``TaskManager`` class
provides a method to retrieve the current task object::

  TaskManager.current_task()

The above method is only available when using the default executor
``ThreadedExecutor`` for executing the task.
The ``current_task()`` method returns the current ``Task`` object. The
``Task`` object provides two public methods to update the execution progress
value: the ``set_progress(percentage)``, and the ``inc_progress(delta)``
methods.

The ``set_progress`` method receives as argument an integer value representing
the absolute percentage that we want to set to the task.

The ``inc_progress`` method receives as argument an integer value representing
the delta we want to increment to the current execution progress percentage.

Take the following example of a controller that triggers a new task and
updates its progress:

.. code-block:: python

  from __future__ import absolute_import
  import random
  import time
  import cherrypy
  from ..tools import TaskManager, ApiController, BaseController


  @ApiController('dummy_task')
  class DummyTask(BaseController):
      def _dummy(self):
          top = random.randrange(100)
          for i in range(top):
              TaskManager.current_task().set_progress(i*100/top)
              # or TaskManager.current_task().inc_progress(100/top)
              time.sleep(1)
          return "finished"

      @cherrypy.expose
      @cherrypy.tools.json_out()
      def default(self):
          task = TaskManager.run("dummy/task", {}, self._dummy)
          return task.wait(5)  # wait for five seconds


How to deal with asynchronous tasks in the front-end?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All executing and most recently finished asynchronous tasks are displayed on the
"Backgroud-Tasks" menu.

The front-end developer should provide a description, success message and error
messages for each task on ``TaskManagerMessageService.messages``.
This messages can make use of the task metadata to provide more personalized messages.

When submitting an asynchronous task, the developer should provide a callback
that will be automatically triggered after the execution of that task.
This can be done by using the ``TaskManagerService.subscribe``.

Most of the times, all we want to do after a task completes the execution, is
displaying a notification message based on the execution result. The
``NotificationService.notifyTask`` will use the messages from
``TaskManagerMessageService`` to display a success / error message based on the
execution result of a task.

Usage example:

.. code-block:: javascript

  export class TaskManagerMessageService {

    messages = {
      // Messages for 'rbd/create' task
      'rbd/create': new TaskManagerMessage(
        // Description
        (metadata) => `Create RBD '${metadata.pool_name}/${metadata.image_name}'`,
        // Success message
        (metadata) => `RBD '${metadata.pool_name}/${metadata.image_name}'
                       have been created successfully`,
        // Error messages
        (metadata) => {
          return {
            '17': `Name '${metadata.pool_name}/${metadata.image_name}' is already
                   in use.`
          };
        }
      ),
      // ...
    };

    // ...
  }

  export class RBDFormComponent {
    // ...

    submit() {
      // ...
      this.rbdService.create(request).then((resp) => {
        // Subscribe the submitted task
        this.taskManagerService.subscribe('rbd/create',
          {'pool_name': request.pool_name, 'rbd_name': request.name},
          // Callback that will be invoked after task is finished
          (finishedTask: FinishedTask) => {
            // Will display a notification message (success or error)
            this.notificationService.notifyTask(finishedTask, finishedTask.ret_value.success);
          });
        // ...
      })
    }
  }
