Dashboard Developer Documentation
====================================

.. contents:: Table of Contents

Frontend Development
--------------------

Before you can start the dashboard from within a development environment, you
will need to generate the frontend code and either use a compiled and running
Ceph cluster (e.g. started by ``vstart.sh``) or the standalone development web
server.

The build process is based on `Node.js <https://nodejs.org/>`_ and requires the
`Node Package Manager <https://www.npmjs.com/>`_ ``npm`` to be installed.

Prerequisites
~~~~~~~~~~~~~

 * Node 8.9.0 or higher
 * NPM 5.7.0 or higher

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

Code linting and formatting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use the following tools to lint and format the code in all our TS, SCSS and
HTML files:

- `codelyzer <http://codelyzer.com/>`_
- `html-linter <https://github.com/chinchiheather/html-linter>`_
- `Prettier <https://prettier.io/>`_
- `TSLint <https://palantir.github.io/tslint/>`_

We added 2 npm scripts to help run these tools:

- ``npm run lint``, will check frontend files against all linters
- ``npm run fix``, will try to fix all the detected linting errors

Writing Unit Tests
~~~~~~~~~~~~~~~~~~

To write unit tests most efficient we have a small collection of tools,
we use within test suites.

Those tools can be found under
``src/pybind/mgr/dashboard/frontend/src/testing/``, especially take
a look at ``unit-test-helper.ts``.

There you will be able to find:

``configureTestBed`` that replaces the initial ``TestBed``
methods. It takes the same arguments as ``TestBed.configureTestingModule``.
Using it will run your tests a lot faster in development, as it doesn't
recreate everything from scratch on every test. To use the default behaviour
pass ``true`` as the second argument.

``PermissionHelper`` to help determine if
the correct actions are shown based on the current permissions and selection
in a list.

``FormHelper`` which makes testing a form a lot easier
with a few simple methods. It allows you to set a control or multiple
controls, expect if a control is valid or has an error or just do both with
one method. Additional you can expect a template element or multiple elements
to be visible in the rendered template.

Running Unit Tests
~~~~~~~~~~~~~~~~~~

Create ``unit-test-configuration.ts`` file based on
``unit-test-configuration.ts.sample`` in directory
``src/pybind/mgr/dashboard/frontend/src``.

Run ``npm run test`` to execute the unit tests via `Jest
<https://facebook.github.io/jest/>`_.

If you get errors on all tests, it could be because `Jest
<https://facebook.github.io/jest/>`_ or something else was updated.
There are a few ways how you can try to resolve this:

- Remove all modules with ``rm -rf dist node_modules`` and run ``npm install``
  again in order to reinstall them
- Clear the cache of jest by running ``npx jest --clearCache``

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

Writing End-to-End Tests
~~~~~~~~~~~~~~~~~~~~~~~~

When writing e2e tests you don't want to recompile every time from scratch to
try out if your test has succeeded. As usual you have your development server
open (``npm start``) which already has compiled all files. To attach
`Protractor <http://www.protractortest.org/>`__ to this process, instead of
spinning up it's own server, you can use ``npm run e2e -- --dev-server-target``
or just ``npm run e2e:dev`` which is equivalent.

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

Frontend branding
~~~~~~~~~~~~~~~~~

Every vendor can customize the 'Ceph dashboard' to his needs. No matter if
logo, HTML-Template or TypeScript, every file inside the frontend folder can be
replaced.

To replace files, open ``./frontend/angular.json`` and scroll to the section
``fileReplacements`` inside the production configuration. Here you can add the
files you wish to brand. We recommend to place the branded version of a file in
the same directory as the original one and to add a ``.brand`` to the file
name, right in front of the file extension. A ``fileReplacement`` could for
example look like this:

.. code:: javascript

    {
      "replace": "src/app/core/auth/login/login.component.html",
      "with": "src/app/core/auth/login/login.component.brand.html"
    }

To serve or build the branded user interface run:

    $ npm run start -- --prod

or

    $ npm run build -- --prod

Unfortunately it's currently not possible to use multiple configurations when
serving or building the UI at the same time. That means a configuration just
for the branding ``fileReplacements`` is not an option, because you want to use
the production configuration anyway
(https://github.com/angular/angular-cli/issues/10612).
Furthermore it's also not possible to use glob expressions for
``fileReplacements``. As long as the feature hasn't been implemented, you have
to add the file replacements manually to the angular.json file
(https://github.com/angular/angular-cli/issues/12354).

Nevertheless you should stick to the suggested naming scheme because it makes
it easier for you to use glob expressions once it's supported in the future.

To change the variable defaults you can overwrite them in the file
``./frontend/src/vendor.variables.scss``. Just reassign the variable you want
to change, for example ``$color-primary: teal;``
To overwrite or extend the default CSS, you can add your own styles in
``./frontend/src/vendor.overrides.scss``.

I18N
----

How to extract messages from source code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To extract the I18N messages from the templates and the TypeScript files just
run the following command in ``src/pybind/mgr/dashboard/frontend``::

  $ npm run i18n

This will extract all marked messages from the HTML templates first and then
add all marked strings from the TypeScript files to the translation template.
Since the extraction from TypeScript files is still not supported by Angular
itself, we are using the
`ngx-translator <https://github.com/ngx-translate/i18n-polyfill>`_ extractor to
parse the TypeScript files.

When the command ran successfully, it should have created or updated the file
``src/locale/messages.xlf``.

To make sure this file is always up to date in master branch, we added a
validation in ``run-frontend-unittests.sh`` that will fail if it finds
uncommitted translations.

Supported languages
~~~~~~~~~~~~~~~~~~~

All our supported languages should be registeredd in
``supported-languages.enum.ts``, this will then provide that list to both the
language selectors in the frontend.

Translating process
~~~~~~~~~~~~~~~~~~~

To facilitate the translation process of the dashboard we are using a web tool
called `transifex <https://www.transifex.com/>`_.

If you wish to help translating to any language just go to our `transifex
project page <https://www.transifex.com/ceph/ceph-dashboard/>`_, join the
project and you can start translating immediately.

All translations will then be reviewed and later pushed upstream.

Updating translated messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any time there are new messages translated and reviewed in a specific language
we should update the translation file upstream.

To do that, we need to download the language xlf file from transifex and replace
the current one in the repository. Since Angular doesn't support missing
translations, we need to do an extra step and fill all the untranslated strings
with the source string.

Each language file should be placed in ``src/locale/messages.<locale-id>.xlf``.
For example, the path for german would be ``src/locale/messages.de-DE.xlf``.
``<locale-id>`` should match the id previouisly inserted in
``supported-languages.enum.ts``.

Suggestions
~~~~~~~~~~~

Strings need to start and end in the same line as the element:

.. code-block:: xml

  <!-- avoid -->
  <span i18n>
    Foo
  </span>

  <!-- recommended -->
  <span i18n>Foo</span>


  <!-- avoid -->
  <span i18n>
    Foo bar baz.
    Foo bar baz.
  </span>

  <!-- recommended -->
  <span i18n>Foo bar baz.
    Foo bar baz.</span>

Isolated interpolations should not be translated:

.. code-block:: xml

  <!-- avoid -->
  <span i18n>{{ foo }}</span>

  <!-- recommended -->
  <span>{{ foo }}</span>

Interpolations used in a sentence should be kept in the translation:

.. code-block:: xml

  <!-- recommended -->
  <span i18n>There are {{ x }} OSDs.</span>

Remove elements that are outside the context of the translation:

.. code-block:: xml

  <!-- avoid -->
  <label i18n>
    Profile
    <span class="required"></span>
  </label>

  <!-- recommended -->
  <label>
    <ng-container i18n>Profile<ng-container>
    <span class="required"></span>
  </label>

Keep elements that affect the sentence:

.. code-block:: xml

  <!-- recommended -->
  <span i18n>Profile <b>foo</b> will be removed.</span>

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

To run the tests, run ``run-tox.sh`` in the dashboard directory (where
``tox.ini`` is located)::

  ## Run Python 2+3 tests+lint commands:
  $ ./run-tox.sh

  ## Run Python 3 tests+lint commands:
  $ WITH_PYTHON2=OFF ./run-tox.sh

  ## Run Python 3 arbitrary command (e.g. 1 single test):
  $ WITH_PYTHON2=OFF ./run-tox.sh pytest tests/test_rgw_client.py::RgwClientTest::test_ssl_verify

You can also run tox instead of ``run-tox.sh``::

  ## Run Python 3 tests command:
  $ CEPH_BUILD_DIR=.tox tox -e py3-cov

  ## Run Python 3 arbitrary command (e.g. 1 single test):
  $ CEPH_BUILD_DIR=.tox tox -e py3-run pytest tests/test_rgw_client.py::RgwClientTest::test_ssl_verify

We also collect coverage information from the backend code when you run tests. You can check the
coverage information provided by the tox output, or by running the following
command after tox has finished successfully::

  $ coverage html

This command will create a directory ``htmlcov`` with an HTML representation of
the code coverage of the backend.

API tests based on Teuthology
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

How to run existing API tests:
  To run the API tests against a real Ceph cluster, we leverage the Teuthology
  framework. This has the advantage of catching bugs originated from changes in
  the internal Ceph code.

  Our ``run-backend-api-tests.sh`` script will start a ``vstart`` Ceph cluster
  before running the Teuthology tests, and then it stops the cluster after the
  tests are run. Of course this implies that you have built/compiled Ceph
  previously.

  Start all dashboard tests by running::

    $ ./run-backend-api-tests.sh

  Or, start one or multiple specific tests by specifying the test name::

    $ ./run-backend-api-tests.sh tasks.mgr.dashboard.test_pool.PoolTest

  Or, ``source`` the script and run the tests manually::

    $ source run-backend-api-tests.sh
    $ run_teuthology_tests [tests]...
    $ cleanup_teuthology

How to write your own tests:
  There are two possible ways to write your own API tests:

  The first is by extending one of the existing test classes in the
  ``qa/tasks/mgr/dashboard`` directory.

  The second way is by adding your own API test module if you're creating a new
  controller for example. To do so you'll just need to add the file containing
  your new test class to the ``qa/tasks/mgr/dashboard`` directory and implement
  all your tests here.

  .. note:: Don't forget to add the path of the newly created module to
    ``modules`` section in ``qa/suites/rados/mgr/tasks/dashboard.yaml``.

  Short example: Let's assume you created a new controller called
  ``my_new_controller.py`` and the related test module
  ``test_my_new_controller.py``. You'll need to add
  ``tasks.mgr.dashboard.test_my_new_controller`` to the ``modules`` section in
  the ``dashboard.yaml`` file.

  Also, if you're removing test modules please keep in mind to remove the
  related section. Otherwise the Teuthology test run will fail.

  Please run your API tests on your dev environment (as explained above)
  before submitting a pull request. Also make sure that a full QA run in
  Teuthology/sepia lab (based on your changes) has completed successfully
  before it gets merged. You don't need to schedule the QA run yourself, just
  add the 'needs-qa' label to your pull request as soon as you think it's ready
  for merging (e.g. make check was successful, the pull request is approved and
  all comments have been addressed). One of the developers who has access to
  Teuthology/the sepia lab will take care of it and report the result back to
  you.


How to add a new controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A controller is a Python class that extends from the ``BaseController`` class
and is decorated with either the ``@Controller``, ``@ApiController`` or
``@UiApiController`` decorators. The Python class must be stored inside a Python
file located under the ``controllers`` directory. The Dashboard module will
automatically load your new controller upon start.

``@ApiController`` and ``@UiApiController`` are both specializations of the
``@Controller`` decorator.

The ``@ApiController`` should be used for controllers that provide an API-like
REST interface and the ``@UiApiController`` should be used for endpoints consumed
by the UI but that are not part of the 'public' API. For any other kinds of
controllers the ``@Controller`` decorator should be used.

A controller has a URL prefix path associated that is specified in the
controller decorator, and all endpoints exposed by the controller will share
the same URL prefix path.

A controller's endpoint is exposed by implementing a method on the controller
class decorated with the ``@Endpoint`` decorator.

For example create a file ``ping.py`` under ``controllers`` directory with the
following code:

.. code-block:: python

  from ..tools import Controller, ApiController, UiApiController, BaseController, Endpoint

  @Controller('/ping')
  class Ping(BaseController):
    @Endpoint()
    def hello(self):
      return {'msg': "Hello"}

  @ApiController('/ping')
  class ApiPing(BaseController):
    @Endpoint()
    def hello(self):
      return {'msg': "Hello"}

  @UiApiController('/ping')
  class UiApiPing(BaseController):
    @Endpoint()
    def hello(self):
      return {'msg': "Hello"}

The ``hello`` endpoint of the ``Ping`` controller can be reached by the
following URL: https://mgr_hostname:8443/ping/hello using HTTP GET requests.
As you can see the controller URL path ``/ping`` is concatenated to the
method name ``hello`` to generate the endpoint's URL.

In the case of the ``ApiPing`` controller, the ``hello`` endpoint can be
reached by the following URL: https://mgr_hostname:8443/api/ping/hello using a
HTTP GET request.
The API controller URL path ``/ping`` is prefixed by the ``/api`` path and then
concatenated to the method name ``hello`` to generate the endpoint's URL.
Internally, the ``@ApiController`` is actually calling the ``@Controller``
decorator by passing an additional decorator parameter called ``base_url``::

  @ApiController('/ping') <=> @Controller('/ping', base_url="/api")

``UiApiPing`` works in a similar way than the ``ApiPing``, but the URL will be
prefixed by ``/ui-api``: https://mgr_hostname:8443/ui-api/ping/hello. ``UiApiPing`` is
also a ``@Controller`` extension::

  @UiApiController('/ping') <=> @Controller('/ping', base_url="/ui-api")

The ``@Endpoint`` decorator also supports many parameters to customize the
endpoint:

* ``method="GET"``: the HTTP method allowed to access this endpoint.
* ``path="/<method_name>"``: the URL path of the endpoint, excluding the
  controller URL path prefix.
* ``path_params=[]``: list of method parameter names that correspond to URL
  path parameters. Can only be used when ``method in ['POST', 'PUT']``.
* ``query_params=[]``: list of method parameter names that correspond to URL
  query parameters.
* ``json_response=True``: indicates if the endpoint response should be
  serialized in JSON format.
* ``proxy=False``: indicates if the endpoint should be used as a proxy.

An endpoint method may have parameters declared. Depending on the HTTP method
defined for the endpoint the method parameters might be considered either
path parameters, query parameters, or body parameters.

For ``GET`` and ``DELETE`` methods, the method's non-optional parameters are
considered path parameters by default. Optional parameters are considered
query parameters. By specifying the ``query_parameters`` in the endpoint
decorator it is possible to make a non-optional parameter to be a query
parameter.

For ``POST`` and ``PUT`` methods, all method parameters are considered
body parameters by default. To override this default, one can use the
``path_params`` and ``query_params`` to specify which method parameters are
path and query parameters respectivelly.
Body parameters are decoded from the request body, either from a form format, or
from a dictionary in JSON format.

Let's use an example to better understand the possible ways to customize an
endpoint:

.. code-block:: python

  from ..tools import Controller, BaseController, Endpoint

  @Controller('/ping')
  class Ping(BaseController):

    # URL: /ping/{key}?opt1=...&opt2=...
    @Endpoint(path="/", query_params=['opt1'])
    def index(self, key, opt1, opt2=None):
      # ...

    # URL: /ping/{key}?opt1=...&opt2=...
    @Endpoint(query_params=['opt1'])
    def __call__(self, key, opt1, opt2=None):
      # ...

    # URL: /ping/post/{key1}/{key2}
    @Endpoint('POST', path_params=['key1', 'key2'])
    def post(self, key1, key2, data1, data2=None):
      # ...


In the above example we see how the ``path`` option can be used to override the
generated endpoint URL in order to not use the method's name in the URL. In the
``index`` method we set the ``path`` to ``"/"`` to generate an endpoint that is
accessible by the root URL of the controller.

An alternative approach to generate an endpoint that is accessible through just
the controller's path URL is by using the ``__call__`` method, as we show in
the above example.

From the third method you can see that the path parameters are collected from
the URL by parsing the list of values separated by slashes ``/`` that come
after the URL path ``/ping`` for ``index`` method case, and ``/ping/post`` for
the ``post`` method case.

Defining path parameters in endpoints's URLs using python methods's parameters
is very easy but it is still a bit strict with respect to the position of these
parameters in the URL structure.
Sometimes we may want to explicitly define a URL scheme that
contains path parameters mixed with static parts of the URL.
Our controller infrastructure also supports the declaration of URL paths with
explicit path parameters at both the controller level and method level.

Consider the following example:

.. code-block:: python

  from ..tools import Controller, BaseController, Endpoint

  @Controller('/ping/{node}/stats')
  class Ping(BaseController):

    # URL: /ping/{node}/stats/{date}/latency?unit=...
    @Endpoint(path="/{date}/latency")
    def latency(self, node, date, unit="ms"):
      # ...

In this example we explicitly declare a path parameter ``{node}`` in the
controller URL path, and a path parameter ``{date}`` in the ``latency``
method. The endpoint for the ``latency`` method is then accessible through
the URL: https://mgr_hostname:8443/ping/{node}/stats/{date}/latency .

For a full set of examples on how to use the ``@Endpoint``
decorator please check the unit test file: ``tests/test_controllers.py``.
There you will find many examples of how to customize endpoint methods.


Implementing Proxy Controller
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes you might need to relay some requests from the Dashboard frontend
directly to an external service.
For that purpose we provide a decorator called ``@Proxy``.
(As a concrete example, check the ``controllers/rgw.py`` file where we
implemented an RGW Admin Ops proxy.)


The ``@Proxy`` decorator is a wrapper of the ``@Endpoint`` decorator that
already customizes the endpoint for working as a proxy.
A proxy endpoint works by capturing the URL path that follows the controller
URL prefix path, and does not do any decoding of the request body.

Example:

.. code-block:: python

  from ..tools import Controller, BaseController, Proxy

  @Controller('/foo/proxy')
  class FooServiceProxy(BaseController):

    @Proxy()
    def proxy(self, path, **params):
      # if requested URL is "/foo/proxy/access/service?opt=1"
      # then path is "access/service" and params is {'opt': '1'}
      # ...


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
example:

.. code-block:: python

  import cherrypy
  from ..tools import ApiController, RESTController

  @ApiController('ping')
  class Ping(RESTController):
    def list(self):
      return {"msg": "Hello"}

    def get(self, id):
      return self.objects[id]

In this case, the ``list`` method is automatically used for all requests to
``api/ping`` where no additional argument is given and where the request type
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
| POST         | No         | create         | 201         |
+--------------+------------+----------------+-------------+
| DELETE       | No         | bulk_delete    | 204         |
+--------------+------------+----------------+-------------+
| GET          | Yes        | get            | 200         |
+--------------+------------+----------------+-------------+
| PUT          | Yes        | set            | 200         |
+--------------+------------+----------------+-------------+
| DELETE       | Yes        | delete         | 204         |
+--------------+------------+----------------+-------------+

How to use a custom API endpoint in a RESTController?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you don't have any access restriction you can use ``@Endpoint``. If you
have set a permission scope to restrict access to your endpoints,
``@Endpoint`` will fail, as it doesn't know which permission property should be
used. To use a custom endpoint inside a restricted ``RESTController`` use
``@RESTController.Collection`` instead. You can also choose
``@RESTController.Resource`` if you have set a ``RESOURCE_ID`` in your
``RESTController`` class.

.. code-block:: python

  import cherrypy
  from ..tools import ApiController, RESTController

  @ApiController('ping', Scope.Ping)
  class Ping(RESTController):
    RESOURCE_ID = 'ping'

    @RESTController.Resource('GET')
    def some_get_endpoint(self):
      return {"msg": "Hello"}

    @RESTController.Collection('POST')
    def some_post_endpoint(self, **data):
      return {"msg": data}

Both decorators also support four parameters to customize the
endpoint:

* ``method="GET"``: the HTTP method allowed to access this endpoint.
* ``path="/<method_name>"``: the URL path of the endpoint, excluding the
  controller URL path prefix.
* ``status=200``: set the HTTP status response code
* ``query_params=[]``: list of method parameter names that correspond to URL
  query parameters.

How to restrict access to a controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All controllers require authentication by default.
If you require that the controller can be accessed without authentication,
then you can add the parameter ``secure=False`` to the controller decorator.

Example:

.. code-block:: python

  import cherrypy
  from . import ApiController, RESTController


  @ApiController('ping', secure=False)
  class Ping(RESTController):
    def list(self):
      return {"msg": "Hello"}


How to access the manager module instance from a controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We provide the manager module instance as a global variable that can be
imported in any module. We also provide a logger instance in the same way.

Example:

.. code-block:: python

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

If we want to write a unit test for the above ``Ping`` controller, create a
``test_ping.py`` file under the ``tests`` directory with the following code:

.. code-block:: python

  from .helper import ControllerTestCase
  from .controllers.ping import Ping


  class PingTest(ControllerTestCase):
      @classmethod
      def setup_test(cls):
          Ping._cp_config['tools.authenticate.on'] = False
          cls.setup_controllers([Ping])

      def test_ping(self):
          self._get("/api/ping")
          self.assertStatus(200)
          self.assertJsonBody({'msg': 'Hello'})

The ``ControllerTestCase`` class starts by initializing a CherryPy webserver.
Then it will call the ``setup_test()`` class method where we can explicitly
load the controllers that we want to test. In the above example we are only
loading the ``Ping`` controller. We can also disable authentication of a
controller at this stage, as depicted in the example.


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
log viewer page:

.. code-block:: python

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
list of RBD images of the ``rbd`` pool:

.. code-block:: python

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

The unit test code will look like the following:

.. code-block:: python

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
dashboard plugin, you will automatically gain two additional CLI commands to
get and set that setting::

  $ ceph dashboard get-admin-email-address
  $ ceph dashboard set-admin-email-address <value>

To access, or modify the config setting value from your Python code, either
inside a controller or anywhere else, you just need to import the ``Settings``
class and access it like this:

.. code-block:: python

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
function, and defines three methods that can be overridden by
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

All executing and most recently finished asynchronous tasks are displayed on
"Background-Tasks" and if finished on "Recent-Notifications" in the menu bar.
For each task a operation name for three states (running, success and failure),
a function that tells who is involved and error descriptions, if any, have to
be provided. This can be  achieved by appending
``TaskManagerMessageService.messages``.  This has to be done to achieve
consistency among all tasks and states.

Operation Object
  Ensures consistency among all tasks. It consists of three verbs for each
  different state f.e.
  ``{running: 'Creating', failure: 'create', success: 'Created'}``.

#. Put running operations in present participle f.e. ``'Updating'``.
#. Failed messages always start with ``'Failed to '`` and should be continued
   with the operation in present tense f.e. ``'update'``.
#. Put successful operations in past tense f.e. ``'Updated'``.

Involves Function
  Ensures consistency among all messages of a task, it resembles who's
  involved by the operation. It's a function that returns a string which
  takes the metadata from the task to return f.e.
  ``"RBD 'somePool/someImage'"``.

Both combined create the following messages:

* Failure => ``"Failed to create RBD 'somePool/someImage'"``
* Running => ``"Creating RBD 'somePool/someImage'"``
* Success => ``"Created RBD 'somePool/someImage'"``

For automatic task handling use ``TaskWrapperService.wrapTaskAroundCall``.

If for some reason ``wrapTaskAroundCall`` is not working for you,
you have to subscribe to your asynchronous task manually through
``TaskManagerService.subscribe``, and provide it with a callback,
in case of a success to notify the user. A notification can
be triggered with ``NotificationService.notifyTask``. It will use
``TaskManagerMessageService.messages`` to display a message based on the state
of a task.

Notifications of API errors are handled by ``ApiInterceptorService``.

Usage example:

.. code-block:: javascript

  export class TaskManagerMessageService {
    // ...
    messages = {
      // Messages for task 'rbd/create'
      'rbd/create': new TaskManagerMessage(
        // Message prefixes
        ['create', 'Creating', 'Created'],
        // Message suffix
        (metadata) => `RBD '${metadata.pool_name}/${metadata.image_name}'`,
        (metadata) => ({
          // Error code and description
          '17': `Name is already used by RBD '${metadata.pool_name}/${
                 metadata.image_name}'.`
        })
      ),
      // ...
    };
    // ...
  }

  export class RBDFormComponent {
    // ...
    createAction() {
      const request = this.createRequest();
      // Subscribes to 'call' with submitted 'task' and handles notifications
      return this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('rbd/create', {
          pool_name: request.pool_name,
          image_name: request.name
        }),
        call: this.rbdService.create(request)
      });
    }
    // ...
  }

Error Handling in Python
~~~~~~~~~~~~~~~~~~~~~~~~

Good error handling is a key requirement in creating a good user experience
and providing a good API.

Dashboard code should not duplicate C++ code. Thus, if error handling in C++
is sufficient to provide good feedback, a new wrapper to catch these errors
is not necessary. On the other hand, input validation is the best place to
catch errors and generate the best error messages. If required, generate
errors as soon as possible.

The backend provides few standard ways of returning errors.

First, there is a generic Internal Server Error::

    Status Code: 500
    {
        "version": <cherrypy version, e.g. 13.1.0>,
        "detail": "The server encountered an unexpected condition which prevented it from fulfilling the request.",
    }


For errors generated by the backend, we provide a standard error
format::

    Status Code: 400
    {
        "detail": str(e),     # E.g. "[errno -42] <some error message>"
        "component": "rbd",   # this can be null to represent a global error code
        "code": "3",          # Or a error name, e.g. "code": "some_error_key"
    }


In case, the API Endpoints uses @ViewCache to temporarily cache results,
the error looks like so::

    Status Code 400
    {
        "detail": str(e),     # E.g. "[errno -42] <some error message>"
        "component": "rbd",   # this can be null to represent a global error code
        "code": "3",          # Or a error name, e.g. "code": "some_error_key"
        'status': 3,          # Indicating the @ViewCache error status
    }

In case, the API Endpoints uses a task the error looks like so::

    Status Code 400
    {
        "detail": str(e),     # E.g. "[errno -42] <some error message>"
        "component": "rbd",   # this can be null to represent a global error code
        "code": "3",          # Or a error name, e.g. "code": "some_error_key"
        "task": {             # Information about the task itself
            "name": "taskname",
            "metadata": {...}
        }
    }


Our WebUI should show errors generated by the API to the user. Especially
field-related errors in wizards and dialogs or show non-intrusive notifications.

Handling exceptions in Python should be an exception. In general, we
should have few exception handlers in our project. Per default, propagate
errors to the API, as it will take care of all exceptions anyway. In general,
log the exception by adding ``logger.exception()`` with a description to the
handler.

We need to distinguish between user errors from internal errors and
programming errors. Using different exception types will ease the
task for the API layer and for the user interface:

Standard Python errors, like ``SystemError``, ``ValueError`` or ``KeyError``
will end up as internal server errors in the API.

In general, do not ``return`` error responses in the REST API. They will be
returned by the  error handler. Instead, raise the appropriate exception.

Plug-ins
~~~~~~~~

New functionality can be provided by means of a plug-in architecture. Among the
benefits this approach brings in, loosely coupled development is one of the most
notable. As the Ceph-Dashboard grows in feature richness, its code-base becomes
more and more complex. The hook-based nature of a plug-in architecture allows to
extend functionality in a controlled manner, and isolate the scope of the
changes.

Ceph-Dashboard relies on `Pluggy <https://pluggy.readthedocs.io>`_ to provide
for plug-ing support. On top of pluggy, an interface-based approach has been
implemented, with some safety checks (method override and abstract method
checks).

In order to create a new plugin, the following steps are required:

#. Add a new file under ``src/pybind/mgr/dashboard/plugins``.
#. Import the ``PLUGIN_MANAGER`` instance and the ``Interfaces``.
#. Create a class extending the desired interfaces. The plug-in library will check if all the methods of the interfaces have been properly overridden.
#. Register the plugin in the ``PLUGIN_MANAGER`` instance.
#. Import the plug-in from within the Ceph-Dashboard ``module.py`` (currently no dynamic loading is implemented).

The available interfaces are the following:

- ``CanMgr``: provides the plug-in with access to the ``mgr`` instance under ``self.mgr``.
- ``CanLog``: provides the plug-in with access to the Ceph-Dashboard logger under ``self.log``.
- ``Setupable``: requires overriding ``setup()`` hook. This method is run in the Ceph-Dashboard ``serve()`` method, right after CherryPy has been configured, but before it is started. It's a placeholder for the plug-in initialization logic.
- ``HasOptions``: requires overriding ``get_options()`` hook by returning a list of ``Options()``. The options returned here are added to the ``MODULE_OPTIONS``.
- ``HasCommands``: requires overriding ``register_commands()`` hook by defining the commands the plug-in can handle and decorating them with ``@CLICommand`. The commands can be optionally returned, so that they can be invoked externally (which makes unit testing easier).
- ``HasControllers``: requires overriding ``get_controllers()`` hook by defining and returning the controllers as usual.
- ``FilterRequest.BeforeHandler``: requires overriding ``filter_request_before_handler()`` hook. This method receives a ``cherrypy.request`` object for processing. A usual implementation of this method will allow some requests to pass or will raise a ``cherrypy.HTTPError` based on the ``request`` metadata and other conditions.

New interfaces and hooks should be added as soon as they are required to
implement new functionality. The above list only comprises the hooks needed for
the existing plugins.

A sample plugin implementation would look like this:

.. code-block:: python

  # src/pybind/mgr/dashboard/plugins/mute.py

  from . import PLUGIN_MANAGER as PM
  from . import interfaces as I

  from mgr_module import CLICommand, Option
  import cherrypy

  @PM.add_plugin
  class Mute(I.CanMgr, I.CanLog, I.Setupable, I.HasOptions,
                       I.HasCommands, I.FilterRequest.BeforeHandler,
                       I.HasControllers):
    @PM.add_hook
    def get_options(self):
      return [Option('mute', default=False, type='bool')]

    @PM.add_hook
    def setup(self):
      self.mute = self.mgr.get_module_options('mute')

    @PM.add_hook
    def register_commands(self):
      @CLICommand("dashboard mute")
      def _(mgr):
        self.mute = True
        self.mgr.set_module_options('mute', True)
        return 0

    @PM.add_hook
    def filter_request_before_handler(self, request):
      if self.mute:
        raise cherrypy.HTTPError(500, "I'm muted :-x")

    @PM.add_hook
    def get_controllers(self):
      from ..controllers import ApiController, RESTController

      @ApiController('/mute')
      class MuteController(RESTController):
        def get(_):
          return self.mute

      return [FeatureTogglesEndpoint]
