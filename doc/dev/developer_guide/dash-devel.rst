.. _dashdevel:

Ceph Dashboard Developer Documentation
======================================

.. contents:: Table of Contents

Feature Design
--------------

To promote collaboration on new Ceph Dashboard features, the first step is
the definition of a design document. These documents then form the basis of
implementation scope and permit wider participation in the evolution of the
Ceph Dashboard UI.

.. toctree::
   :maxdepth: 1
   :caption: Design Documents:

    UI Design Goals <../dashboard/ui_goals>


Preliminary Steps
-----------------

The following documentation chapters expect a running Ceph cluster and at
least a running ``dashboard`` manager module (with few exceptions). This
chapter gives an introduction on how to set up such a system for development,
without the need to set up a full-blown production environment. All options
introduced in this chapter are based on a so called ``vstart`` environment.

.. note::

  Every ``vstart`` environment needs Ceph `to be compiled`_ from its GitHub
  repository, though Docker environments simplify that step by providing a
  shell script that contains those instructions.

  One exception to this rule are the `build-free`_ capabilities of
  `ceph-dev`_. See below for more information.

.. _to be compiled: https://docs.ceph.com/docs/master/install/build-ceph/

vstart
~~~~~~

"vstart" is actually a shell script in the ``src/`` directory of the Ceph
repository (``src/vstart.sh``). It is used to start a single node Ceph
cluster on the machine where it is executed. Several required and some
optional Ceph internal services are started automatically when it is used to
start a Ceph cluster. vstart is the basis for the three most commonly used
development environments in Ceph Dashboard.

You can read more about vstart in :ref:`Deploying a development cluster
<dev_deploying_a_development_cluster>`. Additional information for developers
can also be found in the `Developer Guide`_.

.. _Developer Guide: https://docs.ceph.com/docs/master/dev/quick_guide/

Host-based vs Docker-based Development Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This document introduces you to three different development environments, all
based on vstart. Those are:

- vstart running on your host system

- vstart running in a Docker environment

  * ceph-dev-docker_
  * ceph-dev_

  Besides their independent development branches and sometimes slightly
  different approaches, they also differ with respect to their underlying
  operating systems.

  ========= ======================  ========
  Release   ceph-dev-docker         ceph-dev
  ========= ======================  ========
  Mimic     openSUSE Leap 15        CentOS 7
  Nautilus  openSUSE Leap 15        CentOS 7
  Octopus   openSUSE Leap 15.2      CentOS 8
  --------- ----------------------  --------
  Master    openSUSE Tumbleweed     CentOS 8
  ========= ======================  ========

.. note::

  Independently of which of these environments you will choose, you need to
  compile Ceph in that environment. If you compiled Ceph on your host system,
  you would have to recompile it on Docker to be able to switch to a Docker
  based solution. The same is true vice versa. If you previously used a
  Docker development environment and compiled Ceph there and you now want to
  switch to your host system, you will also need to recompile Ceph (or
  compile Ceph using another separate repository).

  `ceph-dev`_ is an exception to this rule as one of the options it provides
  is `build-free`_. This is accomplished through a Ceph installation using
  RPM system packages. You will still be able to work with a local GitHub
  repository like you are used to.


Development environment on your host system
...........................................

- No need to learn or have experience with Docker, jump in right away.

- Limited amount of scripts to support automation (like Ceph compilation).

- No pre-configured easy-to-start services (Prometheus, Grafana, etc).

- Limited amount of host operating systems supported, depending on which
  Ceph version is supposed to be used.

- Dependencies need to be installed on your host.

- You might find yourself in the situation where you need to upgrade your
  host operating system (for instance due to a change of the GCC version used
  to compile Ceph).


Development environments based on Docker
........................................

- Some overhead in learning Docker if you are not used to it yet.

- Both Docker projects provide you with scripts that help you getting started
  and automate recurring tasks.

- Both Docker environments come with partly pre-configured external services
  which can be used to attach to or complement Ceph Dashboard features, like

  - Prometheus
  - Grafana
  - Node-Exporter
  - Shibboleth
  - HAProxy

- Works independently of the operating system you use on your host.


.. _build-free: https://github.com/rhcs-dashboard/ceph-dev#quick-install-rpm-based

vstart on your host system
~~~~~~~~~~~~~~~~~~~~~~~~~~

The vstart script is usually called from your `build/` directory like so:

.. code::

  ../src/vstart.sh -n -d

In this case ``-n`` ensures that a new vstart cluster is created and that a
possibly previously created cluster isn't re-used. ``-d`` enables debug
messages in log files. There are several more options to chose from. You can
get a list using the ``--help`` argument.

At the end of the output of vstart, there should be information about the
dashboard and its URLs::

  vstart cluster complete. Use stop.sh to stop. See out/* (e.g. 'tail -f out/????') for debug output.

  dashboard urls: https://192.168.178.84:41259, https://192.168.178.84:43259, https://192.168.178.84:45259
    w/ user/pass: admin / admin
  restful urls: https://192.168.178.84:42259, https://192.168.178.84:44259, https://192.168.178.84:46259
    w/ user/pass: admin / 598da51f-8cd1-4161-a970-b2944d5ad200

During development (especially in backend development), you also want to
check on occasions if the dashboard manager module is still running. To do so
you can call `./bin/ceph mgr services` manually. It will list all the URLs of
successfully enabled services. Only URLs of services which are available over
HTTP(S) will be listed there. Ceph Dashboard is one of these services. It
should look similar to the following output:

.. code::

  $ ./bin/ceph mgr services
  {
      "dashboard": "https://home:41931/",
      "restful": "https://home:42931/"
  }

By default, this environment uses a randomly chosen port for Ceph Dashboard
and you need to use this command to find out which one it has become.

Docker
~~~~~~

Docker development environments usually ship with a lot of useful scripts.
``ceph-dev-docker`` for instance contains a file called `start-ceph.sh`,
which cleans up log files, always starts a Rados Gateway service, sets some
Ceph Dashboard configuration options and automatically runs a frontend proxy,
all before or after starting up your vstart cluster.

Instructions on how to use those environments are contained in their
respective repository README files.

- ceph-dev-docker_
- ceph-dev_

.. _ceph-dev-docker: https://github.com/ricardoasmarques/ceph-dev-docker
.. _ceph-dev: https://github.com/rhcs-dashboard/ceph-dev

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

 * Node 18.17.0 or higher
 * NPM 9.6.7 or higher

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

Run ``npm ci`` in directory ``src/pybind/mgr/dashboard/frontend`` to
install the required packages locally.

Adding or updating packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the following commands to add/update a package::

  npm install <PACKAGE_NAME>
  npm ci

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
stored in the ``dist/`` directory. Use the ``--prod`` flag for a
production build (``npm run build -- --prod``). Navigate to ``https://localhost:8443``.

Build the Code Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run ``npm run doc-build`` to generate code docs in the ``documentation/``
directory. To make them accessible locally for a web browser, run
``npm run doc-serve`` and they will become available at ``http://localhost:8444``.
With ``npm run compodoc -- <opts>`` you may
`fully configure it <https://compodoc.app/guides/usage.html>`_.

Code linting and formatting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use the following tools to lint and format the code in all our TS, SCSS and
HTML files:

- `codelyzer <http://codelyzer.com/>`_
- `html-linter <https://github.com/chinchiheather/html-linter>`_
- `htmllint-cli <https://github.com/htmllint/htmllint-cli>`_
- `Prettier <https://prettier.io/>`_
- `ESLint <https://eslint.org/>`_
- `stylelint <https://stylelint.io/>`_

We added 2 npm scripts to help run these tools:

- ``npm run lint``, will check frontend files against all linters
- ``npm run fix``, will try to fix all the detected linting errors

Ceph Dashboard and Bootstrap
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently we are using Bootstrap on the Ceph Dashboard as a CSS framework. This means that most of our SCSS and HTML
code can make use of all the utilities and other advantages Bootstrap is offering. In the past we often have used our
own custom styles and this lead to more and more variables with a single use and double defined variables which
sometimes are forgotten to be removed or it led to styling be inconsistent because people forgot to change a color or to
adjust a custom SCSS class.

To get the current version of Bootstrap used inside Ceph please refer to the ``package.json`` and search for:

- ``bootstrap``: For the Bootstrap version used.
- ``@ng-bootstrap``: For the version of the Angular bindings which we are using.

So for the future please do the following when visiting a component:

- Does this HTML/SCSS code use custom code? - If yes: Is it needed? --> Clean it up before changing the things you want
  to fix or change.
- If you are creating a new component: Please make use of Bootstrap as much as reasonably possible! Don't try to
  reinvent the wheel.
- If possible please look up if Bootstrap has guidelines on how to extend it properly to do achieve what you want to
  achieve.

The more bootstrap alike our code is the easier it is to theme, to maintain and the less bugs we will have. Also since
Bootstrap is a framework which tries to have usability and user experience in mind we increase both points
exponentially. The biggest benefit of all is that there is less code for us to maintain which makes it easier to read
for beginners and even more easy for people how are already familiar with the code.

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

Run ``npm run test`` to execute the unit tests via `Jest
<https://facebook.github.io/jest/>`_.

If you get errors on all tests, it could be because `Jest
<https://facebook.github.io/jest/>`__ or something else was updated.
There are a few ways how you can try to resolve this:

- Remove all modules with ``rm -rf dist node_modules`` and run ``npm install``
  again in order to reinstall them
- Clear the cache of jest by running ``npx jest --clearCache``

Running End-to-End (E2E) Tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use `Cypress <https://www.cypress.io/>`__ to run our frontend E2E tests.

E2E Prerequisites
.................

You need to previously build the frontend.

In some environments, depending on your user permissions and the CYPRESS_CACHE_FOLDER,
you might need to run ``npm ci`` with the ``--unsafe-perm`` flag.

You might need to install additional packages to be able to run Cypress.
Please run ``npx cypress verify`` to verify it.

run-frontend-e2e-tests.sh
.........................

Our ``run-frontend-e2e-tests.sh`` script is the go to solution when you wish to
do a full scale e2e run.
It will verify if everything needed is installed, start a new vstart cluster
and run the full test suite.

Start all frontend E2E tests with::

  $ cd src/pybind/mgr/dashboard
  $ ./run-frontend-e2e-tests.sh

Report:
  You can follow the e2e report on the terminal and you can find the screenshots
  of failed test cases by opening the following directory::

    src/pybind/mgr/dashboard/frontend/cypress/screenshots/

Device:
  You can force the script to use a specific device with the ``-d`` flag::

    $ ./run-frontend-e2e-tests.sh -d <chrome|chromium|electron|docker>

Remote:
  By default this script will stop and start a new vstart cluster.
  If you want to run the tests outside the ceph environment, you will need to
  manually define the dashboard url using ``-r`` and, optionally, credentials
  (``-u``, ``-p``)::

    $ ./run-frontend-e2e-tests.sh -r <DASHBOARD_URL> -u <E2E_LOGIN_USER> -p <E2E_LOGIN_PWD>

Note:
  When using docker, as your device, you might need to run the script with sudo
  permissions.

run-cephadm-e2e-tests.sh
.........................

``run-cephadm-e2e-tests.sh`` runs a subset of E2E tests to verify that the Dashboard and cephadm as
Orchestrator backend behave correctly.

Prerequisites: you need to install `KCLI
<https://kcli.readthedocs.io/en/latest/>`_ and Node.js in your local machine.

Configure KCLI plan requirements::

  $ sudo chown -R $(id -un) /var/lib/libvirt/images
  $ mkdir -p /var/lib/libvirt/images/ceph-dashboard
  $ kcli create pool -p /var/lib/libvirt/images/ceph-dashboard ceph-dashboard
  $ kcli create network -c 192.168.100.0/24 ceph-dashboard

Note:
  This script is aimed to be run as jenkins job so the cleanup is triggered only in a jenkins
  environment. In local, the user will shutdown the cluster when desired (i.e. after debugging).

Start E2E tests by running::

  $ cd <your/ceph/repo/dir>
  $ sudo chown -R $(id -un) src/pybind/mgr/dashboard/frontend/{dist,node_modules,src/environments}
  $ ./src/pybind/mgr/dashboard/ci/cephadm/run-cephadm-e2e-tests.sh

Note:
  In fedora 35, there can occur a permission error when trying to mount the shared_folders. This can be
  fixed by running::

    $ sudo setfacl -R -m u:qemu:rwx <abs-path-to-your-user-home>

  or also by setting the appropriate permission to your $HOME directory

You can also start a cluster in development mode (so the frontend build starts in watch mode and you
only have to reload the page for the changes to be reflected) by running::

  $ ./src/pybind/mgr/dashboard/ci/cephadm/start-cluster.sh --dev-mode

Note:
  Add ``--expanded`` if you need a cluster ready to deploy services (one with enough monitor
  daemons spread across different hosts and enough OSDs).

Test your changes by running:

  $ ./src/pybind/mgr/dashboard/ci/cephadm/run-cephadm-e2e-tests.sh

Shutdown the cluster by running:

  $ kcli delete plan -y ceph
  $ # In development mode, also kill the npm build watch process (e.g., pkill -f "ng build")

Other running options
.....................

During active development, it is not recommended to run the previous script,
as it is not prepared for constant file changes.
Instead you should use one of the following commands:

- ``npm run e2e`` - This will run ``ng serve`` and open the Cypress Test Runner.
- ``npm run e2e:ci`` - This will run ``ng serve`` and run the Cypress Test Runner once.
- ``npx cypress run`` - This calls cypress directly and will run the Cypress Test Runner.
  You need to have a running frontend server.
- ``npx cypress open`` - This calls cypress directly and will open the Cypress Test Runner.
  You need to have a running frontend server.

Calling Cypress directly has the advantage that you can use any of the available
`flags <https://docs.cypress.io/guides/guides/command-line.html#cypress-run>`__
to customize your test run and you don't need to start a frontend server each time.

Using one of the ``open`` commands, will open a cypress application where you
can see all the test files you have and run each individually.
This is going to be run in watch mode, so if you make any changes to test files,
it will retrigger the test run.
This cannot be used inside docker, as it requires X11 environment to be able to open.

By default Cypress will look for the web page at ``https://localhost:4200/``.
If you are serving it in a different URL you will need to configure it by
exporting the environment variable CYPRESS_BASE_URL with the new value.
E.g.: ``CYPRESS_BASE_URL=https://localhost:41076/ npx cypress open``

CYPRESS_CACHE_FOLDER
.....................

When installing cypress via npm, a binary of the cypress app will also be
downloaded and stored in a cache folder.
This removes the need to download it every time you run ``npm ci`` or even when
using cypress in a separate project.

By default Cypress uses ~/.cache to store the binary.
To prevent changes to the user home directory, we have changed this folder to
``/ceph/build/src/pybind/mgr/dashboard/cypress``, so when you build ceph or run
``run-frontend-e2e-tests.sh`` this is the directory Cypress will use.

When using any other command to install or run cypress,
it will go back to the default directory. It is recommended that you export the
CYPRESS_CACHE_FOLDER environment variable with a fixed directory, so you always
use the same directory no matter which command you use.


Writing End-to-End Tests
~~~~~~~~~~~~~~~~~~~~~~~~

The PagerHelper class
.....................

The ``PageHelper`` class is supposed to be used for general purpose code that
can be used on various pages or suites.

Examples are

- ``navigateTo()`` - Navigates to a specific page and waits for it to load
- ``getFirstTableCell()`` - returns the first table cell. You can also pass a
  string with the desired content and it will return the first cell that
  contains it.
- ``getTabsCount()`` - returns the amount of tabs

Every method that could be useful on several pages belongs there. Also, methods
which enhance the derived classes of the PageHelper belong there. A good
example for such a case is the ``restrictTo()`` decorator. It ensures that a
method implemented in a subclass of PageHelper is called on the correct page.
It will also show a developer-friendly warning if this is not the case.

Subclasses of PageHelper
........................

Helper Methods
""""""""""""""

In order to make code reusable which is specific for a particular suite, make
sure to put it in a derived class of the ``PageHelper``. For instance, when
talking about the pool suite, such methods would be ``create()``, ``exist()``
and ``delete()``. These methods are specific to a pool but are useful for other
suites.

Methods that return HTML elements which can only be found on a specific page,
should be either implemented in the helper methods of the subclass of PageHelper
or as own methods of the subclass of PageHelper.

Using PageHelpers
"""""""""""""""""

In any suite, an instance of the specific ``Helper`` class should be
instantiated and called directly.

.. code:: TypeScript

  const pools = new PoolPageHelper();

  it('should create a pool', () => {
    pools.exist(poolName, false);
    pools.navigateTo('create');
    pools.create(poolName, 8);
    pools.exist(poolName, true);
  });

Code Style
..........

Please refer to the official `Cypress Core Concepts
<https://docs.cypress.io/guides/core-concepts/introduction-to-cypress.html#Cypress-Can-Be-Simple-Sometimes>`__
for a better insight on how to write and structure tests.

``describe()`` vs ``it()``
""""""""""""""""""""""""""

Both ``describe()`` and ``it()`` are function blocks, meaning that any
executable code necessary for the test can be contained in either block.
However, Typescript scoping rules still apply, therefore any variables declared
in a ``describe`` are available to the ``it()`` blocks inside of it.

``describe()`` typically are containers for tests, allowing you to break tests
into multiple parts. Likewise, any setup that must be made before your tests are
run can be initialized within the ``describe()`` block. Here is an example:

.. code:: TypeScript

  describe('create, edit & delete image test', () => {
    const poolName = 'e2e_images_pool';

    before(() => {
      cy.login();
      pools.navigateTo('create');
      pools.create(poolName, 8, 'rbd');
      pools.exist(poolName, true);
    });

    beforeEach(() => {
      cy.login();
      images.navigateTo();
    });

    //...

  });

As shown, we can initiate the variable ``poolName`` as well as run commands
before our test suite begins (creating a pool). ``describe()`` block messages
should include what the test suite is.

``it()`` blocks typically are parts of an overarching test. They contain the
functionality of the test suite, each performing individual roles.
Here is an example:

.. code:: TypeScript

  describe('create, edit & delete image test', () => {
    //...

    it('should create image', () => {
      images.createImage(imageName, poolName, '1');
      images.getFirstTableCell(imageName).should('exist');
    });

    it('should edit image', () => {
      images.editImage(imageName, poolName, newImageName, '2');
      images.getFirstTableCell(newImageName).should('exist');
    });

    //...
  });

As shown from the previous example, our ``describe()`` test suite is to create,
edit and delete an image. Therefore, each ``it()`` completes one of these steps,
one for creating, one for editing, and so on. Likewise, every ``it()`` blocks
message should be in lowercase and written so long as "it" can be the prefix of
the message. For example, ``it('edits the test image' () => ...)`` vs.
``it('image edit test' () => ...)``. As shown, the first example makes
grammatical sense with ``it()`` as the prefix whereas the second message does
not. ``it()`` should describe what the individual test is doing and what it
expects to happen.


Visual Regression Testing
~~~~~~~~~~~~~~~~~~~~~~~~~

For visual regression testing, we use `Applitools Eyes <https://applitools.com/products-eyes/>`_
an AI powered automated  visual regression testing tool.
Applitools integrates with our existing Cypress E2E tests.
The tests currently are located at: ``ceph/src/pybind/mgr/dashboard/frontend/cypress/integration/visualTests`` and
follow the naming convention: ``<component-name>.vrt-spec.ts``.

Running Visual Regression Tests Locally
.......................................

To run the tests locally, you'll need an Applitools API key, if you don't have one, you can sign up
for a free account. After obtaining the API key, export it as an environment variable: ``APPLITOOLS_API_KEY``.

Now you can run the tests like normal cypress E2E tests, using either ``npx cypress open`` or in headless mode by running ``npx cypress run``.

Capturing Screenshots
.....................

Baseline screenshots are the screenshots against which checkpoint screenshots
(or the screenshots from your feature branch) will be tested.

To capture baseline screenshots, you can run the tests against the master branch,
and then switch to your feature branch and run the tests again to capture checkpoint screenshots.

Now to see your screenshots, login to applitools.com and on the landing page you'll be greeted with
applitools eyes test runner, where you can see all your screenshots. And if there's any visual regression or difference (diff) between your baseline and checkpoint screenshots, they'll be highlighted with a mask over the diff.

Writing More Visual Regression Tests
....................................

Please refer to `Applitools's official cypress sdk documentation <https://www.npmjs.com/package/@applitools/eyes-cypress#usage>`_ to write more tests.

Visual Regression Tests In Jenkins
..................................

Currently, all visual regression tests are being run under `ceph dashboard tests <https://jenkins.ceph.com/job/ceph-dashboard-pull-requests>`_ GitHub check in the Jenkins job.

Accepting or Rejecting Differences
..................................

Currently, only the ceph dashboard team has read and write access to the applitools test runner. If any differences are reported by the tests, and you want to accept them and update the baseline screenshots, or if the differences are due to a genuine regression you can fail them. To perform the above actions, please follow `this <https://applitools.com/docs/topics/test-manager/pages/page-test-results/tm-accepting-and-rejecting-steps.html>`_ guide.

Debugging Regressions
.....................

If you're running the tests locally and regressions are reported, you can take advantage of `Applitools's Root Cause Analysis feature <https://applitools.com/docs/topics/test-manager/viewers/root-cause-analysis.html>`_ to find the cause of the regression.


Differences between Frontend Unit Tests and End-to-End (E2E) Tests / FAQ
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

General introduction about testing and E2E/unit tests


What are E2E/unit tests designed for?
.....................................

E2E test:

It requires a fully functional system and tests the interaction of all components
of the application (Ceph, back-end, front-end).
E2E tests are designed to mimic the behavior of the user when interacting with the application
- for example when it comes to workflows like creating/editing/deleting an item.
Also the tests should verify that certain items are displayed as a user would see them
when clicking through the UI (for example a menu entry or a pool that has been
created during a test and the pool and its properties should be displayed in the table).

Angular Unit Tests:

Unit tests, as the name suggests, are tests for smaller units of the code.
Those tests are designed for testing all kinds of Angular components (e.g. services, pipes etc.).
They do not require a connection to the backend, hence those tests are independent of it.
The expected data of the backend is mocked in the frontend and by using this data
the functionality of the frontend can be tested without having to have real data from the backend.
As previously mentioned, data is either mocked or, in a simple case, contains a static input,
a function call and an expected static output.
More complex examples include the state of a component (attributes of the component class),
that define how the output changes according to the given input.

Which E2E/unit tests are considered to be valid?
................................................

This is not easy to answer, but new tests that are written in the same way as already existing
dashboard tests should generally be considered valid.
Unit tests should focus on the component to be tested.
This is either an Angular component, directive, service, pipe, etc.

E2E tests should focus on testing the functionality of the whole application.
Approximately a third of the overall E2E tests should verify the correctness
of user visible elements.

How should an E2E/unit test look like?
......................................

Unit tests should focus on the described purpose
and shouldn't try to test other things in the same `it` block.

E2E tests should contain a description that either verifies
the correctness of a user visible element or a complete process
like for example the creation/validation/deletion of a pool.

What should an E2E/unit test cover?
...................................

E2E tests should mostly, but not exclusively, cover interaction with the backend.
This way the interaction with the backend is utilized to write integration tests.

A unit test should mostly cover critical or complex functionality
of a component (Angular Components, Services, Pipes, Directives, etc).

What should an E2E/unit test NOT cover?
.......................................

Avoid duplicate testing: do not write E2E tests for what's already
been covered as frontend-unit tests and vice versa.
It may not be possible to completely avoid an overlap.

Unit tests should not be used to extensively click through components and E2E tests
shouldn't be used to extensively test a single component of Angular.

Best practices/guideline
........................

As a general guideline we try to follow the 70/20/10 approach - 70% unit tests,
20% integration tests and 10% end-to-end tests.
For further information please refer to `this document
<https://testing.googleblog.com/2015/04/just-say-no-to-more-end-to-end-tests.html>`__
and the included "Testing Pyramid".

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

    import { ToastrManager } from 'ngx-toastr';

    import { Credentials } from '../../../shared/models/credentials.model';
    import { HostService } from './services/host.service';

Frontend components
~~~~~~~~~~~~~~~~~~~

There are several components that can be reused on different pages.
This components are declared on the components module:
`src/pybind/mgr/dashboard/frontend/src/app/shared/components`.

Helper
~~~~~~

This component should be used to provide additional information to the user.

Example:

.. code:: html

    <cd-helper>
      Some <strong>helper</strong> html text
    </cd-helper>

Terminology and wording
~~~~~~~~~~~~~~~~~~~~~~~

Instead of using the Ceph component names, the approach
suggested is to use the logical/generic names (Block over RBD, Filesystem over
CephFS, Object over RGW). Nevertheless, as Ceph-Dashboard cannot completely hide
the Ceph internals, some Ceph-specific names might remain visible.

Regarding the wording for action labels and other textual elements (form titles,
buttons, etc.), the chosen approach is to follow `these guidelines
<https://www.patternfly.org/styles/terminology-and-wording/#terminology-and-wording-for-action-labels>`_.
As a rule of thumb, 'Create' and 'Delete' are the proper wording for most forms,
instead of 'Add' and 'Remove', unless some already created item is either added
or removed to/from a set of items (e.g.: 'Add permission' to a user vs. 'Create
(new) permission').

In order to enforce the use of this wording, a service ``ActionLabelsI18n`` has
been created, which provides translated labels for use in UI elements.

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

To change the variable defaults or add your own ones you can overwrite them in
``./frontend/src/styles/vendor/_variables.scss``.
Just reassign the variable you want to change, for example ``$color-primary: teal;``
To overwrite or extend the default CSS, you can add your own styles in
``./frontend/src/styles/vendor/_style-overrides.scss``.

UI Style Guide
~~~~~~~~~~~~~~

The style guide is created to document Ceph Dashboard standards and maintain
consistency across the project. Its an effort to make it easier for
contributors to process designing and deciding mockups and designs for
Dashboard.

The development environment for Ceph Dashboard has live reloading enabled so
any changes made in UI are reflected in open browser windows. Ceph Dashboard
uses Bootstrap as the main third-party CSS library.

Avoid duplication of code. Be consistent with the existing UI by reusing
existing SCSS declarations as much as possible.

Always check for existing code similar to what you want to write.
You should always try to keep the same look-and-feel as the existing code.

Colors
......

All the colors used in Ceph Dashboard UI are listed in
`frontend/src/styles/defaults/_bootstrap-defaults.scss`. If using new color
always define color variables in the `_bootstrap-defaults.scss` and
use the variable instead of hard coded color values so that changes to the
color are reflected in similar UI elements.

The main color for the Ceph Dashboard is `$primary`. The primary color is
used in navigation components and as the `$border-color` for input components of
form.

The secondary color is `$secondary` and is the background color for Ceph
Dashboard.

Buttons
.......

Buttons are used for performing actions such as: “Submit”, “Edit, “Create" and
“Update”.

**Forms:** When using to submit forms anywhere in the Dashboard, the main action
button should use the `cd-submit-button` component and the secondary button should
use `cd-back-button` component. The text on the action button should be same as the
form title and follow a title case. The text on the secondary button should be
`Cancel`. `Perform action` button should always be on right while `Cancel`
button should always be on left.

**Modals**: The main action button should use the `cd-submit-button` component and
the secondary button should use `cd-back-button` component. The text on the action
button should follow a title case and correspond to the action to be performed.
The text on the secondary button should be `Close`.

**Disclosure Button:** Disclosure buttons should be used to allow users to
display and hide additional content in the interface.

**Action Button**: Use the action button to perform actions such as edit or update
a component. All action button should have an icon corresponding to the actions they
perform and button text should follow title case. The button color should be the
same as the form's main button color.

**Drop Down Buttons:** Use dropdown buttons to display predefined lists of
actions. All drop down buttons have icons corresponding to the action they
perform.

Links
.....

Use text hyperlinks as navigation to guide users to a new page in the application
or to anchor users to a section within a page. The color of the hyperlinks
should be `$primary`.

Forms
.....

Mark invalid form fields with red outline and show a meaningful error message.
Use red as font color for message and be as specific as possible.
`This field is required.` should be the exact error message for required fields.
Mark valid forms with a green outline and a green tick at the end of the form.
Sections should not have a bigger header than the parent.

Modals
......

Blur any interface elements in the background to bring the modal content into
focus. The heading of the modal should reflect the action it can perform and
should be clearly mentioned at the top of the modal. Use `cd-back-button`
component in the footer for closing the modal.

Icons
.....

We use `Fork Awesome <https://forkaweso.me/Fork-Awesome/>`_ classes for icons.
We have a list of used icons in `src/app/shared/enum/icons.enum.ts`, these
should be referenced in the HTML, so its easier to change them later. When
icons are next to text, they should be center-aligned horizontally. If icons
are stacked, they should also be center-aligned vertically. Use small icons
with buttons. For notifications use large icons.

Navigation
..........

For local navigation use tabs. For overall navigation use expandable vertical
navigation to collapse and expand items as needed.

Alerts and notifications
........................

Default notification should have `text-info` color. Success notification should
have `text-success` color. Failure notification should have `text-danger` color.

Error Handling
~~~~~~~~~~~~~~

For handling front-end errors, there is a generic Error Component which can be
found in ``./src/pybind/mgr/dashboard/frontend/src/app/core/error``. For
reporting a new error, you can simply extend the ``DashboardError`` class
in ``error.ts`` file and add specific header and message for the new error. Some
generic error classes are already in place such as ``DashboardNotFoundError``
and ``DashboardForbiddenError`` which can be called and reused in different
scenarios.

For example - ``throw new DashboardNotFoundError()``.

Internationalization (i18n)
---------------------------

How to extract messages from source code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To extract the I18N messages from the templates and the TypeScript files just
run the following command in ``src/pybind/mgr/dashboard/frontend``::

  $ npm run i18n:extract

This will extract all marked messages from the HTML templates first and then
add all marked strings from the TypeScript files to the translation template.
Since the extraction from TypeScript files is still not supported by Angular
itself, we are using the
`ngx-translator <https://github.com/ngx-translate/i18n-polyfill>`_ extractor to
parse the TypeScript files.

When the command ran successfully, it should have created or updated the file
``src/locale/messages.xlf``.

The file isn't tracked by git, you can just use it to start with the
translation offline or add/update the resource files on transifex.

Supported languages
~~~~~~~~~~~~~~~~~~~

All our supported languages should be registered in both exports in
``supported-languages.enum.ts`` and have a corresponding test in
``language-selector.component.spec.ts``.

The ``SupportedLanguages`` enum will provide the list for the default language selection.

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

To do that, check the settings in the i18n config file
``src/pybind/mgr/dashboard/frontend/i18n.config.json``:: and make sure that the
organization is *ceph*, the project is *ceph-dashboard* and the resource is
the one you want to pull from and push to e.g. *Master:master*. To find a list
of available resources visit `<https://www.transifex.com/ceph/ceph-dashboard/content/>`_.

After you checked the config go to the directory ``src/pybind/mgr/dashboard/frontend`` and run::

  $ npm run i18n

This command will extract all marked messages from the HTML templates and
TypeScript files. Once the source file has been created it will push it to
transifex and pull the latest translations. It will also fill all the
untranslated strings with the source string.
The tool will ask you for an api token, unless you added it by running:

  $ npm run i18n:token

To create a transifex api token visit `<https://www.transifex.com/user/settings/api/>`_.

After the command ran successfully, build the UI and check if everything is
working as expected. You also might want to run the frontend tests.

Add a new release resource to transifex
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to organize the translations, we create a
`transifex resource <https://www.transifex.com/ceph/ceph-dashboard/content/>`_
for every Ceph release. This means, once a new version has been released, the
``src/pybind/mgr/dashboard/frontend/i18n.config.json`` needs to be updated on
the release branch.

Please replace::

"resource": "Master:master"

by::

"resource": "<Release-name>:<release-name>"

E.g. the resource definition for the pacific release::

"resource": "Pacific:pacific"

Note:
  The first part of the resource definition (before the colon) needs to be
  written with a capital letter.

Suggestions
~~~~~~~~~~~

Strings need to start and end in the same line as the element:

.. code-block:: html

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

.. code-block:: html

  <!-- avoid -->
  <span i18n>{{ foo }}</span>

  <!-- recommended -->
  <span>{{ foo }}</span>

Interpolations used in a sentence should be kept in the translation:

.. code-block:: html

  <!-- recommended -->
  <span i18n>There are {{ x }} OSDs.</span>

Remove elements that are outside the context of the translation:

.. code-block:: html

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

.. code-block:: html

  <!-- recommended -->
  <span i18n>Profile <b>foo</b> will be removed.</span>


.. _accessibility:

Accessibility
-------------

Many parts of the Ceph Dashboard are modeled on `Web Content Accessibility Guidelines (WCAG) 2.1 <https://www.w3.org/TR/WCAG21/>`_  level A accessibility conformance guidelines. 
By implementing accessibility best practices, you are improving the usability of the Ceph Dashboard for blind and visually impaired users.

Summary
~~~~~~~

A few things you should check before introducing a new code change include:

1) Add `ARIA labels and descriptions <https://www.w3.org/TR/wai-aria/>`_ to actionable HTML elements.
2) Don't forget to tag ARIA labels/descriptions or any user-readable text for translation (i18n-title, i18n-aria-label...).
3) Add `ARIA roles <https://www.w3.org/TR/wai-aria/#usage_intro>`_ to tag HTML elements that behave different from their intended behaviour (<a> tags behaving as <buttons>) or that provide extended behaviours (roles).
4) Avoid poor `color contrast choices <https://www.w3.org/TR/WCAG21/#contrast-minimum>`_ (foreground-background) when styling a component. Here are some :ref:`tools <color-contrast-checkers>` you can use.
5) When testing menus or dropdowns, be sure to scan them with an :ref:`accessibility checker <accessibility-checkers>` in both opened and closed states. Sometimes issues are hidden when menus are closed.

.. _accessibility-checkers:

Accessibility checkers
~~~~~~~~~~~~~~~~~~~~~~

During development, you can test the accessibility compliance of your features using one of the tools below:

- `Accessibility insights plugin <https://accessibilityinsights.io/downloads/>`_
- `Site Improve plugin <https://www.siteimprove.com/integrations/browser-extensions/>`_
- `Axe devtools <https://www.deque.com/axe/devtools/>`_

Testing with two or more of these tools can greatly improve the detection of accessibility violations.

.. _color-contrast-checkers:

Color contrast checkers
~~~~~~~~~~~~~~~~~~~~~~~

When adding new colors, making sure they are accessible is also important. Here are some tools which can help with color contrast testing:

- `Accessible web color-contrast checker <https://accessibleweb.com/color-contrast-checker/>`_
- `Colorsafe generator <https://colorsafe.co/>`_

Accessibility linters
~~~~~~~~~~~~~~~~~~~~~

If you use VSCode, you may install the `axe accessibility linter <https://marketplace.visualstudio.com/items?itemName=deque-systems.vscode-axe-linter>`_,
which can help you catch and fix potential issues during development.

Accessibility testing
~~~~~~~~~~~~~~~~~~~~~

Our e2e testing suite, which is based on Cypress, supports the addition of accessibility tests using `axe-core <https://github.com/dequelabs/axe-core>`_ 
and `cypress-axe <https://github.com/component-driven/cypress-axe>`_. A custom Cypress command, `cy.checkAccessibility`, can also be used directly. 
This is a great way to prevent accessibility regressions on high impact components.

Tests can be found under the `a11y folder <./src/pybind/mgr/dashboard/frontend/cypress/integration/a11y>`_ in the dashboard. Here is an example:

.. code:: TypeScript

  describe('Navigation accessibility', { retries: 0 }, () => {
    const shared = new NavigationPageHelper();
  
    beforeEach(() => {
      cy.login();
      shared.navigateTo();
    });
  
    it('top-nav should have no accessibility violations', () => {
      cy.injectAxe();
      cy.checkAccessibility('.cd-navbar-top');
    });
  
    it('sidebar should have no accessibility violations', () => {
      cy.injectAxe();
      cy.checkAccessibility('nav[id=sidebar]');
    });
  
  });

Additional guidelines
~~~~~~~~~~~~~~~~~~~~~

If you're unsure about which UI pattern to follow in order to implement an accessibility fix, `patternfly <https://www.patternfly.org/v4/accessibility/accessibility-fundamentals>`_ guidelines can be used.

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
Python 3, as well as linting tools to guarantee the uniformity of code.

You need to install ``tox`` and ``coverage`` before running it. To install the
packages in your system, either install it via your operating system's package
management tools, e.g. by running ``dnf install python-tox python-coverage`` on
Fedora Linux.

Alternatively, you can use Python's native package installation method::

  $ pip install tox
  $ pip install coverage

To run the tests, run ``src/script/run_tox.sh`` in the dashboard directory (where
``tox.ini`` is located)::

  ## Run Python 3 tests+lint commands:
  $ ../../../script/run_tox.sh --tox-env py3,lint,check

  ## Run Python 3 arbitrary command (e.g. 1 single test):
  $ ../../../script/run_tox.sh --tox-env py3 "" tests/test_rgw_client.py::RgwClientTest::test_ssl_verify

You can also run tox instead of ``run_tox.sh``::

  ## Run Python 3 tests command:
  $ tox -e py3

  ## Run Python 3 arbitrary command (e.g. 1 single test):
  $ tox -e py3 tests/test_rgw_client.py::RgwClientTest::test_ssl_verify

Python files can be automatically fixed and formatted according to PEP8
standards by using ``run_tox.sh --tox-env fix`` or ``tox -e fix``.

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
path and query parameters respectively.
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
      """..."""

    # URL: /ping/{key}?opt1=...&opt2=...
    @Endpoint(query_params=['opt1'])
    def __call__(self, key, opt1, opt2=None):
      """..."""

    # URL: /ping/post/{key1}/{key2}
    @Endpoint('POST', path_params=['key1', 'key2'])
    def post(self, key1, key2, data1, data2=None):
      """..."""


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
      """ ..."""

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
      """
      if requested URL is "/foo/proxy/access/service?opt=1"
      then path is "access/service" and params is {'opt': '1'}
      """


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

To use a custom endpoint for the above listed methods, you can
use ``@RESTController.MethodMap``

.. code-block:: python

  import cherrypy
  from ..tools import ApiController, RESTController

    @RESTController.MethodMap(version='0.1')
    def create(self):
      return {"msg": "Hello"}

This decorator supports three parameters to customize the
endpoint:

* ``resource"``: resource id.
* ``status=200``: set the HTTP status response code
* ``version``: version

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

Both decorators also support five parameters to customize the
endpoint:

* ``method="GET"``: the HTTP method allowed to access this endpoint.
* ``path="/<method_name>"``: the URL path of the endpoint, excluding the
  controller URL path prefix.
* ``status=200``: set the HTTP status response code
* ``query_params=[]``: list of method parameter names that correspond to URL
  query parameters.
* ``version``: version

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

How to create a dedicated UI endpoint which uses the 'public' API?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes we want to combine multiple calls into one single call
to save bandwidth or for other performance reasons.
In order to achieve that, we first have to create an ``@UiApiController`` which
is used for endpoints consumed by the UI but that are not part of the
'public' API. Let the ui class inherit from the REST controller class.
Now you can use all methods from the api controller.

Example:

.. code-block:: python

  import cherrypy
  from . import UiApiController, ApiController, RESTController


  @ApiController('ping', secure=False)  # /api/ping
  class Ping(RESTController):
    def list(self):
      return self._list()

    def _list(self):  # To not get in conflict with the JSON wrapper
      return [1,2,3]


  @UiApiController('ping', secure=False)  # /ui-api/ping
  class PingUi(Ping):
    def list(self):
      return self._list() + [4, 5, 6]

How to access the manager module instance from a controller?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We provide the manager module instance as a global variable that can be
imported in any module.

Example:

.. code-block:: python

  import logging
  import cherrypy
  from .. import mgr
  from ..tools import ApiController, RESTController

  logger = logging.getLogger(__name__)

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
          cp_config = {'tools.authenticate.on': True}
          cls.setup_controllers([Ping], cp_config=cp_config)

      def test_ping(self):
          self._get("/api/ping")
          self.assertStatus(200)
          self.assertJsonBody({'msg': 'Hello'})

The ``ControllerTestCase`` class starts by initializing a CherryPy webserver.
Then it will call the ``setup_test()`` class method where we can explicitly
load the controllers that we want to test. In the above example we are only
loading the ``Ping`` controller. We can also provide ``cp_config`` in order to
update the controller's cherrypy config (e.g. enable authentication as shown in the example).

How to update or create new dashboards in grafana?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We are using ``jsonnet`` and ``grafonnet-lib`` to write code for the grafana dashboards.
All the dashboards are written inside ``grafana_dashboards.jsonnet`` file in the
monitoring/grafana/dashboards/jsonnet directory.

We generate the dashboard json files directly from this jsonnet file by running this
command in the grafana/dashboards directory:
``jsonnet -m . jsonnet/grafana_dashboards.jsonnet``.
(For the above command to succeed we need ``jsonnet`` package installed and ``grafonnet-lib``
directory cloned in our machine. Please refer -
``https://grafana.github.io/grafonnet-lib/getting-started/`` in case you have some trouble.)

To update an existing grafana dashboard or to create a new one, we need to update
the ``grafana_dashboards.jsonnet`` file and generate the new/updated json files using the
above mentioned command. For people who are not familiar with grafonnet or jsonnet implementation
can follow this doc - ``https://grafana.github.io/grafonnet-lib/``.

Example grafana dashboard in jsonnet format:

To specify the grafana dashboard properties such as title, uid etc we can create a local function -

::

    local dashboardSchema(title, uid, time_from, refresh, schemaVersion, tags,timezone, timepicker)

To add a graph panel we can specify the graph schema in a local function such as -

::

    local graphPanelSchema(title, nullPointMode, stack, formatY1, formatY2, labelY1, labelY2, min, fill, datasource)

and then use these functions inside the dashboard definition like -

::

    {
        radosgw-sync-overview.json: //json file name to be generated

        dashboardSchema(
          'RGW Sync Overview', 'rgw-sync-overview', 'now-1h', '15s', .., .., ..
        )

        .addPanels([
          graphPanelSchema(
            'Replication (throughput) from Source Zone', 'Bps', null, .., .., ..)
        ])
    }

The valid grafonnet-lib attributes can be found here - ``https://grafana.github.io/grafonnet-lib/api-docs/``.


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
dashboard module, you will automatically gain two additional CLI commands to
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


REST API documentation
~~~~~~~~~~~~~~~~~~~~~~
Ceph-Dashboard provides two types of documentation for the **Ceph RESTful API**:

* **Static documentation**: available at :ref:`mgr ceph api`. This comes from a versioned specification located at ``src/pybind/mgr/dashboard/openapi.yaml``.
* **Interactive documentation**: available from a running Ceph-Dashboard instance (top-right ``?`` icon > API Docs).

If changes are made to the ``controllers/`` directory, it's very likely that
they will result in changes to the generated OpenAPI specification. For that
reason, a checker has been implemented to block unintended changes. This check
is automatically triggered by the Pull Request CI (``make check``) and can be
also manually invoked: ``tox -e openapi-check``.

If that checker failed, it means that the current Pull Request is modifying the
Ceph API and therefore:

#. The versioned OpenAPI specification should be updated explicitly: ``tox -e openapi-fix``.
#. The team @ceph/api will be requested for reviews (this is automated via GitHub CODEOWNERS), in order to asses the impact of changes.

Additionally, Sphinx documentation can be generated from the OpenAPI
specification with ``tox -e openapi-doc``.

The Ceph RESTful OpenAPI specification is dynamically generated from the
``Controllers`` in ``controllers/`` directory.  However, by default it is not
very detailed, so there are two decorators that can and should be used to add
more information:

* ``@EndpointDoc()`` for documentation of endpoints. It has four optional arguments
  (explained below): ``description``, ``group``, ``parameters`` and
  ``responses``.
* ``@ControllerDoc()`` for documentation of controller or group associated with
  the endpoints. It only takes the two first arguments: ``description`` and
  ``group``.


``description``: A a string with a short (1-2 sentences) description of the object.


``group``: By default, an endpoint is grouped together with other endpoints
within the same controller class. ``group`` is a string that can be used to
assign an endpoint or all endpoints in a class to another controller or a
conceived group name.


``parameters``: A dict used to describe path, query or request body parameters.
By default, all parameters for an endpoint are listed on the Swagger UI page,
including information of whether the parameter is optional/required and default
values. However, there will be no description of the parameter and the parameter
type will only be displayed in some cases.
When adding information, each parameters should be described as in the example
below. Note that the parameter type should be expressed as a built-in python
type and not as a string. Allowed values are ``str``, ``int``, ``bool``, ``float``.

.. code-block:: python

 @EndpointDoc(parameters={'my_string': (str, 'Description of my_string')})
 def method(my_string): pass

For body parameters, more complex cases are possible. If the parameter is a
dictionary, the type should be replaced with a ``dict`` containing its nested
parameters. When describing nested parameters, the same format as other
parameters is used. However, all nested parameters are set as required by default.
If the nested parameter is optional this must be specified as for ``item2`` in
the example below. If a nested parameters is set to optional, it is also
possible to specify the default value (this will not be provided automatically
for nested parameters).

.. code-block:: python

  @EndpointDoc(parameters={
    'my_dictionary': ({
      'item1': (str, 'Description of item1'),
      'item2': (str, 'Description of item2', True),  # item2 is optional
      'item3': (str, 'Description of item3', True, 'foo'),  # item3 is optional with 'foo' as default value
  }, 'Description of my_dictionary')})
  def method(my_dictionary): pass

If the parameter is a ``list`` of primitive types, the type should be
surrounded with square brackets.

.. code-block:: python

  @EndpointDoc(parameters={'my_list': ([int], 'Description of my_list')})
  def method(my_list): pass

If the parameter is a ``list`` with nested parameters, the nested parameters
should be placed in a dictionary and surrounded with square brackets.

.. code-block:: python

  @EndpointDoc(parameters={
    'my_list': ([{
      'list_item': (str, 'Description of list_item'),
      'list_item2': (str, 'Description of list_item2')
  }], 'Description of my_list')})
  def method(my_list): pass


``responses``: A dict used for describing responses. Rules for describing
responses are the same as for request body parameters, with one difference:
responses also needs to be assigned to the related response code as in the
example below:

.. code-block:: python

  @EndpointDoc(responses={
    '400':{'my_response': (str, 'Description of my_response')}})
  def method(): pass


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
notable. As the Ceph Dashboard grows in feature richness, its code-base becomes
more and more complex. The hook-based nature of a plug-in architecture allows to
extend functionality in a controlled manner, and isolate the scope of the
changes.

Ceph Dashboard relies on `Pluggy <https://pluggy.readthedocs.io>`_ to provide
for plug-ing support. On top of pluggy, an interface-based approach has been
implemented, with some safety checks (method override and abstract method
checks).

In order to create a new plugin, the following steps are required:

#. Add a new file under ``src/pybind/mgr/dashboard/plugins``.
#. Import the ``PLUGIN_MANAGER`` instance and the ``Interfaces``.
#. Create a class extending the desired interfaces. The plug-in library will
   check if all the methods of the interfaces have been properly overridden.
#. Register the plugin in the ``PLUGIN_MANAGER`` instance.
#. Import the plug-in from within the Ceph Dashboard ``module.py`` (currently no
   dynamic loading is implemented).

The available Mixins (helpers) are:

- ``CanMgr``: provides the plug-in with access to the ``mgr`` instance under ``self.mgr``.

The available Interfaces are:

- ``Initializable``: requires overriding ``init()`` hook. This method is run at
  the very beginning of the dashboard module, right after all imports have been
  performed.
- ``Setupable``: requires overriding ``setup()`` hook. This method is run in the
  Ceph Dashboard ``serve()`` method, right after CherryPy has been configured,
  but before it is started. It's a placeholder for the plug-in initialization
  logic.
- ``HasOptions``: requires overriding ``get_options()`` hook by returning a list
  of ``Options()``. The options returned here are added to the
  ``MODULE_OPTIONS``.
- ``HasCommands``: requires overriding ``register_commands()`` hook by defining
  the commands the plug-in can handle and decorating them with ``@CLICommand``.
  The commands can be optionally returned, so that they can be invoked
  externally (which makes unit testing easier).
- ``HasControllers``: requires overriding ``get_controllers()`` hook by defining
  and returning the controllers as usual.
- ``FilterRequest.BeforeHandler``: requires overriding
  ``filter_request_before_handler()`` hook. This method receives a
  ``cherrypy.request`` object for processing. A usual implementation of this
  method will allow some requests to pass or will raise a ``cherrypy.HTTPError``
  based on the ``request`` metadata and other conditions.

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
  class Mute(I.CanMgr, I.Setupable, I.HasOptions, I.HasCommands,
                       I.FilterRequest.BeforeHandler, I.HasControllers):
    @PM.add_hook
    def get_options(self):
      return [Option('mute', default=False, type='bool')]

    @PM.add_hook
    def setup(self):
      self.mute = self.mgr.get_module_option('mute')

    @PM.add_hook
    def register_commands(self):
      @CLICommand("dashboard mute")
      def _(mgr):
        self.mute = True
        self.mgr.set_module_option('mute', True)
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

      return [MuteController]


Additionally, a helper for creating plugins ``SimplePlugin`` is provided. It
facilitates the basic tasks (Options, Commands, and common Mixins). The previous
plugin could be rewritten like this:

.. code-block:: python

  from . import PLUGIN_MANAGER as PM
  from . import interfaces as I
  from .plugin import SimplePlugin as SP

  import cherrypy

  @PM.add_plugin
  class Mute(SP, I.Setupable, I.FilterRequest.BeforeHandler, I.HasControllers):
    OPTIONS = [
        SP.Option('mute', default=False, type='bool')
    ]

    def shut_up(self):
      self.set_option('mute', True)
      self.mute = True
      return 0

    COMMANDS = [
        SP.Command("dashboard mute", handler=shut_up)
    ]

    @PM.add_hook
    def setup(self):
      self.mute = self.get_option('mute')

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

      return [MuteController]
