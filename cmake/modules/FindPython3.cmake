# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

#[=======================================================================[.rst:
FindPython3
-----------

Find Python 3 interpreter, compiler and development environment (include
directories and libraries).

Three components are supported:

* ``Interpreter``: search for Python 3 interpreter
* ``Compiler``: search for Python 3 compiler. Only offered by IronPython.
* ``Development``: search for development artifacts (include directories and
  libraries)
* ``NumPy``: search for NumPy include directories.

If no ``COMPONENTS`` is specified, ``Interpreter`` is assumed.

To ensure consistent versions between components ``Interpreter``, ``Compiler``,
``Development`` and ``NumPy``, specify all components at the same time::

  find_package (Python3 COMPONENTS Interpreter Development)

This module looks only for version 3 of Python. This module can be used
concurrently with :module:`FindPython2` module to use both Python versions.

The :module:`FindPython` module can be used if Python version does not matter
for you.

.. note::

  If components ``Interpreter`` and ``Development`` are both specified, this
  module search only for interpreter with same platform architecture as the one
  defined by ``CMake`` configuration. This contraint does not apply if only
  ``Interpreter`` component is specified.

Imported Targets
^^^^^^^^^^^^^^^^

This module defines the following :ref:`Imported Targets <Imported Targets>`
(when :prop_gbl:`CMAKE_ROLE` is ``PROJECT``):

``Python3::Interpreter``
  Python 3 interpreter. Target defined if component ``Interpreter`` is found.
``Python3::Compiler``
  Python 3 compiler. Target defined if component ``Compiler`` is found.
``Python3::Python``
  Python 3 library. Target defined if component ``Development`` is found.
``Python3::NumPy``
  NumPy library for Python 3. Target defined if component ``NumPy`` is found.

Result Variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project
(see :ref:`Standard Variable Names <CMake Developer Standard Variable Names>`):

``Python3_FOUND``
  System has the Python 3 requested components.
``Python3_Interpreter_FOUND``
  System has the Python 3 interpreter.
``Python3_EXECUTABLE``
  Path to the Python 3 interpreter.
``Python3_INTERPRETER_ID``
  A short string unique to the interpreter. Possible values include:
    * Python
    * ActivePython
    * Anaconda
    * Canopy
    * IronPython
``Python3_STDLIB``
  Standard platform independent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=False,standard_lib=True)``.
``Python3_STDARCH``
  Standard platform dependent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=True,standard_lib=True)``.
``Python3_SITELIB``
  Third-party platform independent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=False,standard_lib=False)``.
``Python3_SITEARCH``
  Third-party platform dependent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=True,standard_lib=False)``.
``Python3_Compiler_FOUND``
  System has the Python 3 compiler.
``Python3_COMPILER``
  Path to the Python 3 compiler. Only offered by IronPython.
``Python3_COMPILER_ID``
  A short string unique to the compiler. Possible values include:
    * IronPython
``Python3_Development_FOUND``
  System has the Python 3 development artifacts.
``Python3_INCLUDE_DIRS``
  The Python 3 include directories.
``Python3_LIBRARIES``
  The Python 3 libraries.
``Python3_LIBRARY_DIRS``
  The Python 3 library directories.
``Python3_RUNTIME_LIBRARY_DIRS``
  The Python 3 runtime library directories.
``Python3_VERSION``
  Python 3 version.
``Python3_VERSION_MAJOR``
  Python 3 major version.
``Python3_VERSION_MINOR``
  Python 3 minor version.
``Python3_VERSION_PATCH``
  Python 3 patch version.
``Python3_NumPy_FOUND``
  System has the NumPy.
``Python3_NumPy_INCLUDE_DIRS``
  The NumPy include directries.
``Python3_NumPy_VERSION``
  The NumPy version.

Hints
^^^^^

``Python3_ROOT_DIR``
  Define the root directory of a Python 3 installation.

``Python3_USE_STATIC_LIBS``
  * If not defined, search for shared libraries and static libraries in that
    order.
  * If set to TRUE, search **only** for static libraries.
  * If set to FALSE, search **only** for shared libraries.

``Python3_FIND_REGISTRY``
  On Windows the ``Python3_FIND_REGISTRY`` variable determine the order
  of preference between registry and environment variables.
  the ``Python3_FIND_REGISTRY`` variable can be set to empty or one of the
  following:

  * ``FIRST``: Try to use registry before environment variables.
    This is the default.
  * ``LAST``: Try to use registry after environment variables.
  * ``NEVER``: Never try to use registry.

``CMAKE_FIND_FRAMEWORK``
  On OS X the :variable:`CMAKE_FIND_FRAMEWORK` variable determine the order of
  preference between Apple-style and unix-style package components.

  .. note::

    Value ``ONLY`` is not supported so ``FIRST`` will be used instead.

.. note::

  If a Python virtual environment is configured, set variable
  ``Python_FIND_REGISTRY`` (Windows) or ``CMAKE_FIND_FRAMEWORK`` (macOS) with
  value ``LAST`` or ``NEVER`` to select it preferably.

Commands
^^^^^^^^

This module defines the command ``Python3_add_library`` (when
:prop_gbl:`CMAKE_ROLE` is ``PROJECT``), which has the same semantics as
:command:`add_library`, but takes care of Python module naming rules
(only applied if library is of type ``MODULE``), and adds a dependency to target
``Python3::Python``::

  Python3_add_library (my_module MODULE src1.cpp)

If library type is not specified, ``MODULE`` is assumed.
#]=======================================================================]


set (_PYTHON_PREFIX Python3)

set (_Python3_REQUIRED_VERSION_MAJOR 3)

include (${CMAKE_CURRENT_LIST_DIR}/FindPython/Support.cmake)

if (COMMAND __Python3_add_library)
  macro (Python3_add_library)
    __Python3_add_library (Python3 ${ARGV})
  endmacro()
endif()

unset (_PYTHON_PREFIX)
