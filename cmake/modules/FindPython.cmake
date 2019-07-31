# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

#[=======================================================================[.rst:
FindPython
----------

Find Python interpreter, compiler and development environment (include
directories and libraries).

Three components are supported:

* ``Interpreter``: search for Python interpreter.
* ``Compiler``: search for Python compiler. Only offered by IronPython.
* ``Development``: search for development artifacts (include directories and
  libraries).
* ``NumPy``: search for NumPy include directories.

If no ``COMPONENTS`` is specified, ``Interpreter`` is assumed.

To ensure consistent versions between components ``Interpreter``, ``Compiler``,
``Development`` and ``NumPy``, specify all components at the same time::

  find_package (Python COMPONENTS Interpreter Development)

This module looks preferably for version 3 of Python. If not found, version 2
is searched.
To manage concurrent versions 3 and 2 of Python, use :module:`FindPython3` and
:module:`FindPython2` modules rather than this one.

.. note::

  If components ``Interpreter`` and ``Development`` are both specified, this
  module search only for interpreter with same platform architecture as the one
  defined by ``CMake`` configuration. This contraint does not apply if only
  ``Interpreter`` component is specified.

Imported Targets
^^^^^^^^^^^^^^^^

This module defines the following :ref:`Imported Targets <Imported Targets>`
(when :prop_gbl:`CMAKE_ROLE` is ``PROJECT``):

``Python::Interpreter``
  Python interpreter. Target defined if component ``Interpreter`` is found.
``Python::Compiler``
  Python compiler. Target defined if component ``Compiler`` is found.
``Python::Python``
  Python library. Target defined if component ``Development`` is found.
``Python::NumPy``
  NumPy Python library. Target defined if component ``NumPy`` is found.

Result Variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project
(see :ref:`Standard Variable Names <CMake Developer Standard Variable Names>`):

``Python_FOUND``
  System has the Python requested components.
``Python_Interpreter_FOUND``
  System has the Python interpreter.
``Python_EXECUTABLE``
  Path to the Python interpreter.
``Python_INTERPRETER_ID``
  A short string unique to the interpreter. Possible values include:
    * Python
    * ActivePython
    * Anaconda
    * Canopy
    * IronPython
``Python_STDLIB``
  Standard platform independent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=False,standard_lib=True)``.
``Python_STDARCH``
  Standard platform dependent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=True,standard_lib=True)``.
``Python_SITELIB``
  Third-party platform independent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=False,standard_lib=False)``.
``Python_SITEARCH``
  Third-party platform dependent installation directory.

  Information returned by
  ``distutils.sysconfig.get_python_lib(plat_specific=True,standard_lib=False)``.
``Python_Compiler_FOUND``
  System has the Python compiler.
``Python_COMPILER``
  Path to the Python compiler. Only offered by IronPython.
``Python_COMPILER_ID``
  A short string unique to the compiler. Possible values include:
    * IronPython
``Python_Development_FOUND``
  System has the Python development artifacts.
``Python_INCLUDE_DIRS``
  The Python include directories.
``Python_LIBRARIES``
  The Python libraries.
``Python_LIBRARY_DIRS``
  The Python library directories.
``Python_RUNTIME_LIBRARY_DIRS``
  The Python runtime library directories.
``Python_VERSION``
  Python version.
``Python_VERSION_MAJOR``
  Python major version.
``Python_VERSION_MINOR``
  Python minor version.
``Python_VERSION_PATCH``
  Python patch version.
``Python_NumPy_FOUND``
  System has the NumPy.
``Python_NumPy_INCLUDE_DIRS``
  The NumPy include directries.
``Python_NumPy_VERSION``
  The NumPy version.

Hints
^^^^^

``Python_ROOT_DIR``
  Define the root directory of a Python installation.

``Python_USE_STATIC_LIBS``
  * If not defined, search for shared libraries and static libraries in that
    order.
  * If set to TRUE, search **only** for static libraries.
  * If set to FALSE, search **only** for shared libraries.

``Python_FIND_REGISTRY``
  On Windows the ``Python_FIND_REGISTRY`` variable determine the order
  of preference between registry and environment variables.
  the ``Python_FIND_REGISTRY`` variable can be set to empty or one of the
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

This module defines the command ``Python_add_library`` (when
:prop_gbl:`CMAKE_ROLE` is ``PROJECT``), which has the same semantics as
:command:`add_library`, but takes care of Python module naming rules
(only applied if library is of type ``MODULE``), and adds a dependency to target
``Python::Python``::

  Python_add_library (my_module MODULE src1.cpp)

If library type is not specified, ``MODULE`` is assumed.
#]=======================================================================]


set (_PYTHON_PREFIX Python)

if (DEFINED Python_FIND_VERSION)
  set (_Python_REQUIRED_VERSION_MAJOR ${Python_FIND_VERSION_MAJOR})

  include (${CMAKE_CURRENT_LIST_DIR}/FindPython/Support.cmake)
else()
  # iterate over versions in quiet and NOT required modes to avoid multiple
  # "Found" messages and prematurally failure.
  set (_Python_QUIETLY ${Python_FIND_QUIETLY})
  set (_Python_REQUIRED ${Python_FIND_REQUIRED})
  set (Python_FIND_QUIETLY TRUE)
  set (Python_FIND_REQUIRED FALSE)

  set (_Python_REQUIRED_VERSIONS 3 2)
  set (_Python_REQUIRED_VERSION_LAST 2)

  foreach (_Python_REQUIRED_VERSION_MAJOR IN LISTS _Python_REQUIRED_VERSIONS)
    set (Python_FIND_VERSION ${_Python_REQUIRED_VERSION_MAJOR})
    include (${CMAKE_CURRENT_LIST_DIR}/FindPython/Support.cmake)
    if (Python_FOUND OR
        _Python_REQUIRED_VERSION_MAJOR EQUAL _Python_REQUIRED_VERSION_LAST)
      break()
    endif()
    # clean-up some CACHE variables to ensure look-up restart from scratch
    foreach (_Python_ITEM IN LISTS _Python_CACHED_VARS)
      unset (${_Python_ITEM} CACHE)
    endforeach()
  endforeach()

  unset (Python_FIND_VERSION)

  set (Python_FIND_QUIETLY ${_Python_QUIETLY})
  set (Python_FIND_REQUIRED ${_Python_REQUIRED})
  if (Python_FIND_REQUIRED OR NOT Python_FIND_QUIETLY)
    # call again validation command to get "Found" or error message
    find_package_handle_standard_args (Python HANDLE_COMPONENTS
                                              REQUIRED_VARS ${_Python_REQUIRED_VARS}
                                              VERSION_VAR Python_VERSION)
  endif()
endif()

if (COMMAND __Python_add_library)
  macro (Python_add_library)
    __Python_add_library (Python ${ARGV})
  endmacro()
endif()

unset (_PYTHON_PREFIX)
