find_package(Python3 ${WITH_PYTHON3} EXACT
  QUIET
  REQUIRED
  COMPONENTS Interpreter)

function(create_python_package pkgname)
  set(options BUILD_ISOLATION)
  set(oneValueArgs WHEELDIR)
  cmake_parse_arguments(PARSE_ARGV 0 pypkg
    "${options}" "${oneValueArgs}" "")
  if(NOT "${pypkg_WHEELDIR}")
    set(pypkg_WHEELDIR "${CMAKE_CURRENT_BINARY_DIR}/wheels/${pkgname}")
  endif()

  python_package_build_pip_wheel(
    "${pkgname}" "${pypkg_WHEELDIR}" "${pypkg_BUILD_ISOLATION}"
  )
  python_package_install_pip_wheel("${pkgname}" "${pypkg_WHEELDIR}")
endfunction(create_python_package)


function(
  python_package_build_pip_wheel
  pkgname
  wheeldir
  build_isolation
)
  list(APPEND build_args
    --wheel-dir "${wheeldir}"
    --no-deps
    --use-pep517
    --disable-pip-version-check
    --no-clean
    --progress-bar=off
    --verbose
  )
  if(NOT "${build_isolation}")
    list(APPEND build_args --no-build-isolation)
  endif()
  list(APPEND build_args "${CMAKE_CURRENT_SOURCE_DIR}")

  add_custom_command(
    OUTPUT ${wheeldir}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/pyproject.toml
    COMMAND ${Python3_EXECUTABLE} -m pip wheel ${build_args}
  )
  if(NOT TARGET build-wheel-${pkgname})
    add_custom_target(build-wheel-${pkgname} ALL
      DEPENDS ${wheeldir})
  endif()
endfunction(python_package_build_pip_wheel)

function(
  python_package_install_pip_wheel
  pkgname
  wheeldir
)
  list(APPEND install_args
    "--prefix=${CMAKE_INSTALL_PREFIX}"
    --no-deps
    --disable-pip-version-check
    --progress-bar=off
    --root-user-action=ignore
    --verbose
    --ignore-installed
    --no-warn-script-location
    --no-index
    --no-cache-dir
    --find-links "${wheeldir}"
    "${pkgname}"
  )

  install(CODE "
    set(args \"${install_args}\")
    if(DEFINED ENV{DESTDIR})
      list(INSERT args 1 --root=\$ENV{DESTDIR})
    endif()
    message(DEBUG PythonPackage.install_cmd=
      \"${Python3_EXECUTABLE} -m pip install\" \"\${args}\")
    execute_process(
      COMMAND ${Python3_EXECUTABLE} -m pip install \${args}
      WORKING_DIRECTORY \"${CMAKE_CURRENT_BINARY_DIR}\"
      COMMAND_ERROR_IS_FATAL ANY)")
endfunction(python_package_install_pip_wheel)
