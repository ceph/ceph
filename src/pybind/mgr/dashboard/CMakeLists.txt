include(CMakeParseArguments)
function(add_npm_command)
  set(options NODEENV)
  set(single_kw OUTPUT COMMENT WORKING_DIRECTORY)
  set(multi_kw COMMAND DEPENDS)
  cmake_parse_arguments(NC "${options}" "${single_kw}" "${multi_kw}" ${ARGN})
  string(REPLACE ";" " " command "${NC_COMMAND}")
  if(NC_NODEENV)
    string(REGEX REPLACE "^(.*(npm|npx) .*)$" ". ${mgr-dashboard-nodeenv-dir}/bin/activate && \\1 && deactivate" command ${command})
  endif()
  string(REPLACE " " ";" command "${command}")
  add_custom_command(
    OUTPUT "${NC_OUTPUT}"
    COMMAND ${command}
    DEPENDS ${NC_DEPENDS}
    WORKING_DIRECTORY "${NC_WORKING_DIRECTORY}"
    COMMENT ${NC_COMMENT})
endfunction(add_npm_command)

if(WITH_SYSTEM_NPM)
  set(mgr-dashboard-nodeenv-dir )
  set(nodeenv "")
  add_custom_target(mgr-dashboard-frontend-deps
    DEPENDS frontend/node_modules
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
  )
else()
  set(mgr-dashboard-nodeenv-dir ${CMAKE_CURRENT_BINARY_DIR}/node-env)
  set(nodeenv NODEENV)

  add_custom_command(
    OUTPUT "${mgr-dashboard-nodeenv-dir}/bin/npm"
    COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${MGR_PYTHON_EXECUTABLE} ${mgr-dashboard-nodeenv-dir}
    COMMAND ${mgr-dashboard-nodeenv-dir}/bin/pip install nodeenv
    COMMAND ${mgr-dashboard-nodeenv-dir}/bin/nodeenv -p --node=12.16.2
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "dashboard nodeenv is being installed"
  )
  add_custom_target(mgr-dashboard-nodeenv
    COMMAND . ${mgr-dashboard-nodeenv-dir}/bin/activate && npm config set python ${MGR_PYTHON_EXECUTABLE} && deactivate
    DEPENDS ${mgr-dashboard-nodeenv-dir}/bin/npm
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  )
  add_custom_target(mgr-dashboard-frontend-deps
    DEPENDS frontend/node_modules mgr-dashboard-nodeenv
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
  )
endif()

add_npm_command(
  OUTPUT "${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend/node_modules"
  COMMAND CYPRESS_CACHE_FOLDER=${CMAKE_SOURCE_DIR}/build/src/pybind/mgr/dashboard/cypress npm ci
  DEPENDS frontend/package.json
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
  COMMENT "dashboard frontend dependencies are being installed"
  ${nodeenv}
)

# Glob some frontend files. With CMake 3.6, this can be simplified
# to *.ts *.html. Just add:
# list(FILTER frontend_src INCLUDE REGEX "frontend/src")
file(
  GLOB_RECURSE frontend_src
  frontend/src/*.ts
  frontend/src/*.html
  frontend/src/*/*.ts
  frontend/src/*/*.html
  frontend/src/*/*/*.ts
  frontend/src/*/*/*.html
  frontend/src/*/*/*/*.ts
  frontend/src/*/*/*/*.html
  frontend/src/*/*/*/*/*.ts
  frontend/src/*/*/*/*/*.html
  frontend/src/*/*/*/*/*/*.ts
  frontend/src/*/*/*/*/*/*.html)

# these files are generated during build
list(REMOVE_ITEM frontend_src
  ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend/src/environments/environment.prod.ts
  ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend/src/environments/environment.ts)

execute_process(
    COMMAND bash -c "jq -r .config.locale ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend/package.json"
    OUTPUT_VARIABLE default_lang
    OUTPUT_STRIP_TRAILING_WHITESPACE)

if(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
  set(npm_args "--prod --progress=false")
else()
  set(npm_args "--progress=false")
endif()

add_npm_command(
  OUTPUT "${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend/dist"
  COMMAND DASHBOARD_FRONTEND_LANGS="${DASHBOARD_FRONTEND_LANGS}" npm run build:localize -- ${npm_args}
  DEPENDS ${frontend_src} frontend/node_modules
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
  COMMENT "dashboard frontend is being created"
  ${nodeenv}
)
add_custom_target(mgr-dashboard-frontend-build
  ALL
  DEPENDS frontend/dist mgr-dashboard-frontend-deps
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend)

add_dependencies(tests mgr-dashboard-frontend-build)

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-dashboard TOX_ENVS lint check)
endif()
