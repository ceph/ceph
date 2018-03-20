set(MGR_DASHBOARD_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-dashboard-virtualenv)

add_custom_target(mgr-dashboard-test-venv
  COMMAND
  ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh ${MGR_DASHBOARD_VIRTUALENV} &&
  ${MGR_DASHBOARD_VIRTUALENV}/bin/pip install --no-index --use-wheel --find-links=file:${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/wheelhouse -r requirements.txt
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard
  COMMENT "dashboard tests virtualenv is being created")
add_dependencies(tests mgr-dashboard-test-venv)

if(WITH_MGR_DASHBOARD_FRONTEND AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")

set(mgr-dashboard-nodeenv ${CMAKE_CURRENT_BINARY_DIR}/node-env)

add_custom_command(
  OUTPUT "${mgr-dashboard-nodeenv}/bin/npm"
  COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh ${mgr-dashboard-nodeenv}
  COMMAND ${mgr-dashboard-nodeenv}/bin/pip install nodeenv
  COMMAND ${mgr-dashboard-nodeenv}/bin/nodeenv -p -n 8.10.0
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "dashboard nodeenv is being installed"
)

add_custom_target(mgr-dashboard-nodeenv
  DEPENDS ${mgr-dashboard-nodeenv}/bin/npm
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_command(
  OUTPUT "${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend/node_modules"
  COMMAND . ${mgr-dashboard-nodeenv}/bin/activate && npm install && deactivate
  DEPENDS frontend/package.json
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
  COMMENT "dashboard frontend dependencies are being installed"
)

add_custom_target(mgr-dashboard-frontend-deps
  DEPENDS frontend/node_modules mgr-dashboard-nodeenv
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
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

if(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
  set(npm_command npm run build -- --prod)
else()
  set(npm_command npm run build)
endif()

add_custom_command(
  OUTPUT "${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend/dist"
  COMMAND . ${mgr-dashboard-nodeenv}/bin/activate && ${npm_command} && deactivate
  DEPENDS ${frontend_src} frontend/node_modules
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
  COMMENT "dashboard frontend is being created"
)
add_custom_target(mgr-dashboard-frontend-build
  DEPENDS frontend/dist mgr-dashboard-frontend-deps
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/frontend
)
add_dependencies(ceph-mgr mgr-dashboard-frontend-build)
endif(WITH_MGR_DASHBOARD_FRONTEND AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")
