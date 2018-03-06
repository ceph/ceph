set(MGR_DASHBOARD_V2_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-dashboard_v2-virtualenv)

add_custom_target(mgr-dashboard_v2-test-venv
  COMMAND
  ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh ${MGR_DASHBOARD_V2_VIRTUALENV} &&
  ${MGR_DASHBOARD_V2_VIRTUALENV}/bin/pip install --no-index --use-wheel --find-links=file:${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/wheelhouse -r requirements.txt
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2
  COMMENT "dashboard_v2 tests virtualenv is being created")
add_dependencies(tests mgr-dashboard_v2-test-venv)

if(WITH_MGR_DASHBOARD_V2_FRONTEND AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")

add_custom_command(
  OUTPUT "${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/frontend/node_modules"
  COMMAND npm install
  DEPENDS frontend/package.json
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/frontend
  COMMENT "dashboard_v2 frontend dependencies are being installed"
)

add_custom_target(mgr-dashboard_v2-frontend-deps
  DEPENDS frontend/node_modules
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/frontend
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
  OUTPUT "${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/frontend/dist"
  COMMAND ${npm_command}
  DEPENDS ${frontend_src} frontend/node_modules
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/frontend
  COMMENT "dashboard_v2 frontend is being created"
)
add_custom_target(mgr-dashboard_v2-frontend-build
  DEPENDS frontend/dist mgr-dashboard_v2-frontend-deps
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/frontend
)
add_dependencies(ceph-mgr mgr-dashboard_v2-frontend-build)
endif(WITH_MGR_DASHBOARD_V2_FRONTEND AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")
