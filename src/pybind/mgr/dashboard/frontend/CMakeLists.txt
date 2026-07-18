include(CMakeParseArguments)
function(add_npm_command)
  set(options NODEENV)
  set(single_kw OUTPUT COMMENT WORKING_DIRECTORY)
  set(multi_kw COMMAND DEPENDS)
  cmake_parse_arguments(NC "${options}" "${single_kw}" "${multi_kw}" ${ARGN})
  string(REPLACE ";" " " command "${NC_COMMAND}")
  if(NC_NODEENV)
    string(REGEX REPLACE
      "^(([^ ]+=[^ ]+ )*npm .*)$"
      ". ${mgr-dashboard-nodeenv-dir}/bin/activate && \\1 && deactivate"
      command ${command})
  else()
    string(REGEX REPLACE
      "^([^ ]=[^ ] )*npm (.*)$"
      "\\1${NPM_EXECUTABLE} \\2"
      command ${command})
  endif()
  string(REPLACE " " ";" command "${command}")
  add_custom_command(
    OUTPUT "${NC_OUTPUT}"
    COMMAND ${command}
    COMMAND ${CMAKE_COMMAND} -E touch "${NC_OUTPUT}"
    DEPENDS ${NC_DEPENDS}
    WORKING_DIRECTORY "${NC_WORKING_DIRECTORY}"
    COMMENT ${NC_COMMENT}
    VERBATIM)
  set_property(DIRECTORY APPEND
    PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "${NC_OUTPUT}")
endfunction(add_npm_command)

function(add_npm_options)
  set(commands)
  cmake_parse_arguments(NC "" "NODEENV_DIR;TARGET" "OPTION" ${ARGN})
  foreach(opt ${NC_OPTION})
    string(REPLACE "=" ";" opt ${opt})
    list(GET opt 0 key)
    list(GET opt 1 value)
    list(APPEND commands
      COMMAND
      . ${NC_NODEENV_DIR}/bin/activate &&
      npm config set ${key} ${value} --userconfig ${NC_NODEENV_DIR}/.npmrc &&
      deactivate)
  endforeach()
  set(npm_config_file ${NC_NODEENV_DIR}/.npmrc)
  add_custom_command(
    OUTPUT ${npm_config_file}
    ${commands}
    DEPENDS ${NC_NODEENV_DIR}/bin/npm
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "dashboard npm options are being configured"
    VERBATIM)
  add_custom_target(${NC_TARGET}
    DEPENDS ${npm_config_file})
endfunction(add_npm_options)

set(node_modules_stamp
  ${CMAKE_CURRENT_BINARY_DIR}/node_modules.stamp)
set(dashboard_frontend_deps ${node_modules_stamp})

if(WITH_SYSTEM_NPM)
  set(mgr-dashboard-nodeenv-dir )
  set(nodeenv "")
else(WITH_SYSTEM_NPM)
  set(mgr-dashboard-nodeenv-dir ${CMAKE_CURRENT_BINARY_DIR}/node-env)
  set(nodeenv NODEENV)
  set(mgr-dashboard-userconfig --userconfig ${mgr-dashboard-nodeenv-dir}/.npmrc)
  if(DEFINED ENV{NODE_MIRROR})
    set(node_mirror_opt "--mirror=$ENV{NODE_MIRROR}")
  endif()
  if(DEFINED ENV{NPM_CACHEDIR})
    set(npm-cache-dir "$ENV{NPM_CACHEDIR}")
  else()
    set(npm-cache-dir "${mgr-dashboard-nodeenv-dir}/.npm")
  endif()
  add_custom_command(
    OUTPUT "${mgr-dashboard-nodeenv-dir}/bin/npm"
    COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${MGR_PYTHON_EXECUTABLE} ${mgr-dashboard-nodeenv-dir}
    COMMAND ${mgr-dashboard-nodeenv-dir}/bin/pip install nodeenv
    COMMAND ${mgr-dashboard-nodeenv-dir}/bin/nodeenv --verbose ${node_mirror_opt} -p --node=22.21.1
    # ensure that the files that nodeenv unpacks from tarballs are owned by
    # the current user. This can be an issue due to the node tarball using
    # uid 1000 and running the unpack in a id-mapped namespace (container)
    # that lets tar set the uid to a "bad" uid outside the namespace
    COMMAND bash -c 'chown -R `id -u`:`id -g` ${mgr-dashboard-nodeenv-dir}/src'
    COMMAND ${CMAKE_COMMAND} -E make_directory ${npm-cache-dir}
    DEPENDS ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "dashboard nodeenv is being installed")
  if(DEFINED ENV{NPM_REGISTRY})
    set(npm_registry_opts "OPTION" "registry=$ENV{NPM_REGISTRY}")
  endif()
  add_npm_options(
    NODEENV_DIR ${mgr-dashboard-nodeenv-dir}
    TARGET mgr-dashboard-nodeenv
    OPTION cache=${npm-cache-dir}
    ${npm_registry_opts})
  list(APPEND dashboard_frontend_deps mgr-dashboard-nodeenv)
endif(WITH_SYSTEM_NPM)

add_custom_target(mgr-dashboard-frontend-deps
  DEPENDS ${dashboard_frontend_deps}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_npm_command(
  OUTPUT ${node_modules_stamp}
  COMMAND CYPRESS_CACHE_FOLDER=${CMAKE_BINARY_DIR}/src/pybind/mgr/dashboard/cypress NG_CLI_ANALYTICS=false IBM_TELEMETRY_DISABLED=true npm ci -f ${mgr-dashboard-userconfig}
  DEPENDS package.json package-lock.json
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "dashboard frontend dependencies are being installed"
  ${nodeenv})

# Glob some frontend files.
file(
  GLOB_RECURSE frontend_src
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  src/*.ts
  src/*.html
  src/*.scss
  src/*.css
  src/*.svg
  src/*.json
  src/*.xlf)

# these files are generated during build
list(REMOVE_ITEM frontend_src
  src/environments/environment.prod.ts
  src/environments/environment.ts)

set(frontend_dist_dir "${CMAKE_CURRENT_BINARY_DIR}/dist")
set(frontend_dist_stamp "${frontend_dist_dir}/.ceph-built")
set(npm_args "--output-path ${frontend_dist_dir}")
if(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
  string(APPEND npm_args " --configuration=production --progress=false")
else()
  string(APPEND npm_args " --progress=false")
endif()

add_npm_command(
  OUTPUT ${frontend_dist_stamp}
  COMMAND DASHBOARD_FRONTEND_LANGS="${DASHBOARD_FRONTEND_LANGS}" npm run build:localize -- ${npm_args}
  DEPENDS
    ${frontend_src}
    ${node_modules_stamp}
    .browserslistrc
    angular.json
    babel.config.js
    cd.js
    ngcc.config.js
    package.json
    tsconfig.app.json
    tsconfig.json
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "dashboard frontend is being created"
  ${nodeenv})

set_property(DIRECTORY APPEND PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/node_modules"
  "${frontend_dist_dir}")

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/package.json
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/package.json
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
    ${CMAKE_CURRENT_SOURCE_DIR}/package.json
    ${CMAKE_CURRENT_BINARY_DIR}/package.json
  VERBATIM)

add_custom_target(mgr-dashboard-frontend-build
  ALL
  DEPENDS
    ${frontend_dist_stamp}
    ${CMAKE_CURRENT_BINARY_DIR}/package.json
    mgr-dashboard-frontend-deps
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_dependencies(tests mgr-dashboard-frontend-build)

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dist
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/package.json
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
