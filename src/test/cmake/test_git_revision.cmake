# SPDX-License-Identifier: LGPL-2.1-or-later

cmake_minimum_required(VERSION 3.22.1)

function(run output_variable)
  execute_process(COMMAND ${ARGN}
    RESULT_VARIABLE status
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT status EQUAL 0)
    message(FATAL_ERROR "${ARGN} failed (${status}):\n${output}\n${error}")
  endif()
  set(${output_variable} "${output}" PARENT_SCOPE)
endfunction()

function(assert_revision expected source build)
  file(READ "${build}/revision.txt" actual)
  string(STRIP "${actual}" actual)
  if(NOT actual STREQUAL expected)
    message(FATAL_ERROR "${source}: expected ${expected}, got ${actual}")
  endif()
endfunction()

function(configure_fixture source build)
  run(expected "${GIT_EXECUTABLE}" -C "${source}" rev-parse HEAD)
  message(STATUS "Configure ${source}: expect ${expected}")
  run(unused "${CMAKE_COMMAND}" -S "${source}" -B "${build}"
    -G "${TEST_GENERATOR}"
    "-DCMAKE_MAKE_PROGRAM=${TEST_MAKE_PROGRAM}"
    "-DREVISION_MODULE_DIR=${REVISION_MODULE_DIR}")
  assert_revision("${expected}" "${source}" "${build}")
endfunction()

function(build_fixture source build)
  run(expected "${GIT_EXECUTABLE}" -C "${source}" rev-parse HEAD)
  message(STATUS "Build ${source}: expect ${expected}")
  run(unused "${CMAKE_COMMAND}" --build "${build}"
    --target check_revision --parallel 1 --config Debug)
  assert_revision("${expected}" "${source}" "${build}")
endfunction()

string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef nonce)
set(root "${TEST_ROOT}/git-revision-${nonce}")
if(EXISTS "${root}")
  message(FATAL_ERROR "Fixture already exists: ${root}")
endif()
file(MAKE_DIRECTORY "${root}/hooks")

# Keep fixture commits independent of the caller's repository and signing hooks.
foreach(variable GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE
    GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES
    GIT_CONFIG_PARAMETERS GIT_CONFIG_COUNT)
  unset(ENV{${variable}})
endforeach()
file(WRITE "${root}/gitconfig"
  "[user]\nname = Git revision test\nemail = git-revision@example.invalid\n"
  "[commit]\ngpgsign = false\n[core]\nhooksPath = ${root}/hooks\n")
set(ENV{GIT_CONFIG_GLOBAL} "${root}/gitconfig")
set(ENV{GIT_CONFIG_NOSYSTEM} 1)

set(primary "${root}/primary")
set(detached "${root}/detached")
set(named "${root}/named")
run(unused "${GIT_EXECUTABLE}" init --quiet "${primary}")
run(unused "${GIT_EXECUTABLE}" -C "${primary}" symbolic-ref HEAD refs/heads/primary)
file(WRITE "${primary}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.22.1)
project(git_revision_fixture LANGUAGES NONE)
include("${REVISION_MODULE_DIR}/GetGitRevisionDescription.cmake")
get_git_head_revision(ref revision)
file(WRITE "${CMAKE_BINARY_DIR}/revision.txt" "${revision}\n")
file(APPEND "${CMAKE_BINARY_DIR}/configure-log.txt" "${revision}\n")
add_custom_target(check_revision)
]=])
run(unused "${GIT_EXECUTABLE}" -C "${primary}" add CMakeLists.txt)
run(unused "${GIT_EXECUTABLE}" -C "${primary}" commit --quiet -m initial)
run(initial "${GIT_EXECUTABLE}" -C "${primary}" rev-parse HEAD)
run(unused "${GIT_EXECUTABLE}" -C "${primary}" commit --quiet --allow-empty -m primary)
run(unused "${GIT_EXECUTABLE}" -C "${primary}" worktree add --quiet --detach
  "${detached}" "${initial}")
run(unused "${GIT_EXECUTABLE}" -C "${primary}" worktree add --quiet -b named "${named}")
run(unused "${GIT_EXECUTABLE}" -C "${named}" commit --quiet --allow-empty -m named)

configure_fixture("${primary}" "${root}/build-primary")
configure_fixture("${named}" "${root}/build-named")
configure_fixture("${detached}" "${root}/build-detached")

# A loose branch ref and a detached HEAD must both trigger regeneration.
run(unused "${CMAKE_COMMAND}" -E sleep 1)
run(unused "${GIT_EXECUTABLE}" -C "${named}" commit --quiet --allow-empty -m advance)
build_fixture("${named}" "${root}/build-named")
run(unused "${CMAKE_COMMAND}" -E sleep 1)
run(unused "${GIT_EXECUTABLE}" -C "${detached}" checkout --quiet --detach named)
build_fixture("${detached}" "${root}/build-detached")

# The pinned worktree must not depend on the primary checkout's HEAD file.
file(STRINGS "${root}/build-detached/configure-log.txt" configurations_before)
run(unused "${CMAKE_COMMAND}" -E sleep 1)
run(unused "${GIT_EXECUTABLE}" -C "${primary}" checkout --quiet -b unrelated)
build_fixture("${detached}" "${root}/build-detached")
file(STRINGS "${root}/build-detached/configure-log.txt" configurations_after)
if(NOT configurations_before STREQUAL configurations_after)
  message(FATAL_ERROR "An unrelated primary HEAD change regenerated the detached build")
endif()

run(unused "${GIT_EXECUTABLE}" -C "${primary}" pack-refs --all --prune)
configure_fixture("${primary}" "${root}/build-primary-packed")
configure_fixture("${named}" "${root}/build-named-packed")
configure_fixture("${detached}" "${root}/build-detached-packed")

# A submodule has a gitdir file but no linked-worktree commondir.
set(child "${root}/child")
run(unused "${GIT_EXECUTABLE}" init --quiet "${child}")
file(COPY "${primary}/CMakeLists.txt" DESTINATION "${child}")
run(unused "${GIT_EXECUTABLE}" -C "${child}" add CMakeLists.txt)
run(unused "${GIT_EXECUTABLE}" -C "${child}" commit --quiet -m child)
run(unused "${GIT_EXECUTABLE}" -C "${primary}" -c protocol.file.allow=always
  submodule add --quiet "${child}" nested)
run(child_revision "${GIT_EXECUTABLE}" -C "${child}" rev-parse HEAD)
run(unused "${GIT_EXECUTABLE}" -C "${primary}/nested" checkout --quiet --detach
  "${child_revision}")
configure_fixture("${primary}/nested" "${root}/build-submodule")

run(unused "${GIT_EXECUTABLE}" -C "${primary}" worktree remove "${named}")
run(unused "${GIT_EXECUTABLE}" -C "${primary}" worktree remove "${detached}")
file(REMOVE_RECURSE "${root}")
message(STATUS "Git revisions and rebuild dependencies passed for primary, linked and submodule checkouts")
