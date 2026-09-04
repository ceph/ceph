
#[[
Check for presence of C++ features against feature test macros.
If not found, optionally use CPM to try to provide support for the feature.

C++ library feature test macros may be found here:
https://en.cppreference.com/cpp/feature_test#Library_features

We provide two basic ideas: simple native presence check and a presence check that also
tries to install support:

has_cpp_feature(): checks feature-test macro, sets CMake variable with presence result;

require_cpp_feature(): requires feature to be available, optionally trying to install
it if not native.

EXAMPLES:

Note: all examples are assumed to have this in the cmake file, I've omitted
it from further examples for brevity:
include(HasCppFeature)

* Simple test for the presence of a feature (uses the feature macro alone, does
    not attempt to install anything):

    has_cpp_feature(__cpp_lib_generator HAVE_CPP_LIB_GENERATOR)

    if(HAVE_CPP_LIB_GENERATOR)
     message("-- std::generator support is active")
    endif()

* Versioned test for the presence of a feature:
    has_cpp_feature(__cpp_lib_flat_set HAVE_CPP_LIB_FLAT_SET MIN_VALUE 202207L)

    if(HAVE_CPP_LIB_FLAT_SET)
     message("-- std::flat_set support is active")
    endif()

* Require a locally available feature and define the result on a target (a more
verbose approach, you'll see why soon):
    add_library(bletch INTERFACE)
    require_cpp_feature(__cpp_lib_span BLAH TARGET bletch)

    if(BLAH)
     message("-- std::span support is active")
    endif()

* Try to use an alternative target if the native feature is not found, otherwise
fail with FATAL_ERROR. On success, the result variable name is the uppercase
feature tag name. If TARGET is given, that target gets the result variable name
as a preprocessor definition-- i.e.:
  given:
      require_cpp_feature(__cpp_lib_span MARMOT TARGET bletch)
  ...if the feature is available, CMake sets variable MARMOT to true, and:
      target_compile_definitions(bletch INTERFACE MARMOT)

The USE_ALTERNATIVE parameter lists existing CMake targets that can provide the
capability if the native feature check fails. If one is selected, TARGET also gets the
generic feature preprocessor definition and a corresponding *_HAS_ALT_* or *_ALT
preprocessor definition:

    require_cpp_feature(__cpp_lib_flat_map CEPH_LIBFDB_HAS_FLAT_MAP
        TARGET rgw_fdb
        USE_ALTERNATIVE Boost::headers Boost::boost)

    if(CEPH_LIBFDB_HAS_FLAT_MAP)
     message("-- flat_map support is active")
    endif()

* Try to install support if feature is not found, otherwise fail with FATAL_ERROR. Extra
arguments are passed to CPMAddPackage()-- refer to its documentation for details on
GIT_REPOSITORY, GIT_TAG, EXCLUDE_FROM_ALL, and SYSTEM:

    require_cpp_feature(__cpp_lib_generator cpp_generator
        TARGET rgw_fdb
        GIT_REPOSITORY https://github.com/lewissbaker/generator
        GIT_TAG main
        EXCLUDE_FROM_ALL YES
        SYSTEM YES)

    if(CPP_GENERATOR)
     message("-- generator support is active")
    endif()

...if the compiler/library already supports std::generator, these extra options do nothing. If CPM
fetches lewissbaker/generator, they keep the fallback quiet and out of the default build except
where rgw_fdb needs it.

]]#

# Check for cpp_feature_macro support, set variable ${our_name} if present:
# HAVE_CPP_LIB_GENERATOR if std::generator support is present.
# MIN_VALUE can be used to require a minimum feature-test macro value.
function(has_cpp_feature cpp_feature_macro our_name)

  cmake_parse_arguments(PARSED "" "MIN_VALUE" "" ${ARGN})
  set(MIN_VALUE_ARG ${PARSED_MIN_VALUE})
  set(UNPARSED_ARGS ${PARSED_UNPARSED_ARGUMENTS})

  if(UNPARSED_ARGS)
    message(FATAL_ERROR "has_cpp_feature(): unknown arguments: ${UNPARSED_ARGS}")
  endif()

  set(feature_description "${cpp_feature_macro} / ${our_name}")

  if(MIN_VALUE_ARG)
    string(APPEND feature_description " >= ${MIN_VALUE_ARG}")
    set(value_check "
      #if ${MIN_VALUE_ARG} > ${cpp_feature_macro}
       #error feature test macro does not meet MIN_VALUE
      #endif")
  endif()

  message(STATUS "Checking for C++ feature: ${feature_description}")

  set(try_compile_args)

  if(DEFINED CMAKE_CXX_STANDARD)
    list(APPEND try_compile_args CXX_STANDARD ${CMAKE_CXX_STANDARD})
  endif()

  if(DEFINED CMAKE_CXX_STANDARD_REQUIRED)
    list(APPEND try_compile_args CXX_STANDARD_REQUIRED ${CMAKE_CXX_STANDARD_REQUIRED})
  endif()

  if(DEFINED CMAKE_CXX_EXTENSIONS)
    list(APPEND try_compile_args CXX_EXTENSIONS ${CMAKE_CXX_EXTENSIONS})
  endif()

  set(src "
      #include <version>
      #ifndef ${cpp_feature_macro}
       #error feature is not present: ${cpp_feature_macro}
      #endif
      ${value_check}
      int main() {}
    ")

  unset(${our_name})
  unset(${our_name} CACHE)

  if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.25)
    try_compile(${our_name}
      SOURCE_FROM_CONTENT src-check-for-${cpp_feature_macro}.cpp "${src}"
      ${try_compile_args}
    )
  else()
    # Support older versions of cmake:

    set(feature_check_dir "${CMAKE_BINARY_DIR}/CMakeFiles/HasCppFeature/${cpp_feature_macro}")
    set(feature_check_src "${feature_check_dir}/src-check-for-${cpp_feature_macro}.cpp")

    file(MAKE_DIRECTORY "${feature_check_dir}")
    file(WRITE "${feature_check_src}" "${src}")

    try_compile(${our_name}
      "${feature_check_dir}"
      "${feature_check_src}"
      ${try_compile_args}
    )
  endif()

  set(${our_name} ${${our_name}} PARENT_SCOPE)

  if(${our_name})
    message(STATUS "C++ feature available: ${feature_description}")
  else()
    message(STATUS "C++ feature not available: ${feature_description}")
  endif()

endfunction()


# Try to parse out the CMake target names exported by a CPM package:
function(cpp_feature_git_repository_alternatives_detail out_name)

  cmake_parse_arguments(CFG "" "GIT_REPOSITORY" "" ${ARGN})
  set(alternatives)

  if(CFG_GIT_REPOSITORY)
    string(REGEX REPLACE "/+$" "" git_repository "${CFG_GIT_REPOSITORY}")
    string(REGEX REPLACE "\\.git$" "" git_repository "${git_repository}")
    get_filename_component(git_repository_name "${git_repository}" NAME)

    if(git_repository_name)
      list(APPEND alternatives
        ${git_repository_name}
        std${git_repository_name})
    endif()
  endif()

  set(${out_name} ${alternatives} PARENT_SCOPE)

endfunction()

function(cpp_feature_alternative_target_detail out_name)

  set(alternative_target)

  foreach(alternative ${ARGN})
    if(TARGET ${alternative})
      set(alternative_target ${alternative})
      break()
    endif()
  endforeach()

  set(${out_name} ${alternative_target} PARENT_SCOPE)

endfunction()

function(cpp_feature_alternative_macro_detail out_name feature_name)

  if(feature_name MATCHES "^(.+)_HAS_(.+)$")
    set(alternative_name "${CMAKE_MATCH_1}_HAS_ALT_${CMAKE_MATCH_2}")
  else()
    set(alternative_name "${feature_name}_ALT")
  endif()

  set(${out_name} ${alternative_name} PARENT_SCOPE)

endfunction()

# Try to get the C++ feature using CPM if not already installed:
# (MIN_VALUE is used by this helper; the extra arguments are passed to CPM.)
function(obtain_cpp_feature_detail cpp_feature_macro feature_tag_name our_name)

  cmake_parse_arguments(PARSED "" "MIN_VALUE;TARGET" "USE_ALTERNATIVE" ${ARGN})
  set(MIN_VALUE_ARG ${PARSED_MIN_VALUE})
  set(TARGET_ARG ${PARSED_TARGET})
  set(USE_ALTERNATIVE_ARGS ${PARSED_USE_ALTERNATIVE})
  set(UNPARSED_ARGS ${PARSED_UNPARSED_ARGUMENTS})

  if(TARGET_ARG AND NOT TARGET ${TARGET_ARG})
    message(FATAL_ERROR "C++ feature target does not exist: ${TARGET_ARG}")
  endif()

  set(feature_check_args)

  if(MIN_VALUE_ARG)
    list(APPEND feature_check_args MIN_VALUE ${MIN_VALUE_ARG})
  endif()

  set(feature_alternative)
  set(feature_alternatives ${USE_ALTERNATIVE_ARGS})
  cpp_feature_git_repository_alternatives_detail(git_repository_alternatives ${UNPARSED_ARGS})
  list(APPEND feature_alternatives ${git_repository_alternatives})

  if(feature_alternatives)
    list(REMOVE_DUPLICATES feature_alternatives)
  endif()

  has_cpp_feature(${cpp_feature_macro} feature_found ${feature_check_args})

  if(NOT feature_found)
    cpp_feature_alternative_target_detail(feature_alternative ${feature_alternatives})
    if(feature_alternative)
      set(feature_found TRUE)
    endif()
  endif()

  if(NOT feature_found AND UNPARSED_ARGS)
    # Try to install; pass any remaining parameters to CPM and see
    # if it can sort out the situation:
    include(CPM)
    CPMAddPackage(NAME ${feature_tag_name} ${UNPARSED_ARGS})

    cpp_feature_alternative_target_detail(feature_alternative ${feature_alternatives})
    if(feature_alternative)
      set(feature_found TRUE)
    endif()
  endif()

  if(feature_found AND TARGET_ARG)
    target_compile_definitions(${TARGET_ARG} INTERFACE ${our_name})

    if(feature_alternative)
      cpp_feature_alternative_macro_detail(alternative_name ${our_name})
      target_compile_definitions(${TARGET_ARG} INTERFACE ${alternative_name})
      target_link_libraries(${TARGET_ARG} INTERFACE ${feature_alternative})
    endif()
  endif()

  set(${our_name} ${feature_found} PARENT_SCOPE)

  if(feature_found)
    if(feature_alternative)
      message(STATUS "C++ feature alternative target: ${cpp_feature_macro} / ${feature_alternative}")
    endif()
    message(STATUS "C++ feature obtained: ${cpp_feature_macro} / ${our_name}")
  else()
    message(STATUS "C++ feature not obtained: ${cpp_feature_macro} / ${our_name}")
  endif()

endfunction()

# Require a C++ feature. The result variable is the uppercase feature_tag_name.
# MIN_VALUE, TARGET, USE_ALTERNATIVE, and CPM package arguments are handled by
# obtain_cpp_feature_detail(). Fail if the feature cannot be made available.
function(require_cpp_feature cpp_feature_macro feature_tag_name)

  string(TOUPPER "${feature_tag_name}" local_feature_tag_name)
  obtain_cpp_feature_detail(${cpp_feature_macro} ${feature_tag_name} ${local_feature_tag_name} ${ARGN})

  if(NOT ${local_feature_tag_name})
    message(FATAL_ERROR "C++ feature ${cpp_feature_macro} / ${feature_tag_name} / ${local_feature_tag_name} could not be made available.")
  endif()

  set(${local_feature_tag_name} ${${local_feature_tag_name}} PARENT_SCOPE)

endfunction()
