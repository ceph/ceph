function(build_opentelemetry)
  set(opentelemetry_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/jaegertracing/opentelemetry-cpp")
  set(opentelemetry_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-cpp")
  set(opentelemetry_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry")
  set(opentelemetry_INSTALL_LIBDIR "${opentelemetry_INSTALL_PREFIX}/lib")

  find_package(gRPC CONFIG QUIET)

  find_package(c-ares REQUIRED)

  set(opentelemetry_CMAKE_ARGS
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DWITH_OTLP_GRPC=ON
    -DWITH_OTLP_HTTP=OFF
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_TESTING=OFF
    -DCMAKE_BUILD_TYPE=Release
    -DWITH_EXAMPLES=OFF
    -DOPENTELEMETRY_INSTALL=ON
    -DCMAKE_INSTALL_PREFIX=${opentelemetry_INSTALL_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=lib
    -DgRPC_SSL_PROVIDER=package)

  set(opentelemetry_BYPRODUCTS "")

  if(NOT TARGET gRPC::grpc++)
    # This is all, unfortunately, quite ugly. 
    # It is here because ubuntu 22 doesn't include the cmake files for gRPC, and the version
    # of both gRPC and absl is well below what opentelemetry needs
    # once we drop ubuntu 22 support it can go. 

    set(_otel_bundled_grpc TRUE)
    set(_otel_bundled_grpc TRUE PARENT_SCOPE)
    # Pin the bundled gRPC to the exact tag listed in otel's third_party_release
    # so grpc.cmake's FetchContent doesn't clone HEAD and break the build.
    set(_otel_third_party_release
      "${opentelemetry_SOURCE_DIR}/third_party_release")
    set(_grpc_git_tag "")
    if(EXISTS "${_otel_third_party_release}")
      file(STRINGS "${_otel_third_party_release}" _release_lines)
      foreach(_line IN LISTS _release_lines)
        if(_line MATCHES "^grpc=(.+)$")
          set(_grpc_git_tag "${CMAKE_MATCH_1}")
          break()
        endif()
      endforeach()
    endif()
    if(_grpc_git_tag STREQUAL "")
      message(WARNING "BuildOpentelemetry: could not determine grpc_GIT_TAG from "
                      "${_otel_third_party_release}; gRPC FetchContent may fail")
    else()
      message(STATUS "BuildOpentelemetry: using gRPC tag ${_grpc_git_tag}")
    endif()

    # All bundled libs as stems — loop below registers byproducts and collects
    # bare paths for the link group.  grpc++ appears here for byproduct tracking
    # but gets a named target later for the gRPC::grpc++ alias + include dirs.
    set(utf8_deps)
    set(_utf8_stems utf8_range utf8_range_lib utf8_validity)
    foreach(_stem IN LISTS _utf8_stems)
      set(_lib "${opentelemetry_INSTALL_LIBDIR}/lib${_stem}.a")
      list(APPEND opentelemetry_BYPRODUCTS ${_lib})
      list(APPEND utf8_deps ${_lib})
    endforeach()
    add_library(utf8 INTERFACE)
    target_link_libraries(utf8 INTERFACE ${utf8_deps})

    set(upb_deps)
    set(_upb_stems upb upb_base_lib upb_hash_lib upb_json_lib upb_lex_lib upb_mem_lib
      upb_message_lib upb_mini_descriptor_lib upb_mini_table_lib
      upb_reflection_lib upb_textformat_lib upb_wire_lib)
    foreach(_stem IN LISTS _upb_stems)
      set(_lib "${opentelemetry_INSTALL_LIBDIR}/lib${_stem}.a")
       list(APPEND opentelemetry_BYPRODUCTS ${_lib})
      list(APPEND upb_deps ${_lib})
    endforeach()
    add_library(upb INTERFACE)
    target_link_libraries(upb INTERFACE ${upb_deps})

    set(absl_deps)
    set(_absl_stems absl_base absl_city absl_civil_time absl_cord absl_cord_internal
      absl_cordz_functions absl_cordz_handle absl_cordz_info absl_cordz_sample_token
      absl_crc32c absl_crc_cord_state absl_crc_cpu_detect absl_crc_internal
      absl_debugging_internal absl_decode_rust_punycode
      absl_demangle_internal absl_demangle_rust absl_die_if_null
      absl_examine_stack absl_exponential_biased absl_failure_signal_handler
      absl_flags_commandlineflag absl_flags_commandlineflag_internal
      absl_flags_config absl_flags_internal absl_flags_marshalling absl_flags_parse
      absl_flags_private_handle_accessor absl_flags_program_name
      absl_flags_reflection absl_flags_usage absl_flags_usage_internal
      absl_graphcycles_internal absl_hash absl_hashtablez_sampler absl_int128
      absl_kernel_timeout_internal absl_leak_check
      absl_log_flags absl_log_globals absl_log_initialize
      absl_log_internal_check_op absl_log_internal_conditions absl_log_internal_fnmatch
      absl_log_internal_format absl_log_internal_globals absl_log_internal_log_sink_set
      absl_log_internal_message absl_log_internal_nullguard absl_log_internal_proto
      absl_log_internal_structured_proto absl_log_severity absl_log_sink
      absl_low_level_hash absl_malloc_internal absl_periodic_sampler absl_poison
      absl_random_distributions absl_random_internal_distribution_test_util
      absl_random_internal_entropy_pool absl_random_internal_platform
      absl_random_internal_randen absl_random_internal_randen_hwaes
      absl_random_internal_randen_hwaes_impl absl_random_internal_randen_slow
      absl_random_internal_seed_material absl_random_seed_gen_exception
      absl_random_seed_sequences absl_raw_hash_set absl_raw_logging_internal
      absl_scoped_set_env absl_spinlock_wait absl_stacktrace
      absl_status absl_statusor absl_str_format_internal absl_strerror
      absl_string_view absl_strings absl_strings_internal absl_symbolize
      absl_synchronization absl_throw_delegate absl_time absl_time_zone
      absl_tracing_internal absl_utf8_for_code_point absl_vlog_config_internal)
    foreach(_stem IN LISTS _absl_stems)
      set(_lib "${opentelemetry_INSTALL_LIBDIR}/lib${_stem}.a")
      list(APPEND opentelemetry_BYPRODUCTS ${_lib})
      list(APPEND absl_deps ${_lib})
    endforeach()
    add_library(absl INTERFACE)
    target_link_libraries(absl INTERFACE ${absl_deps})

    set(grpc_deps)
    set(_grpc_stems grpc++ grpc++_alts grpc++_error_details grpc++_reflection grpc++_unsecure
      grpc grpc_authorization_provider grpc_plugin_support grpc_unsecure grpcpp_channelz
      gpr address_sorting re2)
    foreach(_stem IN LISTS _grpc_stems)
      set(_lib "${opentelemetry_INSTALL_LIBDIR}/lib${_stem}.a")
      list(APPEND opentelemetry_BYPRODUCTS ${_lib})
      list(APPEND grpc_deps ${_lib})
    endforeach()
    add_library(_grpc INTERFACE)
    target_link_libraries(_grpc INTERFACE ${grpc_deps})
    set_target_properties(_grpc PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${opentelemetry_INSTALL_PREFIX}/include")
    add_library(gRPC::grpc++ ALIAS _grpc)

    add_library(_protobuf INTERFACE)
    target_link_libraries(_protobuf INTERFACE "${opentelemetry_INSTALL_LIBDIR}/libprotobuf.a")
    set_target_properties(_protobuf PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${opentelemetry_INSTALL_PREFIX}/include")
    list(APPEND opentelemetry_BYPRODUCTS  ${opentelemetry_INSTALL_LIBDIR}/libprotobuf.a)

    set(_protoc_path "${opentelemetry_INSTALL_PREFIX}/bin/protoc")
    list(APPEND opentelemetry_BYPRODUCTS "${_protoc_path}")
    add_executable(protobuf::protoc IMPORTED GLOBAL)
    set_target_properties(protobuf::protoc PROPERTIES
      IMPORTED_LOCATION "${_protoc_path}")


    target_link_libraries(_protobuf INTERFACE upb absl utf8)

    target_link_libraries(_grpc INTERFACE _protobuf absl)

    add_executable(gRPC::grpc_cpp_plugin IMPORTED)
    set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES
      IMPORTED_LOCATION "${opentelemetry_INSTALL_PREFIX}/bin/grpc_cpp_plugin")

    # Link system c-ares into _grpc so downstream targets get it
    target_link_libraries(_grpc INTERFACE c-ares::cares)

    list(APPEND opentelemetry_CMAKE_ARGS
      -DWITH_ABSEIL=ON
      -Dgrpc_GIT_TAG=${_grpc_git_tag}
      -DgRPC_INSTALL=ON
      -Dprotobuf_INSTALL=ON
      -DgRPC_USE_SYSTEMD=OFF
      # Use the system c-ares rather than bundling one, s
      -DgRPC_CARES_PROVIDER=package
      -DCARES_INSTALL=OFF)
  else()
    set(_otel_bundled_grpc FALSE PARENT_SCOPE)
  endif() # not found grpc

  if(WITH_SYSTEM_BOOST)
    list(APPEND opentelemetry_CMAKE_ARGS -DBOOST_ROOT=${BOOST_ROOT})
  else()
    list(APPEND dependencies Boost)
    list(APPEND opentelemetry_CMAKE_ARGS
         -DBoost_INCLUDE_DIR=${CMAKE_BINARY_DIR}/boost/include)
  endif()

  if(CMAKE_VERSION VERSION_GREATER_EQUAL "4.0.0")
    if(DEFINED CMAKE_POLICY_VERSION_MINIMUM)
      list(APPEND opentelemetry_CMAKE_ARGS
           -DCMAKE_POLICY_VERSION_MINIMUM=${CMAKE_POLICY_VERSION_MINIMUM})
    else()
      list(APPEND opentelemetry_CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5)
    endif()
  endif()

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    set(make_cmd $(MAKE))
    set(install_cmd $(MAKE) install)
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR>)
    set(install_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
  endif()

  # Collect the otel static-lib byproducts BEFORE ExternalProject_Add so that
  # all byproducts are registered with the ExternalProject in one shot.
  set(_otel_lib_stems
    proto proto_grpc common resources logs trace
    otlp_recordable exporter_otlp_grpc_client exporter_otlp_grpc)

  set(_otel_lib_paths "")
  foreach(_stem IN LISTS _otel_lib_stems)
    set(_lib "${opentelemetry_INSTALL_LIBDIR}/libopentelemetry_${_stem}.a")
    list(APPEND opentelemetry_BYPRODUCTS "${_lib}")
    list(APPEND _otel_lib_paths "${_lib}")
  endforeach()

  # Pre-create the install include dir so targets can reference it at configure
  # time before the ExternalProject has actually run.
  file(MAKE_DIRECTORY "${opentelemetry_INSTALL_PREFIX}/include")

  include(ExternalProject)
  ExternalProject_Add(opentelemetry-cpp
    SOURCE_DIR ${opentelemetry_SOURCE_DIR}
    PREFIX "opentelemetry-cpp"
    CMAKE_ARGS ${opentelemetry_CMAKE_ARGS}
    BUILD_COMMAND ${make_cmd}
    INSTALL_COMMAND ${install_cmd}
    BINARY_DIR ${opentelemetry_BINARY_DIR}
    BUILD_BYPRODUCTS ${opentelemetry_BYPRODUCTS}
    DEPENDS ${dependencies}
    LOG_BUILD ON)

  add_library(libopentelemetry INTERFACE)
  add_dependencies(libopentelemetry opentelemetry-cpp)

  # All add_dependencies() calls referencing opentelemetry-cpp must come after
  # ExternalProject_Add — the target doesn't exist before that point.
  if(_otel_bundled_grpc)
    add_dependencies(utf8 opentelemetry-cpp)
    add_dependencies(upb opentelemetry-cpp)
    add_dependencies(absl opentelemetry-cpp)
    add_dependencies(_grpc opentelemetry-cpp)
    add_dependencies(_protobuf opentelemetry-cpp)
    add_dependencies(protobuf::protoc opentelemetry-cpp)
    add_dependencies(gRPC::grpc_cpp_plugin opentelemetry-cpp)

    find_package(OpenSSL REQUIRED)
    target_link_libraries(libopentelemetry INTERFACE
        -Wl,--start-group
        ${_otel_lib_paths}
        _protobuf gRPC::grpc++
        -Wl,--end-group
        OpenSSL::SSL
        OpenSSL::Crypto)

      # This is ugly, however with static libraries we need the ordering correct
      # protobuf::libprotobuf is often before libopentelemetry, and without this 
      # cmake deduplicates the list, and we then can't resolve the circular dependencies
      # with a later cmake we could do LINK_GROUP, but then with a newer distro we wouldn't be bundling grpc ourselves either!
      add_library(protobuf::libprotobuf ALIAS libopentelemetry)
  else()
    target_link_libraries(libopentelemetry INTERFACE
        -Wl,--start-group
        ${_otel_lib_paths}
        -Wl,--end-group
        protobuf::libprotobuf 
        gRPC::grpc++)
  endif()


  set(opentelemetry_includes "${opentelemetry_SOURCE_DIR}/api/include/" "${opentelemetry_SOURCE_DIR}/sdk/include" "${opentelemetry_INSTALL_PREFIX}/include")
  set_target_properties(libopentelemetry PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${opentelemetry_includes}")
  include_directories(SYSTEM "${opentelemetry_includes}")

endfunction()
