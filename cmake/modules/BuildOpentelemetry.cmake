function(target_create _target _lib)
  add_library(${_target} STATIC IMPORTED)
  set_target_properties(
    ${_target} PROPERTIES IMPORTED_LOCATION
                          "${opentelemetry_BINARY_DIR}/${_lib}")
endfunction()

function(build_opentelemetry)
  set(opentelemetry_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/jaegertracing/opentelemetry-cpp")
  set(opentelemetry_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-cpp")
  set(opentelemetry_cpp_targets opentelemetry_trace opentelemetry_exporter_otlp_grpc opentelemetry_exporter_otlp_grpc_client)
  set(opentelemetry_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                               -DWITH_OTLP_GRPC=ON
                               -DWITH_OTLP=ON
                               -DBUILD_TESTING=OFF
                               -DCMAKE_BUILD_TYPE=Release
                               -DWITH_EXAMPLES=OFF)

  set(opentelemetry_libs
      ${opentelemetry_BINARY_DIR}/sdk/src/trace/libopentelemetry_trace.a
      ${opentelemetry_BINARY_DIR}/sdk/src/logs/libopentelemetry_logs.a
      ${opentelemetry_BINARY_DIR}/sdk/src/metrics/libopentelemetry_metrics.a
      ${opentelemetry_BINARY_DIR}/sdk/src/resource/libopentelemetry_resources.a
      ${opentelemetry_BINARY_DIR}/sdk/src/common/libopentelemetry_common.a
      ${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc.a
      ${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_otlp_recordable.a
      ${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc_client.a
      ${opentelemetry_BINARY_DIR}/libopentelemetry_proto.so
      ${opentelemetry_BINARY_DIR}/libopentelemetry_proto_grpc.so
      #${CURL_LIBRARIES}
  )
  set(opentelemetry_include_dir ${opentelemetry_SOURCE_DIR}/api/include/
                                ${opentelemetry_SOURCE_DIR}/exporters/otlp/include/
                                ${opentelemetry_SOURCE_DIR}/ext/include/
                                ${opentelemetry_SOURCE_DIR}/sdk/include/
                                ${opentelemetry_BINARY_DIR}
                                ${opentelemetry_BINARY_DIR}/generated/third_party/opentelemetry-proto)

  # Find required dependencies for OTLP gRPC
  find_package(Protobuf REQUIRED)
  find_package(gRPC REQUIRED)
  
  # TODO: add target based propogation
  set(opentelemetry_deps 
      opentelemetry_exporter_otlp_grpc        # 1. High-level exporter
      opentelemetry_exporter_otlp_grpc_client # 2. The gRPC client implementation
      opentelemetry_otlp_recordable           # 3. Data translation
      opentelemetry_proto_grpc                # 4. gRPC Proto definitions
      opentelemetry_proto                     # 5. Base OTLP Protos
      opentelemetry_trace                     # 6. Core SDK
      opentelemetry_logs
      opentelemetry_resources                 # 7. Metadata
      opentelemetry_common                    # 8. Shared utils
      gRPC::grpc++                            # 9. External transport
      ${PROTOBUF_LIBRARIES}                   # 10. External serialization
)
  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE) ${opentelemetry_cpp_targets})
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target
                 ${opentelemetry_cpp_targets})
  endif()

  if(WITH_SYSTEM_BOOST)
    list(APPEND opentelemetry_CMAKE_ARGS -DBOOST_ROOT=${BOOST_ROOT})
  else()
    list(APPEND dependencies Boost)
    list(APPEND opentelemetry_CMAKE_ARGS -DBoost_INCLUDE_DIR=${CMAKE_BINARY_DIR}/boost/include)
  endif()

  # Check if CMake version is >= 4.0.0
  if(CMAKE_VERSION VERSION_GREATER_EQUAL "4.0.0")
    # Use CMAKE_POLICY_VERSION_MINIMUM if set, otherwise default to 3.5
    if(DEFINED CMAKE_POLICY_VERSION_MINIMUM)
      list(APPEND opentelemetry_CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=${CMAKE_POLICY_VERSION_MINIMUM})
    else()
      list(APPEND opentelemetry_CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5)
    endif()
  endif()

  include(ExternalProject)
  ExternalProject_Add(opentelemetry-cpp
    SOURCE_DIR ${opentelemetry_SOURCE_DIR}
    PREFIX "opentelemetry-cpp"
    CMAKE_ARGS ${opentelemetry_CMAKE_ARGS}
    BUILD_COMMAND ${make_cmd}
    BINARY_DIR ${opentelemetry_BINARY_DIR}
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS ${opentelemetry_libs}
    DEPENDS ${dependencies}
    LOG_BUILD ON)

  # CMake doesn't allow to add a list of libraries to the import property, hence
  # we create individual targets and link their libraries which finally
  # interfaces to opentelemetry target
  target_create("opentelemetry_exporter_otlp_grpc" "exporters/otlp/libopentelemetry_exporter_otlp_grpc.a")
  target_create("opentelemetry_exporter_otlp_grpc_client" "exporters/otlp/libopentelemetry_exporter_otlp_grpc_client.a")
  target_create("opentelemetry_otlp_recordable" "exporters/otlp/libopentelemetry_otlp_recordable.a")
  target_create("opentelemetry_trace" "sdk/src/trace/libopentelemetry_trace.a")
  target_create("opentelemetry_resources" "sdk/src/resource/libopentelemetry_resources.a")
  target_create("opentelemetry_common" "sdk/src/common/libopentelemetry_common.a")
  target_create("opentelemetry_logs" "sdk/src/logs/libopentelemetry_logs.a")
  target_create("opentelemetry_metrics" "sdk/src/metrics/libopentelemetry_metrics.a")
  
  # Create targets for shared proto libraries
  add_library(opentelemetry_proto SHARED IMPORTED)
  set_target_properties(opentelemetry_proto PROPERTIES
    IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/libopentelemetry_proto.so")
  add_dependencies(opentelemetry_proto opentelemetry-cpp)
  
  add_library(opentelemetry_proto_grpc SHARED IMPORTED)
  set_target_properties(opentelemetry_proto_grpc PROPERTIES
    IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/libopentelemetry_proto_grpc.so")
  add_dependencies(opentelemetry_proto_grpc opentelemetry-cpp)

  # will do all linking and path setting fake include path for
  # interface_include_directories since this happens at build time
  file(MAKE_DIRECTORY ${opentelemetry_include_dir})
  add_library(opentelemetry::libopentelemetry INTERFACE IMPORTED)
  add_dependencies(opentelemetry::libopentelemetry opentelemetry-cpp)
  set_target_properties(
    opentelemetry::libopentelemetry
    PROPERTIES
      INTERFACE_LINK_LIBRARIES "${opentelemetry_deps}"
      INTERFACE_INCLUDE_DIRECTORIES "${opentelemetry_include_dir}")
  include_directories(SYSTEM "${opentelemetry_include_dir}")
endfunction()
