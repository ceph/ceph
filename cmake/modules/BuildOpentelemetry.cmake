function(target_create _target _lib)
  add_library(${_target} STATIC IMPORTED)
  set_target_properties(
    ${_target} PROPERTIES IMPORTED_LOCATION
                          "${opentelemetry_BINARY_DIR}/${_lib}")
endfunction()

function(build_opentelemetry)
  set(opentelemetry_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/jaegertracing/opentelemetry-cpp")
  set(opentelemetry_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-cpp")
  set(opentelemetry_cpp_targets opentelemetry_trace opentelemetry_exporter_jaeger_trace)
  set(opentelemetry_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                               -DWITH_JAEGER=ON
                               -DBUILD_TESTING=OFF
                               -DCMAKE_BUILD_TYPE=Release
                               -DWITH_EXAMPLES=OFF)
  list(APPEND opentelemetry_CMAKE_ARGS ${CEPH_EXTERNAL_PROJECT_CMAKE_ARGS})

  set(opentelemetry_libs
      ${opentelemetry_BINARY_DIR}/sdk/src/trace/libopentelemetry_trace.a
      ${opentelemetry_BINARY_DIR}/sdk/src/resource/libopentelemetry_resources.a
      ${opentelemetry_BINARY_DIR}/sdk/src/common/libopentelemetry_common.a
      ${opentelemetry_BINARY_DIR}/exporters/jaeger/libopentelemetry_exporter_jaeger_trace.a
      ${opentelemetry_BINARY_DIR}/ext/src/http/client/curl/libopentelemetry_http_client_curl.a
  )
  set(opentelemetry_include_dir ${opentelemetry_SOURCE_DIR}/api/include/
                                ${opentelemetry_SOURCE_DIR}/exporters/jaeger/include/
                                ${opentelemetry_SOURCE_DIR}/ext/include/
                                ${opentelemetry_SOURCE_DIR}/sdk/include/)
  # TODO: add target based propogation
  set(opentelemetry_deps opentelemetry_trace opentelemetry_resources opentelemetry_common
                         opentelemetry_exporter_jaeger_trace http_client_curl
			 ${CURL_LIBRARIES})

  if(WITH_OTLP)
    # the OTLP/HTTP exporter; gRPC is not needed. Prefer protobuf's own
    # config: since protobuf 22 its target carries the abseil libraries
    find_package(Protobuf CONFIG QUIET)
    if(NOT Protobuf_FOUND)
      find_package(Protobuf REQUIRED)
    endif()
    list(APPEND opentelemetry_cpp_targets opentelemetry_exporter_otlp_http)
    # abseil, which protobuf 22 and later includes, needs C++17
    list(APPEND opentelemetry_CMAKE_ARGS -DWITH_OTLP=ON
                                         -DWITH_OTLP_HTTP=ON
                                         -DWITH_OTLP_GRPC=OFF
                                         -DCMAKE_CXX_STANDARD=17)
    find_package(absl CONFIG QUIET)
    if(absl_FOUND)
      # opentelemetry-cpp bundles its own copy of abseil, which collides with
      # the system abseil that protobuf brings wherever both are included.
      # Build it against the system abseil instead. HAVE_ABSEIL changes the
      # types in opentelemetry's headers, and tracer.h reaches nearly every
      # source through Message.h, so all of Ceph is compiled with it.
      list(APPEND opentelemetry_CMAKE_ARGS -DWITH_ABSEIL=ON -Dabsl_DIR=${absl_DIR})
      add_compile_definitions(HAVE_ABSEIL)
      # last in the include path: with Homebrew it is /opt/homebrew/include,
      # whose fmt would otherwise shadow the fmt that Ceph bundles. Not when it
      # is a compiler default such as /usr/include: -isystem on that breaks
      # the #include_next in libstdc++'s <cstdlib>.
      get_target_property(absl_include_dir absl::base INTERFACE_INCLUDE_DIRECTORIES)
      list(REMOVE_ITEM absl_include_dir ${CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES})
      set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES
          ${CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES} ${absl_include_dir} PARENT_SCOPE)
      set(otlp_absl_deps absl::bad_variant_access absl::any absl::base absl::bits absl::city)
    endif()
    set(otlp_libs
        exporters/otlp/libopentelemetry_exporter_otlp_http.a
        exporters/otlp/libopentelemetry_exporter_otlp_http_client.a
        exporters/otlp/libopentelemetry_otlp_recordable.a
        libopentelemetry_proto.a)
    foreach(lib ${otlp_libs})
      list(APPEND opentelemetry_libs ${opentelemetry_BINARY_DIR}/${lib})
    endforeach()
    list(APPEND opentelemetry_include_dir
         ${opentelemetry_SOURCE_DIR}/exporters/otlp/include/
         ${opentelemetry_BINARY_DIR}/generated/third_party/opentelemetry-proto/)
    # listed before the SDK libraries they depend on
    list(PREPEND opentelemetry_deps opentelemetry_exporter_otlp_http
                                    opentelemetry_exporter_otlp_http_client
                                    opentelemetry_otlp_recordable
                                    opentelemetry_proto)
    list(APPEND opentelemetry_deps protobuf::libprotobuf ${otlp_absl_deps})
  endif()

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
  set(patch_cmd "")
  if(WITH_OTLP)
    # the OTLP/HTTP client of this opentelemetry-cpp release does not build
    # with protobuf 22 or later; applied once, whether or not it already is
    set(otel_patch ${PROJECT_SOURCE_DIR}/src/jaegertracing/opentelemetry-cpp-protobuf-22.patch)
    set(patch_cmd PATCH_COMMAND sh -c
      "git apply --reverse --check ${otel_patch} 2>/dev/null || git apply ${otel_patch}")
  endif()

  ExternalProject_Add(opentelemetry-cpp
    SOURCE_DIR ${opentelemetry_SOURCE_DIR}
    PREFIX "opentelemetry-cpp"
    ${patch_cmd}
    CMAKE_ARGS ${opentelemetry_CMAKE_ARGS}
    BUILD_COMMAND ${make_cmd}
    BINARY_DIR ${opentelemetry_BINARY_DIR}
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS ${opentelemetry_libs}
    DEPENDS ${dependencies}
    LIST_SEPARATOR !
    LOG_BUILD ON)

  # CMake doesn't allow to add a list of libraries to the import property, hence
  # we create individual targets and link their libraries which finally
  # interfaces to opentelemetry target
  target_create("opentelemetry_trace" "sdk/src/trace/libopentelemetry_trace.a")
  target_create("opentelemetry_resources"
                "sdk/src/resource/libopentelemetry_resources.a")
  target_create("opentelemetry_common"
                "sdk/src/common/libopentelemetry_common.a")
  target_create("opentelemetry_exporter_jaeger_trace"
                "exporters/jaeger/libopentelemetry_exporter_jaeger_trace.a")
  target_create("http_client_curl"
                "ext/src/http/client/curl/libopentelemetry_http_client_curl.a")
  if(WITH_OTLP)
    target_create("opentelemetry_exporter_otlp_http"
                  "exporters/otlp/libopentelemetry_exporter_otlp_http.a")
    target_create("opentelemetry_exporter_otlp_http_client"
                  "exporters/otlp/libopentelemetry_exporter_otlp_http_client.a")
    target_create("opentelemetry_otlp_recordable"
                  "exporters/otlp/libopentelemetry_otlp_recordable.a")
    target_create("opentelemetry_proto" "libopentelemetry_proto.a")
  endif()

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
