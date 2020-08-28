include(BuildJaeger)
include(BuildOpenTracing)
include(Buildthrift)
include(Buildyaml-cpp)

include(ExternalProjectHelper)

build_thrift()
build_yamlcpp()
build_opentracing()

set(dependencies OpenTracing thrift yaml-cpp)

build_jaeger()

add_library(thrift::libthrift SHARED IMPORTED)
add_dependencies(thrift::libthrift thrift)
add_library(yaml-cpp::yaml-cpp SHARED IMPORTED)
add_dependencies(yaml-cpp::yaml-cpp yaml-cpp)
add_library(opentracing::libopentracing SHARED IMPORTED)
add_dependencies(opentracing::libopentracing OpenTracing)
add_library(jaegertracing::libjaegertracing SHARED IMPORTED)
add_dependencies(jaegertracing::libjaegertracing Jaeger)

#(set_library_properties_for_external_project _target _lib)
set_library_properties_for_external_project(opentracing::libopentracing
  opentracing ${OpenTracing_FOUND} ${OpenTracing_INCLUDE_DIRS} ${OpenTracing_LIBRARIES})

set_library_properties_for_external_project(jaegertracing::libjaegertracing
  jaegertracing ${Jaeger_FOUND} ${Jaeger_INCLUDE_DIRS} ${Jaeger_LIBRARIES})

set_library_properties_for_external_project(thrift::libthrift
  thrift ${thrift_FOUND} ${thrift_INCLUDE_DIRS} ${thrift_LIBRARIES})

set_library_properties_for_external_project(yaml-cpp::yaml-cpp
  yaml-cpp ${YAML_CPP_FOUND} ${yaml-cpp_INCLUDE_DIRS} ${yaml-cpp_LIBRARIES})
