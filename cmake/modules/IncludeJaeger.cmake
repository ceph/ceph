include(BuildJaeger)
include(BuildOpenTracing)

include(ExternalProjectHelper)

build_jaeger()

add_library(opentracing::libopentracing SHARED IMPORTED)
add_dependencies(opentracing::libopentracing opentracing)
add_library(jaegertracing::libjaegertracing SHARED IMPORTED)
add_dependencies(jaegertracing::libjaegertracing Jaeger)
add_library(thrift::libthrift SHARED IMPORTED)
add_dependencies(thrift::libthrift thrift)

#(set_library_properties_for_external_project _target _lib)
set_library_properties_for_external_project(opentracing::libopentracing
  opentracing)
set_library_properties_for_external_project(jaegertracing::libjaegertracing
  jaegertracing)
set_library_properties_for_external_project(thrift::libthrift
  thrift)
