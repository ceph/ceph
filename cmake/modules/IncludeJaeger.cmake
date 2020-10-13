###############################################################################
#Ceph - scalable distributed file system
#
#Copyright (C) 2020 Red Hat Inc.
#
#This is free software; you can redistribute it and/or
#modify it under the terms of the GNU Lesser General Public
#License version 2.1, as published by the Free Software
#Foundation.  See file COPYING.
################################################################################

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
