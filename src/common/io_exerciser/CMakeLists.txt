add_library(object_io_exerciser STATIC
  DataGenerator.cc
  IoOp.cc
  IoSequence.cc
  Model.cc
  ObjectModel.cc
  RadosIo.cc
  EcIoSequence.cc
)

target_link_libraries(object_io_exerciser
  librados
  global
  json_structures
)