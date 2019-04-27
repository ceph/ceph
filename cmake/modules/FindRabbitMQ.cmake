find_path(rabbitmq_INCLUDE_DIR
  NAMES amqp.h)

find_library(rabbitmq_LIBRARY
  NAMES rabbitmq)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(RabbitMQ DEFAULT_MSG
  rabbitmq_INCLUDE_DIR
  rabbitmq_LIBRARY)

if(RabbitMQ_FOUND AND NOT (TARGET RabbitMQ::RabbitMQ))
  add_library(RabbitMQ::RabbitMQ UNKNOWN IMPORTED)
  set_target_properties(RabbitMQ::RabbitMQ PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${rabbitmq_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${rabbitmq_LIBRARY}")
endif()
