#include "global/error_handlers.h"

static error_handler_t io_tidy_handler;

void register_ceph_io_error_handler(error_handler_t handler) 
{
  io_tidy_handler = handler;
}

void ceph_io_error_tidy_shutdown() 
{
  if (io_tidy_handler) {
    io_tidy_handler();
  }
}
