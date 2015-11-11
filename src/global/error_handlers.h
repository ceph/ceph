#ifndef CEPH_GLOBAL_ERROR_HANDLER_H
#define CEPH_GLOBAL_ERROR_HANDLER_H

typedef void (*error_handler_t)();

void register_ceph_io_error_handler(error_handler_t handler);
void ceph_io_error_tidy_shutdown(); 

#endif
