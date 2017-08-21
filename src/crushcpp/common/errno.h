#ifndef CEPH_ERRNO_H
#define CEPH_ERRNO_H

#include <string>

/* Return a given error code as a string */
std::string cpp_strerror(int err);

#endif
