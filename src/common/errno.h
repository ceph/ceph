#ifndef CEPH_ERRNO_H
#define CEPH_ERRNO_H

#include <string>

/* Return a given error code as a string */
std::string cpp_strerror(int err);

#ifdef _WIN32
// While cpp_strerror handles errors defined in errno.h, this one
// accepts standard Windows error codes.
std::string win32_strerror(int err);
std::string win32_lasterror_str();
#endif /* _WIN32 */

#endif
