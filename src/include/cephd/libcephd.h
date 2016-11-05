#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#define LIBCEPHD_VER_MAJOR 0
#define LIBCEPHD_VER_MINOR 1
#define LIBCEPHD_VER_PATCH 0

#define LIBCEPHFD_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define LIBCEPHFD_VERSION_CODE LIBCEPHD_VERSION(LIBCEPHD_VER_MAJOR, LIBCEPHD_VER_MINOR, LIBCEPHD_VER_PATCH)

#define CEPH_LIBCEPHD_API __attribute__ ((visibility ("default")))

/**
 * Get the API version of libcephd. We use semantic versioning
 * for the API:
 *
 * - incrementing major is for backwards-incompatible changes
 * - incrementing minor is for backwards-compatible changes
 * - incrementing extra is for bug fixes
 *
 * @param pmajor where to store the major version number
 * @param pminor where to store the minor version number
 * @param ppatch where to store the patch version number
 */
CEPH_LIBCEPHD_API void cephd_version(int *pmajor, int *pminor, int *ppatch);

/**
 * Gets the runtime version of ceph.
 *
 * @param pmajor where to store the major version number
 * @param pminor where to store the minor version number
 * @param ppatch where to store the patch version number
 */
CEPH_LIBCEPHD_API const char *ceph_version(int *pmajor, int *pminor, int *ppatch);

#ifdef __cplusplus
}
#endif
