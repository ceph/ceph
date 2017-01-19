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

/**
 * Generates a new cluster id (fsid) and returns a hexadecimal string.
 *
 * @param context where to the store the handle
 * @param buf where to write the fsid
 * @param len the size of buf in bytes (should be at least 37)
 * @returns 0 on success, negative error code on failure
 * @returns -ERANGE if the buffer is too short to contain the key
  */
CEPH_LIBCEPHD_API int cephd_generate_fsid(char *buf, size_t len);

/**
 * Generates a new secret key and returns a base64 encoded string.
 *
 * @param context where to the store the handle
 * @param buf where to write the fsid
 * @param len the size of buf in bytes
 * @returns 0 on success, negative error code on failure
 * @returns -ERANGE if the buffer is too short to contain the key
 */
CEPH_LIBCEPHD_API int cephd_generate_secret_key(char *buf, size_t len);

/**
 * Runs ceph-mon passing in command line args
 *
 * @param argc number of parameters
 * @param argv array of string arguments
 * @returns 0 on success, negative error code on failure
 */
CEPH_LIBCEPHD_API int cephd_run_mon(int argc, const char **argv);

/**
 * Runs ceph-osd passing in command line args
 *
 * @param argc number of parameters
 * @param argv array of string arguments
 * @returns 0 on success, negative error code on failure
 */
CEPH_LIBCEPHD_API int cephd_run_osd(int argc, const char **argv);

/**
 * Runs ceph-mds passing in command line args
 *
 * @param argc number of parameters
 * @param argv array of string arguments
 * @returns 0 on success, negative error code on failure
 */
CEPH_LIBCEPHD_API int cephd_run_mds(int argc, const char **argv);

/**
 * Runs ceph-rgw passing in command line args
 *
 * @param argc number of parameters
 * @param argv array of string arguments
 * @returns 0 on success, negative error code on failure
 */
CEPH_LIBCEPHD_API int cephd_run_rgw(int argc, const char **argv);

/**
 * Runs radosgw-admin passing in command line args
 *
 * @param argc number of parameters
 * @param argv array of string arguments
 * @returns 0 on success, negative error code on failure
 */
CEPH_LIBCEPHD_API int cephd_run_rgw_admin(int argc, const char **argv);

#ifdef __cplusplus
}
#endif
