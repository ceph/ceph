/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
/* vim: ts=8 sw=2 smarttab
*/

/** \file
 * Contains the C and C++ APIs for Ceph's remote LibOSD, which allows an
 * application to communicate with an out-of-process OSD with an interface
 * similar to @libosd.
 */

#ifndef LIBCEPH_OSD_REMOTE_H
#define LIBCEPH_OSD_REMOTE_H

#include "ceph_osd.h"

#ifdef __cplusplus

/**
 * The abstract C++ libosd_remote interface, whose member functions take
 * the same arguments as the C interface.
 *
 * Must be created with libosd_remote_init() and destroyed with
 * libosd_remote_cleanup().
 */
struct libosd_remote {
  const int whoami; /**< osd instance id */
  libosd_remote(int name) : whoami(name) {}

  /**
   * Look up a volume by name to get its uuid.
   * @see libosd_remote_get_volume()
   */
  virtual int get_volume(const char *name, uint8_t id[16]) = 0;

  /**
   * Read from an object.
   * @see libosd_remote_read()
   */
  virtual int read(const char *object, const uint8_t volume[16],
		   uint64_t offset, uint64_t length, char *data,
		   int flags, libosd_io_completion_fn cb, void *user) = 0;

  /** Write to an object.
   * @see libosd_remote_write()
   */
  virtual int write(const char *object, const uint8_t volume[16],
		    uint64_t offset, uint64_t length, char *data,
		    int flags, libosd_io_completion_fn cb, void *user) = 0;

  /** Truncate an object.
   * @see libosd_remote_truncate()
   */
  virtual int truncate(const char *object, const uint8_t volume[16],
		       uint64_t offset, int flags,
		       libosd_io_completion_fn cb, void *user) = 0;

protected:
  /** Destructor protected: must be deleted by libosd_remote_cleanup() */
  virtual ~libosd_remote() {}
};

/* C interface */
extern "C" {
#else
struct libosd_remote;
#endif /* __cplusplus */

/** Initialization arguments for libosd_remote_init() */
struct libosd_remote_args {
  int id;		/**< osd instance id */
  const char *config;	/**< path to ceph configuration file */
  const char *cluster;	/**< ceph cluster name (default "ceph") */
  const char **argv;    /**< command-line argument array */
  int argc;             /**< size of argv array */
};

/**
 * Connect to the Ceph OSD specified in the arguments. Reads the
 * ceph.conf, creates a messenger, starts a MonClient to authenticate
 * with the cluster and obtain an OSDMap, then uses establishes a
 * connection to the requested OSD.
 *
 * @param args	  Initialization arguments
 *
 * @return A pointer to the new libosd_remote instance, or NULL on failure.
 */
struct libosd_remote* libosd_remote_init(const struct libosd_remote_args *args);

/**
 * Release resources associated with an osd remote.
 *
 * @param osd	  The libosd_remote object returned by libosd_remote_init()
 */
void libosd_remote_cleanup(struct libosd_remote *osd);

/**
 * Look up a volume by name to get its uuid.
 *
 * @param osd	  The libosd_remote object returned by libosd_remote_init()
 * @param name    The volume name in the osd map
 * @param uuid    The uuid to be assigned
 *
 * @retval 0 on success.
 * @retval -ENOENT if not found.
 * @retval -ENODEV if the OSD is unreachable.
 */
int libosd_remote_get_volume(struct libosd_remote *osd, const char *name,
                             uint8_t uuid[16]);

/**
 * Read from an object. The function is asynchronous is a callback function
 * is given.
 *
 * @param osd	  The libosd_remote object returned by libosd_remote_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_remote_get_volume()
 * @param offset  Offset into the object
 * @param length  Number of bytes to read
 * @param data	  Buffer to receive the object data
 * @param flags	  LIBOSD_READ_ flags
 * @param cb	  Read completion callback (optional)
 * @param user	  User data passed to the callback (optional)
 *
 * @return Returns the number of bytes read synchronously, 0 if the read
 * request was submitted asynchronously, or a negative error code on failure.
 * @retval -ENODEV if the OSD is unreachable.
 */
int libosd_remote_read(struct libosd_remote *osd, const char *object,
                       const uint8_t volume[16], uint64_t offset,
                       uint64_t length, char *data, int flags,
                       libosd_io_completion_fn cb, void *user);

/**
 * Write to an object. The function is asynchronous if a callback function
 * is given.
 *
 * @param osd	  The libosd_remote object returned by libosd_remote_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_remote_get_volume()
 * @param offset  Offset into the object
 * @param length  Number of bytes to write
 * @param data	  Buffer to receive the object data
 * @param flags	  LIBOSD_WRITE_ flags
 * @param cb	  Write completion callback (optional)
 * @param user	  User data passed to the callback (optional)
 *
 * @return Returns the number of bytes written synchronously, 0 if the write
 * request was submitted asynchronously, or a negative error code on failure.
 * @retval -EINVAL if a write completion is given and flags contains neither
 * LIBOSD_WRITE_CB_UNSTABLE nor LIBOSD_WRITE_CB_STABLE.
 * @retval -EINVAL if no write completion is given and flags does not contain
 * exactly one of LIBOSD_WRITE_CB_UNSTABLE or LIBOSD_WRITE_CB_STABLE.
 * @retval -ENODEV if the OSD is unreachable.
 */
int libosd_remote_write(struct libosd_remote *osd, const char *object,
                        const uint8_t volume[16], uint64_t offset,
                        uint64_t length, char *data, int flags,
                        libosd_io_completion_fn cb, void *user);

/**
 * Truncate an object. The function is asynchronous if a callback function
 * is given.
 *
 * @param osd	  The libosd_remote object returned by libosd_remote_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_remote_get_volume()
 * @param offset  Offset into the object
 * @param flags	  LIBOSD_WRITE_ flags
 * @param cb	  Truncate completion callback (optional)
 * @param user	  User data passed to the callback (optional)
 *
 * @return Returns 0 if the write request was completed synchronously or
 * was submitted asynchronously, or a negative error code on failure.
 * @retval -EINVAL if a write completion is given and flags contains neither
 * LIBOSD_WRITE_CB_UNSTABLE nor LIBOSD_WRITE_CB_STABLE.
 * @retval -EINVAL if no write completion is given and flags does not contain
 * exactly one of LIBOSD_WRITE_CB_UNSTABLE or LIBOSD_WRITE_CB_STABLE.
 * @retval -ENODEV if the OSD is unreachable.
 */
int libosd_remote_truncate(struct libosd_remote *osd, const char *object,
                           const uint8_t volume[16], uint64_t offset,
                           int flags, libosd_io_completion_fn cb, void *user);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_OSD_REMOTE_H */
