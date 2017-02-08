/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
/* vim: ts=8 sw=2 smarttab
*/

/** \mainpage
 * Contains the C and C++ APIs for Ceph's LibOSD, which allows an
 * application to bring up one or more Ceph OSDs within the process.
 *
 * The application must link against libceph-osd.so.
 */
/** \file */

#ifndef LIBCEPH_OSD_H
#define LIBCEPH_OSD_H

#include <stdint.h>

/**
 * Completion callback for asynchronous io
 *
 * @param result  The operation result
 * @param length  Number of bytes read/written
 * @param flags	  LIBOSD_READ_/WRITE_ flags
 * @param user	  Private user data passed to libosd_read/write/truncate()
 */
typedef void (*libosd_io_completion_fn)(int result, uint64_t length,
					int flags, void *user);

#ifdef __cplusplus

/**
 * The abstract C++ libosd interface, whose member functions take
 * the same arguments as the C interface.
 *
 * Must be created with libosd_init() and destroyed with libosd_cleanup().
 */
struct libosd {
  const int whoami; /**< osd instance id */
  libosd(int name) : whoami(name) {}

  /**
   * Blocks until the osd shuts down.
   * @see libosd_join()
   */
  virtual void join() = 0;

  /**
   * Starts shutting down a running osd.
   * @see libosd_shutdown()
   */
  virtual void shutdown() = 0;

  /**
   * Send the given signal to this osd.
   * @see libosd_signal()
   */
  virtual void signal(int signum) = 0;

  /**
   * Look up a volume by name to get its uuid.
   * @see libosd_get_volume()
   */
  virtual int64_t get_volume(const char *name,
			     uint8_t id[16]) = 0;

  /**
   * Read from an object.
   * @see libosd_read()
   */
  virtual int read(const char *object, const uint8_t volume[16],
		   uint64_t offset, uint64_t length, char *data,
		   int flags, libosd_io_completion_fn cb, void *user) = 0;

  /** Write to an object.
   * @see libosd_write()
   */
  virtual int write(const char *object, const uint8_t volume[16],
		    uint64_t offset, uint64_t length, char *data,
		    int flags, libosd_io_completion_fn cb, void *user) = 0;

  /** Truncate an object.
   * @see libosd_truncate()
   */
  virtual int truncate(const char *object, uint8_t volume[16],
		       uint64_t offset, int flags,
		       libosd_io_completion_fn cb, void *user) = 0;

protected:
  /** Destructor protected: must be deleted by libosd_cleanup() */
  virtual ~libosd() {}
};


/* C interface */
extern "C" {
#else
struct libosd;
#endif /* __cplusplus */

/** osd callback function table */
struct libosd_callbacks {
  /**
   * Called when the OSD's OSDMap state switches to up:active.
   *
   * @param osd	  The libosd object returned by libosd_init()
   * @param user  Private user data provided in libosd_init_args
   */
  void (*osd_active)(struct libosd *osd, void *user);

  /**
   * Called if the OSD decides to shut down on its own, not as
   * a result of libosd_shutdown().
   *
   * @param osd	  The libosd object returned by libosd_init()
   * @param user  Private user data provided in libosd_init_args
   */
  void (*osd_shutdown)(struct libosd *osd, void *user);
};

/** Initialization arguments for libosd_init() */
struct libosd_init_args {
  int id;		/**< osd instance id */
  const char *config;	/**< path to ceph configuration file */
  const char *cluster;	/**< ceph cluster name (default "ceph") */
  struct libosd_callbacks *callbacks; /**< optional callbacks */
  const char **argv;	/**< command-line argument array */
  int argc;		/**< size of argv array */
  void *user;		/**< user data for osd_active and osd_shutdown */
};

/**
 * Create and initialize an osd from the given arguments. Reads
 * the ceph.conf, binds messengers, creates an objectstore,
 * and starts running the osd. Returns before initialization is
 * complete; refer to osd_active callback to determine when the
 * osd becomes active.
 *
 * @param args	  Initialization arguments
 *
 * @return A pointer to the new libosd instance, or NULL on failure.
 */
struct libosd* libosd_init(const struct libosd_init_args *args);

/**
 * Blocks until the osd shuts down, either because of a call to
 * libosd_shutdown(), or an osd_shutdown() callback initiated by the osd.
 *
 * @param osd	  The libosd object returned by libosd_init()
 */
void libosd_join(struct libosd *osd);

/**
 * Starts shutting down a running osd.
 *
 * @param osd	  The libosd object returned by libosd_init()
 */
void libosd_shutdown(struct libosd *osd);

/**
 * Release resources associated with an osd that is not running.
 *
 * @param osd	  The libosd object returned by libosd_init()
 */
void libosd_cleanup(struct libosd *osd);

/**
 * Send the given signal to all osds.
 *
 * @param signum  The signal from a signal handler
 */
void libosd_signal(int signum);

/**
 * Look up a volume by name to get its uuid.
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param name    The volume name in the osd map
 * @param uuid    The uuid to be assigned
 *
 * @retval 0 on success.
 * @retval -ENOENT if not found.
 * @retval -ENODEV if the OSD is shutting down.
 */
int libosd_get_volume(struct libosd *osd, const char *name,
		      uint8_t uuid[16]);

/* Completion flags for libosd_read() */
#define LIBOSD_READ_FLAGS_NONE	  0x0

/**
 * Read from an object. The function is asynchronous is a callback function
 * is given.
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_get_volume()
 * @param offset  Offset into the object
 * @param length  Number of bytes to read
 * @param data	  Buffer to receive the object data
 * @param flags	  LIBOSD_READ_ flags
 * @param cb	  Read completion callback (optional)
 * @param user	  User data passed to the callback (optional)
 *
 * @return Returns the number of bytes read synchronously, 0 if the read
 * request was submitted asynchronously, or a negative error code on failure.
 * @retval -ENODEV if the OSD is shutting down.
 */
int libosd_read(struct libosd *osd, const char *object, const uint8_t volume[16],
		uint64_t offset, uint64_t length, char *data,
		int flags, libosd_io_completion_fn cb, void *user);

/* Completion flags for libosd_write() and libosd_truncate() */
#define LIBOSD_WRITE_FLAGS_NONE	  0x0
/** Request a completion once the data is written to cache */
#define LIBOSD_WRITE_CB_UNSTABLE  0x01
/** Request a completion once the data is written to stable storage */
#define LIBOSD_WRITE_CB_STABLE	  0x02

/**
 * Write to an object. The function is asynchronous if a callback function
 * is given.
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_get_volume()
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
 * @retval -ENODEV if the OSD is shutting down.
 */
int libosd_write(struct libosd *osd, const char *object, const uint8_t volume[16],
		 uint64_t offset, uint64_t length, char *data,
		 int flags, libosd_io_completion_fn cb, void *user);

/**
 * Truncate an object. The function is asynchronous if a callback function
 * is given.
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_get_volume()
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
 * @retval -ENODEV if the OSD is shutting down.
 */
int libosd_truncate(struct libosd *osd, const char *object,
		    uint8_t volume[16], uint64_t offset,
		    int flags, libosd_io_completion_fn cb, void *user);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_OSD_H */
