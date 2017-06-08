#ifndef CEPH_LIBRADOSSTRIPER_H
#define CEPH_LIBRADOSSTRIPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <string.h>

#include "../rados/librados.h"

#define LIBRADOSSTRIPER_VER_MAJOR 0
#define LIBRADOSSTRIPER_VER_MINOR 0
#define LIBRADOSSTRIPER_VER_EXTRA 0

#define LIBRADOSSTRIPER_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRADOSSTRIPER_VERSION_CODE LIBRADOSSTRIPER_VERSION(LIBRADOSSTRIPER_VER_MAJOR, LIBRADOSSTRIPER_VER_MINOR, LIBRADOSSTRIPER_VER_EXTRA)

/**
 * @typedef rados_striper_t
 *
 * A handle for interacting with striped objects in a RADOS cluster.
 */
typedef void *rados_striper_t;

/**
 * @defgroup libradosstriper_h_init Setup and Teardown
 * These are the first and last functions to that should be called
 * when using libradosstriper.
 *
 * @{
 */

/**
 * Creates a rados striper using the given io context
 * Striper has initially default object layout.
 * See rados_striper_set_object_layout_*() to change this
 *
 * @param ioctx the rados context to use
 * @param striper where to store the rados striper
 * @returns 0 on success, negative error code on failure
 */
  int rados_striper_create(rados_ioctx_t ioctx,
                           rados_striper_t *striper);

/**
 * Destroys a rados striper
 *
 * @param striper the striper to destroy
 */
void rados_striper_destroy(rados_striper_t striper);

/**
 * Sets the object layout's stripe unit of a rados striper for future objects.
 * This layout will be used when new objects are created (by writing to them)
 * Already existing objects will be opened with their own layout.
 *
 * @param striper the targetted striper
 * @param stripe_unit the stripe_unit value of the new object layout
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_set_object_layout_stripe_unit(rados_striper_t striper,
                                                unsigned int stripe_unit);

/**
 * Sets the object layout's stripe count of a rados striper for future objects.
 * This layout will be used when new objects are created (by writing to them)
 * Already existing objects will be opened with their own layout.
 *
 * @param striper the targetted striper
 * @param stripe_count the stripe_count value of the new object layout
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_set_object_layout_stripe_count(rados_striper_t striper,
                                                 unsigned int stripe_count);

/**
 * Sets the object layout's object_size of a rados striper for future objects.
 * This layout will be used when new objects are created (by writing to them)
 * Already existing objects will be opened with their own layout.
 *
 * @param striper the targetted striper
 * @param object_size the object_size value of the new object layout
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_set_object_layout_object_size(rados_striper_t striper,
                                                unsigned int object_size);

/** @} init */

/**
 * @defgroup libradosstriper_h_synch_io Synchronous I/O
 * Writes are striped to several rados objects which are then
 * replicated to a number of OSDs based on the configuration
 * of the pool they are in. These write functions block
 * until data is in memory on all replicas of the object they're
 * writing to - they are equivalent to doing the corresponding
 * asynchronous write, and the calling
 * rados_striper_ioctx_wait_for_complete().
 *
 * @{
 */

/**
 * Synchronously write data to a striped object at the specified offset
 *
 * @param striper the striper in which the write will occur
 * @param soid the name of the striped object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @param off byte offset in the object to begin writing at
 * @returns 0 on success, negative error code on failure
 * failure
 */
int rados_striper_write(rados_striper_t striper,
                        const char *soid,
                        const char *buf,
                        size_t len,
                        uint64_t off);

/**
 * Synchronously write an entire striped object
 *
 * The striped object is filled with the provided data. If the striped object exists,
 * it is truncated and then written.
 *
 * @param striper the striper in which the write will occur
 * @param soid the name of the striped object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_write_full(rados_striper_t striper,
                             const char *soid,
                             const char *buf,
                             size_t len);

/**
 * Append data to an object
 *
 * @param striper the striper in which the write will occur
 * @param soid the name of the striped object
 * @param buf the data to append
 * @param len length of buf (in bytes)
 * @returns 0 on success, negative error code on failure
 * failure
 */
int rados_striper_append(rados_striper_t striper,
                         const char *soid,
                         const char *buf,
                         size_t len);

/**
 * Synchronously read data from a striped object at the specified offset
 *
 * @param striper the striper in which the read will occur
 * @param soid the name of the striped object
 * @param buf where to store the results
 * @param len the number of bytes to read
 * @param off the offset to start reading from in the object
 * @returns number of bytes read on success, negative error code on
 * failure
 */
int rados_striper_read(rados_striper_t striper,
                       const char *soid,
                       char *buf,
                       size_t len,
                       uint64_t off);

/**
 * Synchronously removes a striped object
 *
 * @note There is no atomicity of the deletion and the striped
 * object may be left incomplete if an error is returned (metadata
 * all present, but some stripes missing)
 * However, there is a atomicity of the metadata deletion and
 * the deletion can not happen if any I/O is ongoing (it
 * will return EBUSY). Identically, no I/O will be able to start
 * during deletion (same EBUSY return code)
 * @param striper the striper in which the remove will occur
 * @param soid the name of the striped object
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_remove(rados_striper_t striper,
                         const char* soid);

/**
 * Resize an object
 *
 * If this enlarges the object, the new area is logically filled with
 * zeroes. If this shrinks the object, the excess data is removed.
 *
 * @note the truncation is not fully atomic. The metadata part is,
 * so the behavior will be atomic from user point of view when
 * the object size is reduced. However, in case of failure, old data
 * may stay around, hidden. They may reappear if the object size is
 * later grown, instead of the expected 0s. When growing the
 * object and in case of failure, the new 0 data may not be
 * fully created. This can lead to ENOENT errors when
 * writing/reading the missing parts.
 * @note the truncation can not happen if any I/O is ongoing (it
 * will return EBUSY). Identically, no I/O will be able to start
 * during truncation (same EBUSY return code)
 * @param io the rados context to use
 * @param soid the name of the striped object
 * @param size the new size of the object in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_trunc(rados_ioctx_t io, const char *soid, uint64_t size);

/** @} Synchronous I/O */

/**
 * @defgroup libradosstriper_h_xattrs Xattrs
 * Extended attributes are stored as extended attributes on the
 * first rados regular object of the striped object.
 * Thus, they have the same limitations as the underlying
 * rados extended attributes.
 *
 * @{
 */

/**
 * Get the value of an extended attribute on a striped object.
 *
 * @param striper the striper in which the getxattr will occur
 * @param oid name of the striped object
 * @param name which extended attribute to read
 * @param buf where to store the result
 * @param len size of buf in bytes
 * @returns length of xattr value on success, negative error code on failure
 */
int rados_striper_getxattr(rados_striper_t striper,
                           const char *oid,
                           const char *name,
                           char *buf,
                           size_t len);

/**
 * Set an extended attribute on a striped object.
 *
 * @param striper the striper in which the setxattr will occur
 * @param oid name of the object
 * @param name which extended attribute to set
 * @param buf what to store in the xattr
 * @param len the number of bytes in buf
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_setxattr(rados_striper_t striper,
                           const char *oid,
                           const char *name,
                           const char *buf,
                           size_t len);

/**
 * Delete an extended attribute from a striped object.
 *
 * @param striper the striper in which the rmxattr will occur
 * @param oid name of the object
 * @param name which xattr to delete
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_rmxattr(rados_striper_t striper,
                          const char *oid,
                          const char *name);

/**
 * Start iterating over xattrs on a striped object.
 *
 * @post iter is a valid iterator
 *
 * @param striper the striper in which the getxattrs will occur
 * @param oid name of the object
 * @param iter where to store the iterator
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_getxattrs(rados_striper_t striper,
                            const char *oid,
                            rados_xattrs_iter_t *iter);

/**
 * Get the next xattr on the striped object
 *
 * @pre iter is a valid iterator
 *
 * @post name is the NULL-terminated name of the next xattr, and val
 * contains the value of the xattr, which is of length len. If the end
 * of the list has been reached, name and val are NULL, and len is 0.
 *
 * @param iter iterator to advance
 * @param name where to store the name of the next xattr
 * @param val where to store the value of the next xattr
 * @param len the number of bytes in val
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_getxattrs_next(rados_xattrs_iter_t iter,
                                 const char **name,
                                 const char **val,
                                 size_t *len);

/**
 * Close the xattr iterator.
 *
 * iter should not be used after this is called.
 *
 * @param iter the iterator to close
 */
void rados_striper_getxattrs_end(rados_xattrs_iter_t iter);

/** @} Xattrs */

/**
 * Synchronously get object stats (size/mtime)
 *
 * @param striper the striper in which the stat will occur
 * @param soid the id of the striped object
 * @param psize where to store object size
 * @param pmtime where to store modification time
 * @returns 0 on success, negative error code on failure
 */
int rados_striper_stat(rados_striper_t striper,
                       const char* soid,
                       uint64_t *psize,
                       time_t *pmtime);

/**
 * @defgroup libradosstriper_h_asynch_io Asynchronous I/O
 * Read and write to objects without blocking.
 *
 * @{
 */

/**
 * @typedef rados_striper_multi_completion_t
 * Represents the state of a set of asynchronous operations
 * it contains the aggregated return value once the operations complete
 * and can be used to block until all operations are complete and/or safe.
 */
typedef void *rados_striper_multi_completion_t;

/**
 * Constructs a multi completion to use with asynchronous operations
 *
 * The complete and safe callbacks correspond to operations being
 * acked and committed, respectively. The callbacks are called in
 * order of receipt, so the safe callback may be triggered before the
 * complete callback, and vice versa. This is affected by journalling
 * on the OSDs.
 *
 * @note Read operations only get a complete callback.
 * @note BUG: this should check for ENOMEM instead of throwing an exception
 *
 * @param cb_arg application-defined data passed to the callback functions
 * @param cb_complete the function to be called when the operation is
 * in memory on all relpicas
 * @param cb_safe the function to be called when the operation is on
 * stable storage on all replicas
 * @param pc where to store the completion
 * @returns 0
 */
int rados_striper_multi_aio_create_completion(void *cb_arg,
                                              rados_callback_t cb_complete,
                                              rados_callback_t cb_safe,
                                              rados_striper_multi_completion_t *pc);

/**
 * Block until all operation complete
 *
 * This means data is in memory on all replicas.
 *
 * @param c operations to wait for
 * @returns 0
 */
void rados_striper_multi_aio_wait_for_complete(rados_striper_multi_completion_t c);

/**
 * Block until all operation are safe
 *
 * This means data is on stable storage on all replicas.
 *
 * @param c operations to wait for
 * @returns 0
 */
void rados_striper_multi_aio_wait_for_safe(rados_striper_multi_completion_t c);

/**
 * Has a multi asynchronous operation completed?
 *
 * @warning This does not imply that the complete callback has
 * finished
 *
 * @param c async operations to inspect
 * @returns whether c is complete
 */
int rados_striper_multi_aio_is_complete(rados_striper_multi_completion_t c);

/**
 * Is a multi asynchronous operation safe?
 *
 * @warning This does not imply that the safe callback has
 * finished
 *
 * @param c async operations to inspect
 * @returns whether c is safe
 */
int rados_striper_multi_aio_is_safe(rados_striper_multi_completion_t c);

/**
 * Block until all operations complete and callback completes
 *
 * This means data is in memory on all replicas and can be read.
 *
 * @param c operations to wait for
 * @returns 0
 */
void rados_striper_multi_aio_wait_for_complete_and_cb(rados_striper_multi_completion_t c);

/**
 * Block until all operations are safe and callback has completed
 *
 * This means data is on stable storage on all replicas.
 *
 * @param c operations to wait for
 * @returns 0
 */
void rados_striper_multi_aio_wait_for_safe_and_cb(rados_striper_multi_completion_t c);

/**
 * Has a multi asynchronous operation and callback completed
 *
 * @param c async operations to inspect
 * @returns whether c is complete
 */
int rados_striper_multi_aio_is_complete_and_cb(rados_striper_multi_completion_t c);

/**
 * Is a multi asynchronous operation safe and has the callback completed
 *
 * @param c async operations to inspect
 * @returns whether c is safe
 */
int rados_striper_multi_aio_is_safe_and_cb(rados_striper_multi_completion_t c);

/**
 * Get the return value of a multi asychronous operation
 *
 * The return value is set when all operations are complete or safe,
 * whichever comes first.
 *
 * @pre The operation is safe or complete
 *
 * @note BUG: complete callback may never be called when the safe
 * message is received before the complete message
 *
 * @param c async operations to inspect
 * @returns aggregated return value of the operations
 */
int rados_striper_multi_aio_get_return_value(rados_striper_multi_completion_t c);

/**
 * Release a multi asynchrnous IO completion
 *
 * Call this when you no longer need the completion. It may not be
 * freed immediately if the operation is not acked and committed.
 *
 * @param c multi completion to release
 */
void rados_striper_multi_aio_release(rados_striper_multi_completion_t c);

/**
 * Asynchronously write data to a striped object at the specified offset
 *
 * The return value of the completion will be 0 on success, negative
 * error code on failure.
 *
 * @param striper the striper in which the write will occur
 * @param soid the name of the striped object
 * @param completion what to do when the write is safe and complete
 * @param buf data to write
 * @param len length of the data, in bytes
 * @param off byte offset in the object to begin writing at
 * @returns 0 on success, negative error code on
 * failure
 */
int rados_striper_aio_write(rados_striper_t striper,
                            const char *soid,
                            rados_completion_t completion,
                            const char *buf,
                            size_t len,
                            uint64_t off);

/**
 * Asynchronously appends data to a striped object
 *
 * The return value of the completion will be 0 on success, negative
 * error code on failure.
 *
 * @param striper the striper in which the write will occur
 * @param soid the name of the striped object
 * @param completion what to do when the write is safe and complete
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, negative error code on
 * failure
 */
int rados_striper_aio_append(rados_striper_t striper,
                             const char *soid,
                             rados_completion_t completion,
                             const char *buf,
                             size_t len);

/**
 * Asynchronously fills and object with the provided data.
 * If the object exists, it is truncated and then written.
 *
 * The return value of the completion will be 0 on success, negative
 * error code on failure.
 *
 * @param striper the striper in which the write will occur
 * @param soid the name of the striped object
 * @param completion what to do when the write is safe and complete
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, negative error code on
 * failure
 */
int rados_striper_aio_write_full(rados_striper_t striper,
                                 const char *soid,
                                 rados_completion_t completion,
                                 const char *buf,
                                 size_t len);

/**
 * Asynchronously read data from a striped object at the specified offset
 *
 * The return value of the completion will be number of bytes read on
 * success, negative error code on failure.
 *
 * @param striper the striper in which the read will occur
 * @param soid the name of the striped object
 * @param completion what to do when the read is safe and complete
 * @param buf where to store the results
 * @param len the number of bytes to read
 * @param off the offset to start reading from in the object
 * @returns 0 on success, negative error code on
 * failure
 */
int rados_striper_aio_read(rados_striper_t striper,
                           const char *soid,
                           rados_completion_t completion,
                           char *buf,
                           const size_t len,
                           uint64_t off);

/**
 * Block until all pending writes in a striper are safe
 *
 * This is not equivalent to calling rados_striper_multi_aio_wait_for_safe() on all
 * write completions, since this waits for the associated callbacks to
 * complete as well.
 *
 * @param striper the striper in which the flush will occur
 * @returns 0 on success, negative error code on failure
*/
void rados_striper_aio_flush(rados_striper_t striper);

/** @} Asynchronous I/O */

#ifdef __cplusplus
}
#endif

#endif
