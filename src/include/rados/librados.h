// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOS_H
#define CEPH_LIBRADOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#if defined(__linux__)
#include <linux/types.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#endif
#include <string.h>
#include "rados_types.h"

#include <sys/time.h>

#ifndef CEPH_OSD_TMAP_SET
/* These are also defined in rados.h and objclass.h. Keep them in sync! */
#define CEPH_OSD_TMAP_HDR 'h'
#define CEPH_OSD_TMAP_SET 's'
#define CEPH_OSD_TMAP_CREATE 'c'
#define CEPH_OSD_TMAP_RM  'r'
#endif

#define LIBRADOS_VER_MAJOR 0
#define LIBRADOS_VER_MINOR 69
#define LIBRADOS_VER_EXTRA 0

#define LIBRADOS_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRADOS_VERSION_CODE LIBRADOS_VERSION(LIBRADOS_VER_MAJOR, LIBRADOS_VER_MINOR, LIBRADOS_VER_EXTRA)

#define LIBRADOS_SUPPORTS_WATCH 1

/* RADOS lock flags
 * They are also defined in cls_lock_types.h. Keep them in sync!
 */
#define LIBRADOS_LOCK_FLAG_RENEW 0x1

/*
 * Constants for rados_write_op_create().
 */
#define LIBRADOS_CREATE_EXCLUSIVE 1
#define LIBRADOS_CREATE_IDEMPOTENT 0

/*
 * Flags that can be set on a per-op basis via
 * rados_read_op_set_flags() and rados_write_op_set_flags().
 */
// fail a create operation if the object already exists
#define LIBRADOS_OP_FLAG_EXCL 1
// allow the transaction to succeed even if the flagged op fails
#define LIBRADOS_OP_FLAG_FAILOK 2

/**
 * @defgroup librados_h_xattr_comp xattr comparison operations
 * Operators for comparing xattrs on objects, and aborting the
 * rados_read_op or rados_write_op transaction if the comparison
 * fails.
 *
 * @{
 */
/** @cond TODO_enums_not_yet_in_asphyxiate */
enum {
	LIBRADOS_CMPXATTR_OP_EQ  = 1,
	LIBRADOS_CMPXATTR_OP_NE  = 2,
	LIBRADOS_CMPXATTR_OP_GT  = 3,
	LIBRADOS_CMPXATTR_OP_GTE = 4,
	LIBRADOS_CMPXATTR_OP_LT  = 5,
	LIBRADOS_CMPXATTR_OP_LTE = 6
};
/** @endcond */
/** @} */

/**
 * @defgroup librados_h_operation_flags
 * Flags for rados_read_op_opeprate(), rados_write_op_operate(),
 * rados_aio_read_op_operate(), and rados_aio_write_op_operate().
 * See librados.hpp for details.
 * @{
 */
/** @cond TODO_enums_not_yet_in_asphyxiate */
enum {
  LIBRADOS_OPERATION_NOFLAG             = 0,
  LIBRADOS_OPERATION_BALANCE_READS      = 1,
  LIBRADOS_OPERATION_LOCALIZE_READS     = 2,
  LIBRADOS_OPERATION_ORDER_READS_WRITES = 4,
  LIBRADOS_OPERATION_IGNORE_CACHE       = 8,
  LIBRADOS_OPERATION_SKIPRWLOCKS        = 16,
  LIBRADOS_OPERATION_IGNORE_OVERLAY     = 32,
};
/** @endcond */
/** @} */

/*
 * snap id contants
 */
#define LIBRADOS_SNAP_HEAD  ((uint64_t)(-2))
#define LIBRADOS_SNAP_DIR   ((uint64_t)(-1))

/**
 * @typedef rados_t
 *
 * A handle for interacting with a RADOS cluster. It encapsulates all
 * RADOS client configuration, including username, key for
 * authentication, logging, and debugging. Talking different clusters
 * -- or to the same cluster with different users -- requires
 * different cluster handles.
 */
typedef void *rados_t;

/**
 * @tyepdef rados_config_t
 *
 * A handle for the ceph configuration context for the rados_t cluster
 * instance.  This can be used to share configuration context/state
 * (e.g., logging configuration) between librados instance.
 *
 * @warning The config context does not have independent reference
 * counting.  As such, a rados_config_t handle retrieved from a given
 * rados_t is only valid as long as that rados_t.
 */
typedef void *rados_config_t;

/**
 * @typedef rados_ioctx_t
 *
 * An io context encapsulates a few settings for all I/O operations
 * done on it:
 * - pool - set when the io context is created (see rados_ioctx_create())
 * - snapshot context for writes (see
 *   rados_ioctx_selfmanaged_snap_set_write_ctx())
 * - snapshot id to read from (see rados_ioctx_snap_set_read())
 * - object locator for all single-object operations (see
 *   rados_ioctx_locator_set_key())
 * - namespace for all single-object operations (see
 *   rados_ioctx_set_namespace()).  Set to LIBRADOS_ALL_NSPACES
 *   before rados_nobjects_list_open() will list all objects in all
 *   namespaces.
 *
 * @warning Changing any of these settings is not thread-safe -
 * librados users must synchronize any of these changes on their own,
 * or use separate io contexts for each thread
 */
typedef void *rados_ioctx_t;

/**
 * @typedef rados_list_ctx_t
 *
 * An iterator for listing the objects in a pool.
 * Used with rados_nobjects_list_open(),
 * rados_nobjects_list_next(), and
 * rados_nobjects_list_close().
 */
typedef void *rados_list_ctx_t;

/**
 * @typedef rados_snap_t
 * The id of a snapshot.
 */
typedef uint64_t rados_snap_t;

/**
 * @typedef rados_xattrs_iter_t
 * An iterator for listing extended attrbutes on an object.
 * Used with rados_getxattrs(), rados_getxattrs_next(), and
 * rados_getxattrs_end().
 */
typedef void *rados_xattrs_iter_t;

/**
 * @typedef rados_omap_iter_t
 * An iterator for listing omap key/value pairs on an object.
 * Used with rados_read_op_omap_get_keys(), rados_read_op_omap_get_vals(),
 * rados_read_op_omap_get_vals_by_keys(), rados_omap_get_next(), and
 * rados_omap_get_end().
 */
typedef void *rados_omap_iter_t;

/**
 * @struct rados_pool_stat_t
 * Usage information for a pool.
 */
struct rados_pool_stat_t {
  /// space used in bytes
  uint64_t num_bytes;
  /// space used in KB
  uint64_t num_kb;
  /// number of objects in the pool
  uint64_t num_objects;
  /// number of clones of objects
  uint64_t num_object_clones;
  /// num_objects * num_replicas
  uint64_t num_object_copies;
  uint64_t num_objects_missing_on_primary;
  /// number of objects found on no OSDs
  uint64_t num_objects_unfound;
  /// number of objects replicated fewer times than they should be
  /// (but found on at least one OSD)
  uint64_t num_objects_degraded;
  uint64_t num_rd;
  uint64_t num_rd_kb;
  uint64_t num_wr;
  uint64_t num_wr_kb;
};

/**
 * @struct rados_cluster_stat_t
 * Cluster-wide usage information
 */
struct rados_cluster_stat_t {
  uint64_t kb, kb_used, kb_avail;
  uint64_t num_objects;
};

/**
 * @typedef rados_write_op_t
 *
 * An object write operation stores a number of operations which can be
 * executed atomically. For usage, see:
 * - Creation and deletion: rados_create_write_op() rados_release_write_op()
 * - Extended attribute manipulation: rados_write_op_cmpxattr()
 *   rados_write_op_cmpxattr(), rados_write_op_setxattr(),
 *   rados_write_op_rmxattr()
 * - Object map key/value pairs: rados_write_op_omap_set(),
 *   rados_write_op_omap_rm_keys(), rados_write_op_omap_clear(),
 *   rados_write_op_omap_cmp()
 * - Creating objects: rados_write_op_create()
 * - IO on objects: rados_write_op_append(), rados_write_op_write(), rados_write_op_zero
 *   rados_write_op_write_full(), rados_write_op_remove, rados_write_op_truncate(),
 *   rados_write_op_zero()
 * - Hints: rados_write_op_set_alloc_hint()
 * - Performing the operation: rados_write_op_operate(), rados_aio_write_op_operate()
 */
typedef void *rados_write_op_t;

/**
 * @typedef rados_read_op_t
 *
 * An object read operation stores a number of operations which can be
 * executed atomically. For usage, see:
 * - Creation and deletion: rados_create_read_op() rados_release_read_op()
 * - Extended attribute manipulation: rados_read_op_cmpxattr(),
 *   rados_read_op_getxattr(), rados_read_op_getxattrs()
 * - Object map key/value pairs: rados_read_op_omap_get_vals(),
 *   rados_read_op_omap_get_keys(), rados_read_op_omap_get_vals_by_keys(),
 *   rados_read_op_omap_cmp()
 * - Object properties: rados_read_op_stat(), rados_read_op_assert_exists()
 * - IO on objects: rados_read_op_read()
 * - Custom operations: rados_read_op_exec(), rados_read_op_exec_user_buf()
 * - Request properties: rados_read_op_set_flags()
 * - Performing the operation: rados_read_op_operate(),
 *   rados_aio_read_op_operate()
 */
typedef void *rados_read_op_t;

/**
 * Get the version of librados.
 *
 * The version number is major.minor.extra. Note that this is
 * unrelated to the Ceph version number.
 *
 * TODO: define version semantics, i.e.:
 * - incrementing major is for backwards-incompatible changes
 * - incrementing minor is for backwards-compatible changes
 * - incrementing extra is for bug fixes
 *
 * @param major where to store the major version number
 * @param minor where to store the minor version number
 * @param extra where to store the extra version number
 */
void rados_version(int *major, int *minor, int *extra);

/**
 * @defgroup librados_h_init Setup and Teardown
 * These are the first and last functions to that should be called
 * when using librados.
 *
 * @{
 */

/**
 * Create a handle for communicating with a RADOS cluster.
 *
 * Ceph environment variables are read when this is called, so if
 * $CEPH_ARGS specifies everything you need to connect, no further
 * configuration is necessary.
 *
 * @param cluster where to store the handle
 * @param id the user to connect as (i.e. admin, not client.admin)
 * @returns 0 on success, negative error code on failure
 */
int rados_create(rados_t *cluster, const char * const id);

/**
 * Extended version of rados_create.
 *
 * Like rados_create, but 
 * 1) don't assume 'client\.'+id; allow full specification of name
 * 2) allow specification of cluster name
 * 3) flags for future expansion
 */
int rados_create2(rados_t *pcluster, const char *const clustername,
	          const char * const name, uint64_t flags);

/**
 * Initialize a cluster handle from an existing configuration.
 *
 * Share configuration state with another rados_t instance.
 *
 * @param cluster where to store the handle
 * @param cct_ the existing configuration to use
 * @returns 0 on success, negative error code on failure
 */
int rados_create_with_context(rados_t *cluster, rados_config_t cct);

/**
 * Ping the monitor with ID mon_id, storing the resulting reply in
 * buf (if specified) with a maximum size of len.
 *
 * The result buffer is allocated on the heap; the caller is
 * expected to release that memory with rados_buffer_free().  The
 * buffer and length pointers can be NULL, in which case they are
 * not filled in.
 *
 * @param      cluster    cluster handle
 * @param[in]  mon_id     ID of the monitor to ping
 * @param[out] outstr     double pointer with the resulting reply
 * @param[out] outstrlen  pointer with the size of the reply in outstr
 */
int rados_ping_monitor(rados_t cluster, const char *mon_id,
                       char **outstr, size_t *outstrlen);

/**
 * Connect to the cluster.
 *
 * @note BUG: Before calling this, calling a function that communicates with the
 * cluster will crash.
 *
 * @pre The cluster handle is configured with at least a monitor
 * address. If cephx is enabled, a client name and secret must also be
 * set.
 *
 * @post If this succeeds, any function in librados may be used
 *
 * @param cluster The cluster to connect to.
 * @returns 0 on sucess, negative error code on failure
 */
int rados_connect(rados_t cluster);

/**
 * Disconnects from the cluster.
 *
 * For clean up, this is only necessary after rados_connect() has
 * succeeded.
 *
 * @warning This does not guarantee any asynchronous writes have
 * completed. To do that, you must call rados_aio_flush() on all open
 * io contexts.
 *
 * @post the cluster handle cannot be used again
 *
 * @param cluster the cluster to shutdown
 */
void rados_shutdown(rados_t cluster);

/** @} init */

/**
 * @defgroup librados_h_config Configuration
 * These functions read and update Ceph configuration for a cluster
 * handle. Any configuration changes must be done before connecting to
 * the cluster.
 *
 * Options that librados users might want to set include:
 * - mon_host
 * - auth_supported
 * - key, keyfile, or keyring when using cephx
 * - log_file, log_to_stderr, err_to_stderr, and log_to_syslog
 * - debug_rados, debug_objecter, debug_monc, debug_auth, or debug_ms
 *
 * All possible options can be found in src/common/config_opts.h in ceph.git
 *
 * @{
 */

/**
 * Configure the cluster handle using a Ceph config file
 *
 * If path is NULL, the default locations are searched, and the first
 * found is used. The locations are:
 * - $CEPH_CONF (environment variable)
 * - /etc/ceph/ceph.conf
 * - ~/.ceph/config
 * - ceph.conf (in the current working directory)
 *
 * @pre rados_connect() has not been called on the cluster handle
 *
 * @param cluster cluster handle to configure
 * @param path path to a Ceph configuration file
 * @returns 0 on success, negative error code on failure
 */
int rados_conf_read_file(rados_t cluster, const char *path);

/**
 * Configure the cluster handle with command line arguments
 *
 * argv can contain any common Ceph command line option, including any
 * configuration parameter prefixed by '--' and replacing spaces with
 * dashes or underscores. For example, the following options are equivalent:
 * - --mon-host 10.0.0.1:6789
 * - --mon_host 10.0.0.1:6789
 * - -m 10.0.0.1:6789
 *
 * @pre rados_connect() has not been called on the cluster handle
 *
 * @param cluster cluster handle to configure
 * @param argc number of arguments in argv
 * @param argv arguments to parse
 * @returns 0 on success, negative error code on failure
 */
int rados_conf_parse_argv(rados_t cluster, int argc, const char **argv);


/**
 * Configure the cluster handle with command line arguments, returning
 * any remainders.  Same rados_conf_parse_argv, except for extra
 * remargv argument to hold returns unrecognized arguments.
 *
 * @pre rados_connect() has not been called on the cluster handle
 *
 * @param cluster cluster handle to configure
 * @param argc number of arguments in argv
 * @param argv arguments to parse
 * @param remargv char* array for returned unrecognized arguments
 * @returns 0 on success, negative error code on failure
 */
int rados_conf_parse_argv_remainder(rados_t cluster, int argc,
				    const char **argv, const char **remargv);
/**
 * Configure the cluster handle based on an environment variable
 *
 * The contents of the environment variable are parsed as if they were
 * Ceph command line options. If var is NULL, the CEPH_ARGS
 * environment variable is used.
 *
 * @pre rados_connect() has not been called on the cluster handle
 *
 * @note BUG: this is not threadsafe - it uses a static buffer
 *
 * @param cluster cluster handle to configure
 * @param var name of the environment variable to read
 * @returns 0 on success, negative error code on failure
 */
int rados_conf_parse_env(rados_t cluster, const char *var);

/**
 * Set a configuration option
 *
 * @pre rados_connect() has not been called on the cluster handle
 *
 * @param cluster cluster handle to configure
 * @param option option to set
 * @param value value of the option
 * @returns 0 on success, negative error code on failure
 * @returns -ENOENT when the option is not a Ceph configuration option
 */
int rados_conf_set(rados_t cluster, const char *option, const char *value);

/**
 * Get the value of a configuration option
 *
 * @param cluster configuration to read
 * @param option which option to read
 * @param buf where to write the configuration value
 * @param len the size of buf in bytes
 * @returns 0 on success, negative error code on failure
 * @returns -ENAMETOOLONG if the buffer is too short to contain the
 * requested value
 */
int rados_conf_get(rados_t cluster, const char *option, char *buf, size_t len);

/** @} config */

/**
 * Read usage info about the cluster
 *
 * This tells you total space, space used, space available, and number
 * of objects. These are not updated immediately when data is written,
 * they are eventually consistent.
 *
 * @param cluster cluster to query
 * @param result where to store the results
 * @returns 0 on success, negative error code on failure
 */
int rados_cluster_stat(rados_t cluster, struct rados_cluster_stat_t *result);

/**
 * Get the fsid of the cluster as a hexadecimal string.
 *
 * The fsid is a unique id of an entire Ceph cluster.
 *
 * @param cluster where to get the fsid
 * @param buf where to write the fsid
 * @param len the size of buf in bytes (should be 37)
 * @returns 0 on success, negative error code on failure
 * @returns -ERANGE if the buffer is too short to contain the
 * fsid
 */
int rados_cluster_fsid(rados_t cluster, char *buf, size_t len);

/**
 * Get/wait for the most recent osdmap
 * 
 * @param cluster the cluster to shutdown
 * @returns 0 on sucess, negative error code on failure
 */
int rados_wait_for_latest_osdmap(rados_t cluster);

/**
 * @defgroup librados_h_pools Pools
 *
 * RADOS pools are separate namespaces for objects. Pools may have
 * different crush rules associated with them, so they could have
 * differing replication levels or placement strategies. RADOS
 * permissions are also tied to pools - users can have different read,
 * write, and execute permissions on a per-pool basis.
 *
 * @{
 */

/**
 * List pools
 *
 * Gets a list of pool names as NULL-terminated strings.  The pool
 * names will be placed in the supplied buffer one after another.
 * After the last pool name, there will be two 0 bytes in a row.
 *
 * If len is too short to fit all the pool name entries we need, we will fill
 * as much as we can.
 *
 * @param cluster cluster handle
 * @param buf output buffer
 * @param len output buffer length
 * @returns length of the buffer we would need to list all pools
 */
int rados_pool_list(rados_t cluster, char *buf, size_t len);

/**
 * Get a configuration handle for a rados cluster handle
 *
 * This handle is valid only as long as the cluster handle is valid.
 *
 * @param cluster cluster handle
 * @returns config handle for this cluster
 */
rados_config_t rados_cct(rados_t cluster);

/**
 * Get a global id for current instance
 *
 * This id is a unique representation of current connection to the cluster
 *
 * @param cluster cluster handle
 * @returns instance global id
 */
uint64_t rados_get_instance_id(rados_t cluster);

/**
 * Create an io context
 *
 * The io context allows you to perform operations within a particular
 * pool. For more details see rados_ioctx_t.
 *
 * @param cluster which cluster the pool is in
 * @param pool_name name of the pool
 * @param ioctx where to store the io context
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_create(rados_t cluster, const char *pool_name, rados_ioctx_t *ioctx);

/**
 * The opposite of rados_ioctx_create
 *
 * This just tells librados that you no longer need to use the io context.
 * It may not be freed immediately if there are pending asynchronous
 * requests on it, but you should not use an io context again after
 * calling this function on it.
 *
 * @warning This does not guarantee any asynchronous
 * writes have completed. You must call rados_aio_flush()
 * on the io context before destroying it to do that.
 *
 * @param io the io context to dispose of
 */
void rados_ioctx_destroy(rados_ioctx_t io);

/**
 * Get configuration hadnle for a pool handle
 *
 * @param io pool handle
 * @returns rados_config_t for this cluster
 */
rados_config_t rados_ioctx_cct(rados_ioctx_t io);

/**
 * Get the cluster handle used by this rados_ioctx_t
 * Note that this is a weak reference, and should not
 * be destroyed via rados_destroy().
 *
 * @param io the io context
 * @returns the cluster handle for this io context
 */
rados_t rados_ioctx_get_cluster(rados_ioctx_t io);

/**
 * Get pool usage statistics
 *
 * Fills in a rados_pool_stat_t after querying the cluster.
 *
 * @param io determines which pool to query
 * @param stats where to store the results
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_pool_stat(rados_ioctx_t io, struct rados_pool_stat_t *stats);

/**
 * Get the id of a pool
 *
 * @param cluster which cluster the pool is in
 * @param pool_name which pool to look up
 * @returns id of the pool
 * @returns -ENOENT if the pool is not found
 */
int64_t rados_pool_lookup(rados_t cluster, const char *pool_name);

/**
 * Get the name of a pool
 *
 * @param cluster which cluster the pool is in
 * @param id the id of the pool
 * @param buf where to store the pool name
 * @param maxlen size of buffer where name will be stored
 * @returns length of string stored, or -ERANGE if buffer too small
 */
int rados_pool_reverse_lookup(rados_t cluster, int64_t id, char *buf,
			      size_t maxlen);

/**
 * Create a pool with default settings
 *
 * The default owner is the admin user (auid 0).
 * The default crush rule is rule 0.
 *
 * @param cluster the cluster in which the pool will be created
 * @param pool_name the name of the new pool
 * @returns 0 on success, negative error code on failure
 */
int rados_pool_create(rados_t cluster, const char *pool_name);

/**
 * Create a pool owned by a specific auid
 *
 * The auid is the authenticated user id to give ownership of the pool.
 * TODO: document auid and the rest of the auth system
 *
 * @param cluster the cluster in which the pool will be created
 * @param pool_name the name of the new pool
 * @param auid the id of the owner of the new pool
 * @returns 0 on success, negative error code on failure
 */
int rados_pool_create_with_auid(rados_t cluster, const char *pool_name, uint64_t auid);

/**
 * Create a pool with a specific CRUSH rule
 *
 * @param cluster the cluster in which the pool will be created
 * @param pool_name the name of the new pool
 * @param crush_rule_num which rule to use for placement in the new pool1
 * @returns 0 on success, negative error code on failure
 */
int rados_pool_create_with_crush_rule(rados_t cluster, const char *pool_name,
				      uint8_t crush_rule_num);

/**
 * Create a pool with a specific CRUSH rule and auid
 *
 * This is a combination of rados_pool_create_with_crush_rule() and
 * rados_pool_create_with_auid().
 *
 * @param cluster the cluster in which the pool will be created
 * @param pool_name the name of the new pool
 * @param crush_rule_num which rule to use for placement in the new pool2
 * @param auid the id of the owner of the new pool
 * @returns 0 on success, negative error code on failure
 */
int rados_pool_create_with_all(rados_t cluster, const char *pool_name, uint64_t auid,
			       uint8_t crush_rule_num);

/**
 * Returns the pool that is the base tier for this pool.
 *
 * The return value is the ID of the pool that should be used to read from/write to.
 * If tiering is not set up for the pool, returns \c pool.
 *
 * @param cluster the cluster the pool is in
 * @param pool ID of the pool to query
 * @param[out] base_tier base tier, or \c pool if tiering is not configured
 * @returns 0 on success, negative error code on failure
 */
int rados_pool_get_base_tier(rados_t cluster, int64_t pool, int64_t* base_tier);

/**
 * Delete a pool and all data inside it
 *
 * The pool is removed from the cluster immediately,
 * but the actual data is deleted in the background.
 *
 * @param cluster the cluster the pool is in
 * @param pool_name which pool to delete
 * @returns 0 on success, negative error code on failure
 */
int rados_pool_delete(rados_t cluster, const char *pool_name);

/**
 * Attempt to change an io context's associated auid "owner."
 *
 * Requires that you have write permission on both the current and new
 * auid.
 *
 * @param io reference to the pool to change.
 * @param auid the auid you wish the io to have.
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_pool_set_auid(rados_ioctx_t io, uint64_t auid);

/**
 * Get the auid of a pool
 *
 * @param io pool to query
 * @param auid where to store the auid
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_pool_get_auid(rados_ioctx_t io, uint64_t *auid);

int rados_ioctx_pool_requires_alignment(rados_ioctx_t io);
uint64_t rados_ioctx_pool_required_alignment(rados_ioctx_t io);

/**
 * Get the pool id of the io context
 *
 * @param io the io context to query
 * @returns the id of the pool the io context uses
 */
int64_t rados_ioctx_get_id(rados_ioctx_t io);

/**
 * Get the pool name of the io context
 *
 * @param io the io context to query
 * @param buf pointer to buffer where name will be stored
 * @param maxlen size of buffer where name will be stored
 * @returns length of string stored, or -ERANGE if buffer too small
 */
int rados_ioctx_get_pool_name(rados_ioctx_t io, char *buf, unsigned maxlen);

/** @} pools */

/**
 * @defgroup librados_h_obj_loc Object Locators
 *
 * @{
 */

/**
 * Set the key for mapping objects to pgs within an io context.
 *
 * The key is used instead of the object name to determine which
 * placement groups an object is put in. This affects all subsequent
 * operations of the io context - until a different locator key is
 * set, all objects in this io context will be placed in the same pg.
 *
 * This is useful if you need to do clone_range operations, which must
 * be done with the source and destination objects in the same pg.
 *
 * @param io the io context to change
 * @param key the key to use as the object locator, or NULL to discard
 * any previously set key
 */
void rados_ioctx_locator_set_key(rados_ioctx_t io, const char *key);

/**
 * Set the namespace for objects within an io context
 *
 * The namespace specification further refines a pool into different
 * domains.  The mapping of objects to pgs is also based on this
 * value.
 *
 * @param io the io context to change
 * @param nspace the name to use as the namespace, or NULL use the
 * default namespace
 */
void rados_ioctx_set_namespace(rados_ioctx_t io, const char *nspace);
/** @} obj_loc */

/**
 * @defgroup librados_h_list_nobj New Listing Objects
 * @{
 */
/**
 * Start listing objects in a pool
 *
 * @param io the pool to list from
 * @param ctx the handle to store list context in
 * @returns 0 on success, negative error code on failure
 */
int rados_nobjects_list_open(rados_ioctx_t io, rados_list_ctx_t *ctx);

/**
 * Return hash position of iterator, rounded to the current PG
 *
 * @param ctx iterator marking where you are in the listing
 * @returns current hash position, rounded to the current pg
 */
uint32_t rados_nobjects_list_get_pg_hash_position(rados_list_ctx_t ctx);

/**
 * Reposition object iterator to a different hash position
 *
 * @param ctx iterator marking where you are in the listing
 * @param pos hash position to move to
 * @returns actual (rounded) position we moved to
 */
uint32_t rados_nobjects_list_seek(rados_list_ctx_t ctx, uint32_t pos);

/**
 * Get the next object name and locator in the pool
 *
 * *entry and *key are valid until next call to rados_objects_list_*
 *
 * @param ctx iterator marking where you are in the listing
 * @param entry where to store the name of the entry
 * @param key where to store the object locator (set to NULL to ignore)
 * @param nspace where to store the object namespace (set to NULL to ignore)
 * @returns 0 on success, negative error code on failure
 * @returns -ENOENT when there are no more objects to list
 */
int rados_nobjects_list_next(rados_list_ctx_t ctx, const char **entry,
	const char **key, const char **nspace);

/**
 * Close the object listing handle.
 *
 * This should be called when the handle is no longer needed.
 * The handle should not be used after it has been closed.
 *
 * @param ctx the handle to close
 */
void rados_nobjects_list_close(rados_list_ctx_t ctx);

/** @} New Listing Objects */

/**
 * @defgroup librados_h_list_obj Deprecated Listing Objects
 *
 * Older listing objects interface.  Please use the new interface.
 * @{
 */
/**
 * @warning Deprecated: Use rados_nobjects_list_open() instead
 */
int rados_objects_list_open(rados_ioctx_t io, rados_list_ctx_t *ctx);

/**
 * @warning Deprecated: Use rados_nobjects_list_get_pg_hash_position() instead
 */
uint32_t rados_objects_list_get_pg_hash_position(rados_list_ctx_t ctx);

/**
 * @warning Deprecated: Use rados_nobjects_list_seek() instead
 */
uint32_t rados_objects_list_seek(rados_list_ctx_t ctx, uint32_t pos);

/**
 * @warning Deprecated: Use rados_nobjects_list_next() instead
 */
int rados_objects_list_next(rados_list_ctx_t ctx, const char **entry, const char **key);

/**
 * @warning Deprecated: Use rados_nobjects_list_close() instead
 */
void rados_objects_list_close(rados_list_ctx_t ctx);

/** @} Listing Objects */

/**
 * @defgroup librados_h_snaps Snapshots
 *
 * RADOS snapshots are based upon sequence numbers that form a
 * snapshot context. They are pool-specific. The snapshot context
 * consists of the current snapshot sequence number for a pool, and an
 * array of sequence numbers at which snapshots were taken, in
 * descending order. Whenever a snapshot is created or deleted, the
 * snapshot sequence number for the pool is increased. To add a new
 * snapshot, the new snapshot sequence number must be increased and
 * added to the snapshot context.
 *
 * There are two ways to manage these snapshot contexts:
 * -# within the RADOS cluster
 *    These are called pool snapshots, and store the snapshot context
 *    in the OSDMap. These represent a snapshot of all the objects in
 *    a pool.
 * -# within the RADOS clients
 *    These are called self-managed snapshots, and push the
 *    responsibility for keeping track of the snapshot context to the
 *    clients. For every write, the client must send the snapshot
 *    context. In librados, this is accomplished with
 *    rados_selfmanaged_snap_set_write_ctx(). These are more
 *    difficult to manage, but are restricted to specific objects
 *    instead of applying to an entire pool.
 *
 * @{
 */

/**
 * Create a pool-wide snapshot
 *
 * @param io the pool to snapshot
 * @param snapname the name of the snapshot
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_snap_create(rados_ioctx_t io, const char *snapname);

/**
 * Delete a pool snapshot
 *
 * @param io the pool to delete the snapshot from
 * @param snapname which snapshot to delete
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_snap_remove(rados_ioctx_t io, const char *snapname);

/**
 * Rollback an object to a pool snapshot
 *
 * The contents of the object will be the same as
 * when the snapshot was taken.
 *
 * @param io the pool in which the object is stored
 * @param oid the name of the object to rollback
 * @param snapname which snapshot to rollback to
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_snap_rollback(rados_ioctx_t io, const char *oid,
		   const char *snapname);

/**
 * @warning Deprecated: Use rados_ioctx_snap_rollback() instead
 */
int rados_rollback(rados_ioctx_t io, const char *oid,
		   const char *snapname);

/**
 * Set the snapshot from which reads are performed.
 *
 * Subsequent reads will return data as it was at the time of that
 * snapshot.
 *
 * @param io the io context to change
 * @param snap the id of the snapshot to set, or LIBRADOS_SNAP_HEAD for no
 * snapshot (i.e. normal operation)
 */
void rados_ioctx_snap_set_read(rados_ioctx_t io, rados_snap_t snap);

/**
 * Allocate an ID for a self-managed snapshot
 *
 * Get a unique ID to put in the snaphot context to create a
 * snapshot. A clone of an object is not created until a write with
 * the new snapshot context is completed.
 *
 * @param io the pool in which the snapshot will exist
 * @param snapid where to store the newly allocated snapshot ID
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io, rados_snap_t *snapid);

/**
 * Remove a self-managed snapshot
 *
 * This increases the snapshot sequence number, which will cause
 * snapshots to be removed lazily.
 *
 * @param io the pool in which the snapshot will exist
 * @param snapid where to store the newly allocated snapshot ID
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io, rados_snap_t snapid);

/**
 * Rollback an object to a self-managed snapshot
 *
 * The contents of the object will be the same as
 * when the snapshot was taken.
 *
 * @param io the pool in which the object is stored
 * @param oid the name of the object to rollback
 * @param snapid which snapshot to rollback to
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_selfmanaged_snap_rollback(rados_ioctx_t io, const char *oid, rados_snap_t snapid);

/**
 * Set the snapshot context for use when writing to objects
 *
 * This is stored in the io context, and applies to all future writes.
 *
 * @param io the io context to change
 * @param seq the newest snapshot sequence number for the pool
 * @param snaps array of snapshots in sorted by descending id
 * @param num_snaps how many snaphosts are in the snaps array
 * @returns 0 on success, negative error code on failure
 * @returns -EINVAL if snaps are not in descending order
 */
int rados_ioctx_selfmanaged_snap_set_write_ctx(rados_ioctx_t io, rados_snap_t seq, rados_snap_t *snaps, int num_snaps);

/**
 * List all the ids of pool snapshots
 *
 * If the output array does not have enough space to fit all the
 * snapshots, -ERANGE is returned and the caller should retry with a
 * larger array.
 *
 * @param io the pool to read from
 * @param snaps where to store the results
 * @param maxlen the number of rados_snap_t that fit in the snaps array
 * @returns number of snapshots on success, negative error code on failure
 * @returns -ERANGE is returned if the snaps array is too short
 */
int rados_ioctx_snap_list(rados_ioctx_t io, rados_snap_t *snaps, int maxlen);

/**
 * Get the id of a pool snapshot
 *
 * @param io the pool to read from
 * @param name the snapshot to find
 * @param id where to store the result
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_snap_lookup(rados_ioctx_t io, const char *name, rados_snap_t *id);

/**
 * Get the name of a pool snapshot
 *
 * @param io the pool to read from
 * @param id the snapshot to find
 * @param name where to store the result
 * @param maxlen the size of the name array
 * @returns 0 on success, negative error code on failure
 * @returns -ERANGE if the name array is too small
 */
int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id, char *name, int maxlen);

/**
 * Find when a pool snapshot occurred
 *
 * @param io the pool the snapshot was taken in
 * @param id the snapshot to lookup
 * @param t where to store the result
 * @returns 0 on success, negative error code on failure
 */
int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id, time_t *t);

/** @} Snapshots */

/**
 * @defgroup librados_h_synch_io Synchronous I/O
 * Writes are replicated to a number of OSDs based on the
 * configuration of the pool they are in. These write functions block
 * until data is in memory on all replicas of the object they're
 * writing to - they are equivalent to doing the corresponding
 * asynchronous write, and the calling
 * rados_ioctx_wait_for_complete().  For greater data safety, use the
 * asynchronous functions and rados_aio_wait_for_safe().
 *
 * @{
 */

/**
 * Return the version of the last object read or written to.
 *
 * This exposes the internal version number of the last object read or
 * written via this io context
 *
 * @param io the io context to check
 * @returns last read or written object version
 */
uint64_t rados_get_last_version(rados_ioctx_t io);

/**
 * Write *len* bytes from *buf* into the *oid* object, starting at
 * offset *off*. The value of *len* must be <= UINT_MAX/2.
 *
 * @note This will never return a positive value not equal to len.
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @param off byte offset in the object to begin writing at
 * @returns 0 on success, negative error code on failure
 */
int rados_write(rados_ioctx_t io, const char *oid, const char *buf, size_t len, uint64_t off);

/**
 * Write *len* bytes from *buf* into the *oid* object. The value of
 * *len* must be <= UINT_MAX/2.
 *
 * The object is filled with the provided data. If the object exists,
 * it is atomically truncated and then written.
 *
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_write_full(rados_ioctx_t io, const char *oid, const char *buf, size_t len);

/**
 * Efficiently copy a portion of one object to another
 *
 * If the underlying filesystem on the OSD supports it, this will be a
 * copy-on-write clone.
 *
 * The src and dest objects must be in the same pg. To ensure this,
 * the io context should have a locator key set (see
 * rados_ioctx_locator_set_key()).
 *
 * @param io the context in which the data is cloned
 * @param dst the name of the destination object
 * @param dst_off the offset within the destination object (in bytes)
 * @param src the name of the source object
 * @param src_off the offset within the source object (in bytes)
 * @param len how much data to copy
 * @returns 0 on success, negative error code on failure
 */
int rados_clone_range(rados_ioctx_t io, const char *dst, uint64_t dst_off,
                      const char *src, uint64_t src_off, size_t len);

/**
 * Append *len* bytes from *buf* into the *oid* object. The value of
 * *len* must be <= UINT_MAX/2.
 *
 * @param io the context to operate in
 * @param oid the name of the object
 * @param buf the data to append
 * @param len length of buf (in bytes)
 * @returns 0 on success, negative error code on failure
 */
int rados_append(rados_ioctx_t io, const char *oid, const char *buf, size_t len);

/**
 * Read data from an object
 *
 * The io context determines the snapshot to read from, if any was set
 * by rados_ioctx_snap_set_read().
 *
 * @param io the context in which to perform the read
 * @param oid the name of the object to read from
 * @param buf where to store the results
 * @param len the number of bytes to read
 * @param off the offset to start reading from in the object
 * @returns number of bytes read on success, negative error code on
 * failure
 */
int rados_read(rados_ioctx_t io, const char *oid, char *buf, size_t len, uint64_t off);

/**
 * Delete an object
 *
 * @note This does not delete any snapshots of the object.
 *
 * @param io the pool to delete the object from
 * @param oid the name of the object to delete
 * @returns 0 on success, negative error code on failure
 */
int rados_remove(rados_ioctx_t io, const char *oid);

/**
 * Resize an object
 *
 * If this enlarges the object, the new area is logically filled with
 * zeroes. If this shrinks the object, the excess data is removed.
 *
 * @param io the context in which to truncate
 * @param oid the name of the object
 * @param size the new size of the object in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_trunc(rados_ioctx_t io, const char *oid, uint64_t size);

/**
 * @defgroup librados_h_xattrs Xattrs
 * Extended attributes are stored as extended attributes on the files
 * representing an object on the OSDs. Thus, they have the same
 * limitations as the underlying filesystem. On ext4, this means that
 * the total data stored in xattrs cannot exceed 4KB.
 *
 * @{
 */

/**
 * Get the value of an extended attribute on an object.
 *
 * @param io the context in which the attribute is read
 * @param o name of the object
 * @param name which extended attribute to read
 * @param buf where to store the result
 * @param len size of buf in bytes
 * @returns length of xattr value on success, negative error code on failure
 */
int rados_getxattr(rados_ioctx_t io, const char *o, const char *name, char *buf, size_t len);

/**
 * Set an extended attribute on an object.
 *
 * @param io the context in which xattr is set
 * @param o name of the object
 * @param name which extended attribute to set
 * @param buf what to store in the xattr
 * @param len the number of bytes in buf
 * @returns 0 on success, negative error code on failure
 */
int rados_setxattr(rados_ioctx_t io, const char *o, const char *name, const char *buf, size_t len);

/**
 * Delete an extended attribute from an object.
 *
 * @param io the context in which to delete the xattr
 * @param o the name of the object
 * @param name which xattr to delete
 * @returns 0 on success, negative error code on failure
 */
int rados_rmxattr(rados_ioctx_t io, const char *o, const char *name);

/**
 * Start iterating over xattrs on an object.
 *
 * @post iter is a valid iterator
 *
 * @param io the context in which to list xattrs
 * @param oid name of the object
 * @param iter where to store the iterator
 * @returns 0 on success, negative error code on failure
 */
int rados_getxattrs(rados_ioctx_t io, const char *oid, rados_xattrs_iter_t *iter);

/**
 * Get the next xattr on the object
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
int rados_getxattrs_next(rados_xattrs_iter_t iter, const char **name,
			 const char **val, size_t *len);

/**
 * Close the xattr iterator.
 *
 * iter should not be used after this is called.
 *
 * @param iter the iterator to close
 */
void rados_getxattrs_end(rados_xattrs_iter_t iter);

/** @} Xattrs */

/**
 * Get the next omap key/value pair on the object
 *
 * @pre iter is a valid iterator
 *
 * @post key and val are the next key/value pair. key is
 * null-terminated, and val has length len. If the end of the list has
 * been reached, key and val are NULL, and len is 0. key and val will
 * not be accessible after rados_omap_get_end() is called on iter, so
 * if they are needed after that they should be copied.
 *
 * @param iter iterator to advance
 * @param key where to store the key of the next omap entry
 * @param val where to store the value of the next omap entry
 * @param len where to store the number of bytes in val
 * @returns 0 on success, negative error code on failure
 */
int rados_omap_get_next(rados_omap_iter_t iter,
			char **key,
			char **val,
			size_t *len);

/**
 * Close the omap iterator.
 *
 * iter should not be used after this is called.
 *
 * @param iter the iterator to close
 */
void rados_omap_get_end(rados_omap_iter_t iter);

/**
 * Get object stats (size/mtime)
 *
 * TODO: when are these set, and by whom? can they be out of date?
 *
 * @param io ioctx
 * @param o object name
 * @param psize where to store object size
 * @param pmtime where to store modification time
 * @returns 0 on success, negative error code on failure
 */
int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime);

/**
 * Update tmap (trivial map)
 *
 * Do compound update to a tmap object, inserting or deleting some
 * number of records.  cmdbuf is a series of operation byte
 * codes, following by command payload.  Each command is a single-byte
 * command code, whose value is one of CEPH_OSD_TMAP_*.
 *
 *  - update tmap 'header'
 *    - 1 byte  = CEPH_OSD_TMAP_HDR
 *    - 4 bytes = data length (little endian)
 *    - N bytes = data
 *
 *  - insert/update one key/value pair
 *    - 1 byte  = CEPH_OSD_TMAP_SET
 *    - 4 bytes = key name length (little endian)
 *    - N bytes = key name
 *    - 4 bytes = data length (little endian)
 *    - M bytes = data
 *
 *  - insert one key/value pair; return -EEXIST if it already exists.
 *    - 1 byte  = CEPH_OSD_TMAP_CREATE
 *    - 4 bytes = key name length (little endian)
 *    - N bytes = key name
 *    - 4 bytes = data length (little endian)
 *    - M bytes = data
 *
 *  - remove one key/value pair
 *    - 1 byte  = CEPH_OSD_TMAP_RM
 *    - 4 bytes = key name length (little endian)
 *    - N bytes = key name
 *
 * Restrictions:
 *  - The HDR update must preceed any key/value updates.
 *  - All key/value updates must be in lexicographically sorted order
 *    in cmdbuf.
 *  - You can read/write to a tmap object via the regular APIs, but
 *    you should be careful not to corrupt it.  Also be aware that the
 *    object format may change without notice.
 *
 * @param io ioctx
 * @param o object name
 * @param cmdbuf command buffer
 * @param cmdbuflen command buffer length in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_tmap_update(rados_ioctx_t io, const char *o, const char *cmdbuf, size_t cmdbuflen);

/**
 * Store complete tmap (trivial map) object
 *
 * Put a full tmap object into the store, replacing what was there.
 *
 * The format of buf is:
 * - 4 bytes - length of header (little endian)
 * - N bytes - header data
 * - 4 bytes - number of keys (little endian)
 *
 * and for each key,
 * - 4 bytes - key name length (little endian)
 * - N bytes - key name
 * - 4 bytes - value length (little endian)
 * - M bytes - value data
 *
 * @param io ioctx
 * @param o object name
 * @param buf buffer
 * @param buflen buffer length in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_tmap_put(rados_ioctx_t io, const char *o, const char *buf, size_t buflen);

/**
 * Fetch complete tmap (trivial map) object
 *
 * Read a full tmap object.  See rados_tmap_put() for the format the
 * data is returned in.
 *
 * @param io ioctx
 * @param o object name
 * @param buf buffer
 * @param buflen buffer length in bytes
 * @returns 0 on success, negative error code on failure
 * @returns -ERANGE if buf isn't big enough
 */
int rados_tmap_get(rados_ioctx_t io, const char *o, char *buf, size_t buflen);

/**
 * Execute an OSD class method on an object
 *
 * The OSD has a plugin mechanism for performing complicated
 * operations on an object atomically. These plugins are called
 * classes. This function allows librados users to call the custom
 * methods. The input and output formats are defined by the class.
 * Classes in ceph.git can be found in src/cls subdirectories
 *
 * @param io the context in which to call the method
 * @param oid the object to call the method on
 * @param cls the name of the class
 * @param method the name of the method
 * @param in_buf where to find input
 * @param in_len length of in_buf in bytes
 * @param buf where to store output
 * @param out_len length of buf in bytes
 * @returns the length of the output, or
 * -ERANGE if out_buf does not have enough space to store it (For methods that return data). For
 * methods that don't return data, the return value is
 * method-specific.
 */
int rados_exec(rados_ioctx_t io, const char *oid, const char *cls, const char *method,
	       const char *in_buf, size_t in_len, char *buf, size_t out_len);


/** @} Synchronous I/O */

/**
 * @defgroup librados_h_asynch_io Asynchronous I/O
 * Read and write to objects without blocking.
 *
 * @{
 */

/**
 * @typedef rados_completion_t
 * Represents the state of an asynchronous operation - it contains the
 * return value once the operation completes, and can be used to block
 * until the operation is complete or safe.
 */
typedef void *rados_completion_t;

/**
 * @typedef rados_callback_t
 * Callbacks for asynchrous operations take two parameters:
 * - cb the completion that has finished
 * - arg application defined data made available to the callback function
 */
typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);

/**
 * Constructs a completion to use with asynchronous operations
 *
 * The complete and safe callbacks correspond to operations being
 * acked and committed, respectively. The callbacks are called in
 * order of receipt, so the safe callback may be triggered before the
 * complete callback, and vice versa. This is affected by journalling
 * on the OSDs.
 *
 * TODO: more complete documentation of this elsewhere (in the RADOS docs?)
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
int rados_aio_create_completion(void *cb_arg, rados_callback_t cb_complete, rados_callback_t cb_safe,
				rados_completion_t *pc);

/**
 * Block until an operation completes
 *
 * This means it is in memory on all replicas.
 *
 * @note BUG: this should be void
 *
 * @param c operation to wait for
 * @returns 0
 */
int rados_aio_wait_for_complete(rados_completion_t c);

/**
 * Block until an operation is safe
 *
 * This means it is on stable storage on all replicas.
 *
 * @note BUG: this should be void
 *
 * @param c operation to wait for
 * @returns 0
 */
int rados_aio_wait_for_safe(rados_completion_t c);

/**
 * Has an asynchronous operation completed?
 *
 * @warning This does not imply that the complete callback has
 * finished
 *
 * @param c async operation to inspect
 * @returns whether c is complete
 */
int rados_aio_is_complete(rados_completion_t c);

/**
 * Is an asynchronous operation safe?
 *
 * @warning This does not imply that the safe callback has
 * finished
 *
 * @param c async operation to inspect
 * @returns whether c is safe
 */
int rados_aio_is_safe(rados_completion_t c);

/**
 * Block until an operation completes and callback completes
 *
 * This means it is in memory on all replicas and can be read.
 *
 * @note BUG: this should be void
 *
 * @param c operation to wait for
 * @returns 0
 */
int rados_aio_wait_for_complete_and_cb(rados_completion_t c);

/**
 * Block until an operation is safe and callback has completed
 *
 * This means it is on stable storage on all replicas.
 *
 * @note BUG: this should be void
 *
 * @param c operation to wait for
 * @returns 0
 */
int rados_aio_wait_for_safe_and_cb(rados_completion_t c);

/**
 * Has an asynchronous operation and callback completed
 *
 * @param c async operation to inspect
 * @returns whether c is complete
 */
int rados_aio_is_complete_and_cb(rados_completion_t c);

/**
 * Is an asynchronous operation safe and has the callback completed
 *
 * @param c async operation to inspect
 * @returns whether c is safe
 */
int rados_aio_is_safe_and_cb(rados_completion_t c);

/**
 * Get the return value of an asychronous operation
 *
 * The return value is set when the operation is complete or safe,
 * whichever comes first.
 *
 * @pre The operation is safe or complete
 *
 * @note BUG: complete callback may never be called when the safe
 * message is received before the complete message
 *
 * @param c async operation to inspect
 * @returns return value of the operation
 */
int rados_aio_get_return_value(rados_completion_t c);

/**
 * Release a completion
 *
 * Call this when you no longer need the completion. It may not be
 * freed immediately if the operation is not acked and committed.
 *
 * @param c completion to release
 */
void rados_aio_release(rados_completion_t c);

/**
 * Write data to an object asynchronously
 *
 * Queues the write and returns. The return value of the completion
 * will be 0 on success, negative error code on failure.
 *
 * @param io the context in which the write will occur
 * @param oid name of the object
 * @param completion what to do when the write is safe and complete
 * @param buf data to write
 * @param len length of the data, in bytes
 * @param off byte offset in the object to begin writing at
 * @returns 0 on success, -EROFS if the io context specifies a snap_seq
 * other than LIBRADOS_SNAP_HEAD
 */
int rados_aio_write(rados_ioctx_t io, const char *oid,
		    rados_completion_t completion,
		    const char *buf, size_t len, uint64_t off);

/**
 * Asychronously append data to an object
 *
 * Queues the append and returns.
 *
 * The return value of the completion will be 0 on success, negative
 * error code on failure.
 *
 * @param io the context to operate in
 * @param oid the name of the object
 * @param completion what to do when the append is safe and complete
 * @param buf the data to append
 * @param len length of buf (in bytes)
 * @returns 0 on success, -EROFS if the io context specifies a snap_seq
 * other than LIBRADOS_SNAP_HEAD
 */
int rados_aio_append(rados_ioctx_t io, const char *oid,
		     rados_completion_t completion,
		     const char *buf, size_t len);

/**
 * Asychronously write an entire object
 *
 * The object is filled with the provided data. If the object exists,
 * it is atomically truncated and then written.
 * Queues the write_full and returns.
 *
 * The return value of the completion will be 0 on success, negative
 * error code on failure.
 *
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param completion what to do when the write_full is safe and complete
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, -EROFS if the io context specifies a snap_seq
 * other than LIBRADOS_SNAP_HEAD
 */
int rados_aio_write_full(rados_ioctx_t io, const char *oid,
			 rados_completion_t completion,
			 const char *buf, size_t len);

/**
 * Asychronously remove an object
 *
 * Queues the remove and returns.
 *
 * The return value of the completion will be 0 on success, negative
 * error code on failure.
 *
 * @param io the context to operate in
 * @param oid the name of the object
 * @param completion what to do when the remove is safe and complete
 * @returns 0 on success, -EROFS if the io context specifies a snap_seq
 * other than LIBRADOS_SNAP_HEAD
 */
int rados_aio_remove(rados_ioctx_t io, const char *oid,
		     rados_completion_t completion);

/**
 * Asychronously read data from an object
 *
 * The io context determines the snapshot to read from, if any was set
 * by rados_ioctx_snap_set_read().
 *
 * The return value of the completion will be number of bytes read on
 * success, negative error code on failure.
 *
 * @note only the 'complete' callback of the completion will be called.
 *
 * @param io the context in which to perform the read
 * @param oid the name of the object to read from
 * @param completion what to do when the read is complete
 * @param buf where to store the results
 * @param len the number of bytes to read
 * @param off the offset to start reading from in the object
 * @returns 0 on success, negative error code on failure
 */
int rados_aio_read(rados_ioctx_t io, const char *oid,
		   rados_completion_t completion,
		   char *buf, size_t len, uint64_t off);

/**
 * Block until all pending writes in an io context are safe
 *
 * This is not equivalent to calling rados_aio_wait_for_safe() on all
 * write completions, since this waits for the associated callbacks to
 * complete as well.
 *
 * @note BUG: always returns 0, should be void or accept a timeout
 *
 * @param io the context to flush
 * @returns 0 on success, negative error code on failure
 */
int rados_aio_flush(rados_ioctx_t io);


/**
 * Schedule a callback for when all currently pending
 * aio writes are safe. This is a non-blocking version of
 * rados_aio_flush().
 *
 * @param io the context to flush
 * @param completion what to do when the writes are safe
 * @returns 0 on success, negative error code on failure
 */
int rados_aio_flush_async(rados_ioctx_t io, rados_completion_t completion);


/**
 * Asynchronously get object stats (size/mtime)
 *
 * @param io ioctx
 * @param o object name
 * @param psize where to store object size
 * @param pmtime where to store modification time
 * @returns 0 on success, negative error code on failure
 */
int rados_aio_stat(rados_ioctx_t io, const char *o,
		   rados_completion_t completion,
		   uint64_t *psize, time_t *pmtime);

/**
 * Cancel async operation
 *
 * @param io ioctx
 * @param completion completion handle
 * @returns 0 on success, negative error code on failure
 */
int rados_aio_cancel(rados_ioctx_t io, rados_completion_t completion);

/** @} Asynchronous I/O */

/**
 * @defgroup librados_h_watch_notify Watch/Notify
 *
 * Watch/notify is a protocol to help communicate among clients. It
 * can be used to sychronize client state. All that's needed is a
 * well-known object name (for example, rbd uses the header object of
 * an image).
 *
 * Watchers register an interest in an object, and receive all
 * notifies on that object. A notify attempts to communicate with all
 * clients watching an object, and blocks on the notifier until each
 * client responds or a timeout is reached.
 *
 * See rados_watch() and rados_notify() for more details.
 *
 * @{
 */

/**
 * @typedef rados_watchcb_t
 *
 * Callback activated when a notify is received on a watched
 * object. Parameters are:
 * - opcode undefined
 * - ver version of the watched object
 * - arg application-specific data
 *
 * @note BUG: opcode is an internal detail that shouldn't be exposed
 */
typedef void (*rados_watchcb_t)(uint8_t opcode, uint64_t ver, void *arg);

/**
 * Register an interest in an object
 *
 * A watch operation registers the client as being interested in
 * notifications on an object. OSDs keep track of watches on
 * persistent storage, so they are preserved across cluster changes by
 * the normal recovery process. If the client loses its connection to
 * the primary OSD for a watched object, the watch will be removed
 * after 30 seconds. Watches are automatically reestablished when a new
 * connection is made, or a placement group switches OSDs.
 *
 * @note BUG: watch timeout should be configurable
 * @note BUG: librados should provide a way for watchers to notice connection resets
 * @note BUG: the ver parameter does not work, and -ERANGE will never be returned
 *            (See URL tracker.ceph.com/issues/2592)
 *
 * @param io the pool the object is in
 * @param o the object to watch
 * @param ver expected version of the object
 * @param handle where to store the internal id assigned to this watch
 * @param watchcb what to do when a notify is received on this object
 * @param arg application defined data to pass when watchcb is called
 * @returns 0 on success, negative error code on failure
 * @returns -ERANGE if the version of the object is greater than ver
 */
int rados_watch(rados_ioctx_t io, const char *o, uint64_t ver, uint64_t *handle,
                rados_watchcb_t watchcb, void *arg);

/**
 * Unregister an interest in an object
 *
 * Once this completes, no more notifies will be sent to us for this
 * watch. This should be called to clean up unneeded watchers.
 *
 * @param io the pool the object is in
 * @param o the name of the watched object
 * @param handle which watch to unregister
 * @returns 0 on success, negative error code on failure
 */
int rados_unwatch(rados_ioctx_t io, const char *o, uint64_t handle);

/**
 * Sychronously notify watchers of an object
 *
 * This blocks until all watchers of the object have received and
 * reacted to the notify, or a timeout is reached.
 *
 * @note BUG: the timeout is not changeable via the C API
 * @note BUG: the bufferlist is inaccessible in a rados_watchcb_t
 *
 * @param io the pool the object is in
 * @param o the name of the object
 * @param ver obsolete - just pass zero
 * @param buf data to send to watchers
 * @param buf_len length of buf in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_notify(rados_ioctx_t io, const char *o, uint64_t ver, const char *buf, int buf_len);

/** @} Watch/Notify */

/**
 * @defgroup librados_h_hints Hints
 *
 * @{
 */

/**
 * Set allocation hint for an object
 *
 * This is an advisory operation, it will always succeed (as if it was
 * submitted with a LIBRADOS_OP_FLAG_FAILOK flag set) and is not
 * guaranteed to do anything on the backend.
 *
 * @param io the pool the object is in
 * @param o the name of the object
 * @param expected_object_size expected size of the object, in bytes
 * @param expected_write_size expected size of writes to the object, in bytes
 * @returns 0 on success, negative error code on failure
 */
int rados_set_alloc_hint(rados_ioctx_t io, const char *o,
                         uint64_t expected_object_size,
                         uint64_t expected_write_size);

/** @} Hints */

/**
 * @defgroup librados_h_obj_op Object Operations
 *
 * A single rados operation can do multiple operations on one object
 * atomicly. The whole operation will suceed or fail, and no partial
 * results will be visible.
 *
 * Operations may be either reads, which can return data, or writes,
 * which cannot. The effects of writes are applied and visible all at
 * once, so an operation that sets an xattr and then checks its value
 * will not see the updated value.
 *
 * @{
 */

/**
 * Create a new rados_write_op_t write operation. This will store all actions
 * to be performed atomically. You must call rados_release_write_op when you are
 * finished with it.
 *
 * @returns non-NULL on success, NULL on memory allocation error.
 */
rados_write_op_t rados_create_write_op(void);

/**
 * Free a rados_write_op_t, must be called when you're done with it.
 * @param write_op operation to deallocate, created with rados_create_write_op
 */
void rados_release_write_op(rados_write_op_t write_op);

/**
 * Set flags for the last operation added to this write_op.
 * At least one op must have been added to the write_op.
 * @param flags see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
void rados_write_op_set_flags(rados_write_op_t write_op, int flags);

/**
 * Ensure that the object exists before writing
 * @param write_op operation to add this action to
 */
void rados_write_op_assert_exists(rados_write_op_t write_op);

/**
 * Ensure that given xattr satisfies comparison.
 * If the comparison is not satisfied, the return code of the
 * operation will be -ECANCELED
 * @param write_op operation to add this action to
 * @param name name of the xattr to look up
 * @param comparison_operator currently undocumented, look for
 * LIBRADOS_CMPXATTR_OP_EQ in librados.h
 * @param value buffer to compare actual xattr value to
 * @param value_len length of buffer to compare actual xattr value to
 */
void rados_write_op_cmpxattr(rados_write_op_t write_op,
                             const char *name,
                             uint8_t comparison_operator,
                             const char *value,
                             size_t value_len);

/**
 * Ensure that the an omap value satisfies a comparison,
 * with the supplied value on the right hand side (i.e.
 * for OP_LT, the comparison is actual_value < value.
 *
 * @param write_op operation to add this action to
 * @param key which omap value to compare
 * @param comparison_operator one of LIBRADOS_CMPXATTR_OP_EQ,
   LIBRADOS_CMPXATTR_OP_LT, or LIBRADOS_CMPXATTR_OP_GT
 * @param val value to compare with
 * @param val_len length of value in bytes
 * @param prval where to store the return value from this action
 */
void rados_write_op_omap_cmp(rados_write_op_t write_op,
			     const char *key,
			     uint8_t comparison_operator,
			     const char *val,
			     size_t val_len,
			     int *prval);

/**
 * Set an xattr
 * @param write_op operation to add this action to
 * @param name name of the xattr
 * @param value buffer to set xattr to
 * @param value_len length of buffer to set xattr to
 */
void rados_write_op_setxattr(rados_write_op_t write_op,
                             const char *name,
                             const char *value,
                             size_t value_len);

/**
 * Remove an xattr
 * @param write_op operation to add this action to
 * @param name name of the xattr to remove
 */
void rados_write_op_rmxattr(rados_write_op_t write_op, const char *name);

/**
 * Create the object
 * @param write_op operation to add this action to
 * @param exclusive set to either LIBRADOS_CREATE_EXCLUSIVE or
   LIBRADOS_CREATE_IDEMPOTENT
 * will error if the object already exists.
 * @param category category string (DEPRECATED, HAS NO EFFECT)
 */
void rados_write_op_create(rados_write_op_t write_op,
                           int exclusive,
                           const char* category);

/**
 * Write to offset
 * @param write_op operation to add this action to
 * @param offset offset to write to
 * @param buffer bytes to write
 * @param len length of buffer
 */
void rados_write_op_write(rados_write_op_t write_op,
                          const char *buffer,
                          size_t len,
                          uint64_t offset);

/**
 * Write whole object, atomically replacing it.
 * @param write_op operation to add this action to
 * @param buffer bytes to write
 * @param len length of buffer
 */
void rados_write_op_write_full(rados_write_op_t write_op,
                               const char *buffer,
                               size_t len);

/**
 * Append to end of object.
 * @param write_op operation to add this action to
 * @param buffer bytes to write
 * @param len length of buffer
 */
void rados_write_op_append(rados_write_op_t write_op,
                           const char *buffer,
                           size_t len);
/**
 * Remove object
 * @param write_op operation to add this action to
 */
void rados_write_op_remove(rados_write_op_t write_op);

/**
 * Truncate an object
 * @param write_op operation to add this action to
 * @offset Offset to truncate to
 */
void rados_write_op_truncate(rados_write_op_t write_op, uint64_t offset);

/**
 * Zero part of an object
 * @param write_op operation to add this action to
 * @offset Offset to zero
 * @len length to zero
 */
void rados_write_op_zero(rados_write_op_t write_op,
			 uint64_t offset,
			 uint64_t len);

/**
 * Execute an OSD class method on an object
 * See rados_exec() for general description.
 *
 * @param write_op operation to add this action to
 * @param cls the name of the class
 * @param method the name of the method
 * @param in_buf where to find input
 * @param in_len length of in_buf in bytes
 * @param prval where to store the return value from the method
 */
void rados_write_op_exec(rados_write_op_t write_op,
			 const char *cls,
			 const char *method,
			 const char *in_buf,
			 size_t in_len,
			 int *prval);

/**
 * Set key/value pairs on an object
 *
 * @param write_op operation to add this action to
 * @param keys array of null-terminated char arrays representing keys to set
 * @param vals array of pointers to values to set
 * @param lens array of lengths corresponding to each value
 * @param num number of key/value pairs to set
 */
void rados_write_op_omap_set(rados_write_op_t write_op,
			     char const* const* keys,
			     char const* const* vals,
			     const size_t *lens,
			     size_t num);

/**
 * Remove key/value pairs from an object
 *
 * @param write_op operation to add this action to
 * @param keys array of null-terminated char arrays representing keys to remove
 * @param keys_len number of key/value pairs to remove
 */
void rados_write_op_omap_rm_keys(rados_write_op_t write_op,
				 char const* const* keys,
				 size_t keys_len);

/**
 * Remove all key/value pairs from an object
 *
 * @param write_op operation to add this action to
 */
void rados_write_op_omap_clear(rados_write_op_t write_op);

/**
 * Set allocation hint for an object
 *
 * @param write_op operation to add this action to
 * @param expected_object_size expected size of the object, in bytes
 * @param expected_write_size expected size of writes to the object, in bytes
 */
void rados_write_op_set_alloc_hint(rados_write_op_t write_op,
                                   uint64_t expected_object_size,
                                   uint64_t expected_write_size);

/**
 * Perform a write operation synchronously
 * @param write_op operation to perform
 * @param io the ioctx that the object is in
 * @param oid the object id
 * @param mtime the time to set the mtime to, NULL for the current time
 * @param flags flags to apply to the entire operation (LIBRADOS_OPERATION_*)
 */
int rados_write_op_operate(rados_write_op_t write_op,
			   rados_ioctx_t io,
			   const char *oid,
			   time_t *mtime,
			   int flags);
/**
 * Perform a write operation asynchronously
 * @param write_op operation to perform
 * @param io the ioctx that the object is in
 * @param completion what to do when operation has been attempted
 * @param oid the object id
 * @param mtime the time to set the mtime to, NULL for the current time
 * @param flags flags to apply to the entire operation (LIBRADOS_OPERATION_*)
 */
int rados_aio_write_op_operate(rados_write_op_t write_op,
                               rados_ioctx_t io,
                               rados_completion_t completion,
                               const char *oid,
                               time_t *mtime,
			       int flags);

/**
 * Create a new rados_read_op_t write operation. This will store all
 * actions to be performed atomically. You must call
 * rados_release_read_op when you are finished with it (after it
 * completes, or you decide not to send it in the first place).
 *
 * @returns non-NULL on success, NULL on memory allocation error.
 */
rados_read_op_t rados_create_read_op(void);

/**
 * Free a rados_read_op_t, must be called when you're done with it.
 * @param read_op operation to deallocate, created with rados_create_read_op
 */
void rados_release_read_op(rados_read_op_t read_op);

/**
 * Set flags for the last operation added to this read_op.
 * At least one op must have been added to the read_op.
 * @param flags see librados.h constants beginning with LIBRADOS_OP_FLAG
 */
void rados_read_op_set_flags(rados_read_op_t read_op, int flags);

/**
 * Ensure that the object exists before reading
 * @param read_op operation to add this action to
 */
void rados_read_op_assert_exists(rados_read_op_t read_op);

/**
 * Ensure that the an xattr satisfies a comparison
 * If the comparison is not satisfied, the return code of the
 * operation will be -ECANCELED
 * @param read_op operation to add this action to
 * @param name name of the xattr to look up
 * @param comparison_operator currently undocumented, look for
 * LIBRADOS_CMPXATTR_OP_EQ in librados.h
 * @param value buffer to compare actual xattr value to
 * @param value_len length of buffer to compare actual xattr value to
 */
void rados_read_op_cmpxattr(rados_read_op_t read_op,
			    const char *name,
			    uint8_t comparison_operator,
			    const char *value,
			    size_t value_len);

/**
 * Start iterating over xattrs on an object.
 *
 * @param read_op operation to add this action to
 * @param iter where to store the iterator
 * @param prval where to store the return value of this action
 */
void rados_read_op_getxattrs(rados_read_op_t read_op,
			     rados_xattrs_iter_t *iter,
			     int *prval);

/**
 * Ensure that the an omap value satisfies a comparison,
 * with the supplied value on the right hand side (i.e.
 * for OP_LT, the comparison is actual_value < value.
 *
 * @param read_op operation to add this action to
 * @param key which omap value to compare
 * @param comparison_operator one of LIBRADOS_CMPXATTR_OP_EQ,
   LIBRADOS_CMPXATTR_OP_LT, or LIBRADOS_CMPXATTR_OP_GT
 * @param val value to compare with
 * @param val_len length of value in bytes
 * @param prval where to store the return value from this action
 */
void rados_read_op_omap_cmp(rados_read_op_t read_op,
			    const char *key,
			    uint8_t comparison_operator,
			    const char *val,
			    size_t val_len,
			    int *prval);

/**
 * Get object size and mtime
 * @param read_op operation to add this action to
 * @param psize where to store object size
 * @param pmtime where to store modification time
 * @param prval where to store the return value of this action
 */
void rados_read_op_stat(rados_read_op_t read_op,
			uint64_t *psize,
			time_t *pmtime,
			int *prval);

/**
 * Read bytes from offset into buffer.
 *
 * prlen will be filled with the number of bytes read if successful.
 * A short read can only occur if the read reaches the end of the
 * object.
 *
 * @param read_op operation to add this action to
 * @param offset offset to read from
 * @param buffer where to put the data
 * @param len length of buffer
 * @param prval where to store the return value of this action
 * @param bytes_read where to store the number of bytes read by this action
 */
void rados_read_op_read(rados_read_op_t read_op,
			uint64_t offset,
			size_t len,
			char *buf,
			size_t *bytes_read,
			int *prval);

/**
 * Execute an OSD class method on an object
 * See rados_exec() for general description.
 *
 * The output buffer is allocated on the heap; the caller is
 * expected to release that memory with rados_buffer_free(). The
 * buffer and length pointers can all be NULL, in which case they are
 * not filled in.
 *
 * @param read_op operation to add this action to
 * @param cls the name of the class
 * @param method the name of the method
 * @param in_buf where to find input
 * @param in_len length of in_buf in bytes
 * @param out_buf where to put librados-allocated output buffer
 * @param out_len length of out_buf in bytes
 * @param prval where to store the return value from the method
 */
void rados_read_op_exec(rados_read_op_t read_op,
			const char *cls,
			const char *method,
			const char *in_buf,
			size_t in_len,
			char **out_buf,
			size_t *out_len,
			int *prval);

/**
 * Execute an OSD class method on an object
 * See rados_exec() for general description.
 *
 * If the output buffer is too small, prval will
 * be set to -ERANGE and used_len will be 0.
 *
 * @param read_op operation to add this action to
 * @param cls the name of the class
 * @param method the name of the method
 * @param in_buf where to find input
 * @param in_len length of in_buf in bytes
 * @param out_buf user-provided buffer to read into
 * @param out_len length of out_buf in bytes
 * @param used_len where to store the number of bytes read into out_buf
 * @param prval where to store the return value from the method
 */
void rados_read_op_exec_user_buf(rados_read_op_t read_op,
				 const char *cls,
				 const char *method,
				 const char *in_buf,
				 size_t in_len,
				 char *out_buf,
				 size_t out_len,
				 size_t *used_len,
				 int *prval);

/**
 * Start iterating over key/value pairs on an object.
 *
 * They will be returned sorted by key.
 *
 * @param read_op operation to add this action to
 * @param start_after list keys starting after start_after
 * @param filter_prefix list only keys beginning with filter_prefix
 * @parem max_return list no more than max_return key/value pairs
 * @param iter where to store the iterator
 * @param prval where to store the return value from this action
 */
void rados_read_op_omap_get_vals(rados_read_op_t read_op,
				 const char *start_after,
				 const char *filter_prefix,
				 uint64_t max_return,
				 rados_omap_iter_t *iter,
				 int *prval);

/**
 * Start iterating over keys on an object.
 *
 * They will be returned sorted by key, and the iterator
 * will fill in NULL for all values if specified.
 *
 * @param read_op operation to add this action to
 * @param start_after list keys starting after start_after
 * @parem max_return list no more than max_return keys
 * @param iter where to store the iterator
 * @param prval where to store the return value from this action
 */
void rados_read_op_omap_get_keys(rados_read_op_t read_op,
				 const char *start_after,
				 uint64_t max_return,
				 rados_omap_iter_t *iter,
				 int *prval);

/**
 * Start iterating over specific key/value pairs
 *
 * They will be returned sorted by key.
 *
 * @param read_op operation to add this action to
 * @param keys array of pointers to null-terminated keys to get
 * @param keys_len the number of strings in keys
 * @param iter where to store the iterator
 * @param prval where to store the return value from this action
 */
void rados_read_op_omap_get_vals_by_keys(rados_read_op_t read_op,
					 char const* const* keys,
					 size_t keys_len,
					 rados_omap_iter_t *iter,
					 int *prval);

/**
 * Perform a read operation synchronously
 * @param read_op operation to perform
 * @io the ioctx that the object is in
 * @oid the object id
 * @flags flags to apply to the entire operation (LIBRADOS_OPERATION_*)
 */
int rados_read_op_operate(rados_read_op_t read_op,
			  rados_ioctx_t io,
			  const char *oid,
			  int flags);

/**
 * Perform a read operation asynchronously
 * @param read_op operation to perform
 * @io the ioctx that the object is in
 * @param completion what to do when operation has been attempted
 * @oid the object id
 * @flags flags to apply to the entire operation (LIBRADOS_OPERATION_*)
 */
int rados_aio_read_op_operate(rados_read_op_t read_op,
			      rados_ioctx_t io,
			      rados_completion_t completion,
			      const char *oid,
			      int flags);

/** @} Object Operations */

/**
 * Take an exclusive lock on an object.
 *
 * @param io the context to operate in
 * @param oid the name of the object
 * @param name the name of the lock
 * @param cookie user-defined identifier for this instance of the lock
 * @param desc user-defined lock description
 * @param duration the duration of the lock. Set to NULL for infinite duration.
 * @param flags lock flags
 * @returns 0 on success, negative error code on failure
 * @returns -EBUSY if the lock is already held by another (client, cookie) pair
 * @returns -EEXIST if the lock is already held by the same (client, cookie) pair
 */
int rados_lock_exclusive(rados_ioctx_t io, const char * o, const char * name,
	       const char * cookie, const char * desc, struct timeval * duration,
	       uint8_t flags);

/**
 * Take a shared lock on an object.
 *
 * @param io the context to operate in
 * @param o the name of the object
 * @param name the name of the lock
 * @param cookie user-defined identifier for this instance of the lock
 * @param tag The tag of the lock
 * @param desc user-defined lock description
 * @param duration the duration of the lock. Set to NULL for infinite duration.
 * @param flags lock flags
 * @returns 0 on success, negative error code on failure
 * @returns -EBUSY if the lock is already held by another (client, cookie) pair
 * @returns -EEXIST if the lock is already held by the same (client, cookie) pair
 */
int rados_lock_shared(rados_ioctx_t io, const char * o, const char * name,
	       const char * cookie, const char * tag, const char * desc,
	       struct timeval * duration, uint8_t flags);

/**
 * Release a shared or exclusive lock on an object.
 *
 * @param io the context to operate in
 * @param o the name of the object
 * @param name the name of the lock
 * @param cookie user-defined identifier for the instance of the lock
 * @returns 0 on success, negative error code on failure
 * @returns -ENOENT if the lock is not held by the specified (client, cookie) pair
 */
int rados_unlock(rados_ioctx_t io, const char *o, const char *name,
		 const char *cookie);

/**
 * List clients that have locked the named object lock and information about
 * the lock.
 *
 * The number of bytes required in each buffer is put in the
 * corresponding size out parameter. If any of the provided buffers
 * are too short, -ERANGE is returned after these sizes are filled in.
 *
 * @param io the context to operate in
 * @param o the name of the object
 * @param name the name of the lock
 * @param exclusive where to store whether the lock is exclusive (1) or shared (0)
 * @param tag where to store the tag associated with the object lock
 * @param tag_len number of bytes in tag buffer
 * @param clients buffer in which locker clients are stored, separated by '\0'
 * @param clients_len number of bytes in the clients buffer
 * @param cookies buffer in which locker cookies are stored, separated by '\0'
 * @param cookies_len number of bytes in the cookies buffer
 * @param addrs buffer in which locker addresses are stored, separated by '\0'
 * @param addrs_len number of bytes in the clients buffer
 * @returns number of lockers on success, negative error code on failure
 * @returns -ERANGE if any of the buffers are too short
 */
ssize_t rados_list_lockers(rados_ioctx_t io, const char *o,
			   const char *name, int *exclusive,
			   char *tag, size_t *tag_len,
			   char *clients, size_t *clients_len,
			   char *cookies, size_t *cookies_len,
			   char *addrs, size_t *addrs_len);

/**
 * Releases a shared or exclusive lock on an object, which was taken by the
 * specified client.
 *
 * @param io the context to operate in
 * @param o the name of the object
 * @param name the name of the lock
 * @param client the client currently holding the lock
 * @param cookie user-defined identifier for the instance of the lock
 * @returns 0 on success, negative error code on failure
 * @returns -ENOENT if the lock is not held by the specified (client, cookie) pair
 * @returns -EINVAL if the client cannot be parsed
 */
int rados_break_lock(rados_ioctx_t io, const char *o, const char *name,
		     const char *client, const char *cookie);
/**
 * @defgroup librados_h_commands Mon/OSD/PG Commands
 *
 * These interfaces send commands relating to the monitor, OSD, or PGs.
 *
 * @{
 */

/**
 * Send monitor command.
 *
 * @note Takes command string in carefully-formatted JSON; must match
 * defined commands, types, etc.
 *
 * The result buffers are allocated on the heap; the caller is
 * expected to release that memory with rados_buffer_free().  The
 * buffer and length pointers can all be NULL, in which case they are
 * not filled in.
 *
 * @param cluster cluster handle
 * @param cmd an array of char *'s representing the command
 * @param cmdlen count of valid entries in cmd
 * @param inbuf any bulk input data (crush map, etc.)
 * @param outbuf double pointer to output buffer
 * @param outbuflen pointer to output buffer length
 * @param outs double pointer to status string
 * @param outslen pointer to status string length
 * @returns 0 on success, negative error code on failure
 */
int rados_mon_command(rados_t cluster, const char **cmd, size_t cmdlen,
		      const char *inbuf, size_t inbuflen,
	       	      char **outbuf, size_t *outbuflen,
		      char **outs, size_t *outslen);

/**
 * Send monitor command to a specific monitor.
 *
 * @note Takes command string in carefully-formatted JSON; must match
 * defined commands, types, etc.
 *
 * The result buffers are allocated on the heap; the caller is
 * expected to release that memory with rados_buffer_free().  The
 * buffer and length pointers can all be NULL, in which case they are
 * not filled in.
 *
 * @param cluster cluster handle
 * @param name target monitor's name
 * @param cmd an array of char *'s representing the command
 * @param cmdlen count of valid entries in cmd
 * @param inbuf any bulk input data (crush map, etc.)
 * @param outbuf double pointer to output buffer
 * @param outbuflen pointer to output buffer length
 * @param outs double pointer to status string
 * @param outslen pointer to status string length
 * @returns 0 on success, negative error code on failure
 */
int rados_mon_command_target(rados_t cluster, const char *name,
			     const char **cmd, size_t cmdlen,
			     const char *inbuf, size_t inbuflen,
			     char **outbuf, size_t *outbuflen,
			     char **outs, size_t *outslen);

/**
 * free a rados-allocated buffer
 *
 * Release memory allocated by librados calls like rados_mon_command().
 *
 * @param buf buffer pointer
 */
void rados_buffer_free(char *buf);

int rados_osd_command(rados_t cluster, int osdid, const char **cmd,
		      size_t cmdlen,
		      const char *inbuf, size_t inbuflen,
		      char **outbuf, size_t *outbuflen,
		      char **outs, size_t *outslen);

int rados_pg_command(rados_t cluster, const char *pgstr, const char **cmd,
		     size_t cmdlen,
		     const char *inbuf, size_t inbuflen,
		     char **outbuf, size_t *outbuflen,
		     char **outs, size_t *outslen);

/*
 * This is not a doxygen comment leadin, because doxygen breaks on
 * a typedef with function params and returns, and I can't figure out
 * how to fix it.
 *
 * Monitor cluster log
 *
 * Monitor events logged to the cluster log.  The callback get each
 * log entry both as a single formatted line and with each field in a
 * separate arg.
 *
 * Calling with a cb argument of NULL will deregister any previously
 * registered callback.
 *
 * @param cluster cluster handle
 * @param level minimum log level (debug, info, warn|warning, err|error)
 * @param cb callback to run for each log message
 * @param arg void argument to pass to cb
 *
 * @returns 0 on success, negative code on error
 */
typedef void (*rados_log_callback_t)(void *arg,
				     const char *line,
				     const char *who, 
				     uint64_t sec, uint64_t nsec,
				     uint64_t seq, const char *level,
				     const char *msg);

int rados_monitor_log(rados_t cluster, const char *level, rados_log_callback_t cb, void *arg);

/** @} Mon/OSD/PG commands */

#ifdef __cplusplus
}
#endif

#endif
