// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIB_H
#define CEPH_LIB_H

#include <features.h>
#include <utime.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/socket.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define LIBCEPHFS_VER_MAJOR 0
#define LIBCEPHFS_VER_MINOR 94
#define LIBCEPHFS_VER_EXTRA 0

#define LIBCEPHFS_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define LIBCEPHFS_VERSION_CODE LIBCEPHFS_VERSION(LIBCEPHFS_VER_MAJOR, LIBCEPHFS_VER_MINOR, LIBCEPHFS_VER_EXTRA)

/*
 * If using glibc check that file offset is 64-bit.
 */
#if defined(__GLIBC__) && !defined(__USE_FILE_OFFSET64)
# error libceph: glibc must define __USE_FILE_OFFSET64 or readdir results will be corrupted
#endif

/*
 * XXXX redeclarations from ceph_fs.h, rados.h, etc.  We need more of this
 * in the interface, but shouldn't be re-typing it (and using different
 * C data types).
 */
#ifndef __cplusplus

#define CEPH_INO_ROOT  1
#define CEPH_NOSNAP  ((uint64_t)(-2))

struct ceph_file_layout {
	/* file -> object mapping */
	uint32_t fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple
				      of page size. */
	uint32_t fl_stripe_count;    /* over this many objects */
	uint32_t fl_object_size;     /* until objects are this big, then move to
				      new objects */
	uint32_t fl_cas_hash;        /* 0 = none; 1 = sha256 */

	/* pg -> disk layout */
	uint32_t fl_object_stripe_unit;  /* for per-object parity, if any */

	/* object -> pg layout */
	uint32_t fl_pg_preferred; /* preferred primary for pg (-1 for none) */
	uint32_t fl_pg_pool;      /* namespace, crush ruleset, rep level */
} __attribute__ ((packed));


typedef struct inodeno_t {
  uint64_t val;
} inodeno_t;

typedef struct _snapid_t {
  uint64_t val;
} snapid_t;

typedef struct vinodeno_t {
  inodeno_t ino;
  snapid_t snapid;
} vinodeno_t;

typedef struct Fh Fh;
#else /* _cplusplus */

struct inodeno_t;
struct vinodeno_t;
typedef struct vinodeno_t vinodeno;

#endif /* ! __cplusplus */

struct Inode;
typedef struct Inode Inode;

struct ceph_mount_info;
struct ceph_dir_result;
struct CephContext;

/* setattr mask bits */
#ifndef CEPH_SETATTR_MODE
# define CEPH_SETATTR_MODE   1
# define CEPH_SETATTR_UID    2
# define CEPH_SETATTR_GID    4
# define CEPH_SETATTR_MTIME  8
# define CEPH_SETATTR_ATIME 16
# define CEPH_SETATTR_SIZE  32
# define CEPH_SETATTR_CTIME 64
#endif

/* define error codes for the mount function*/
# define CEPHFS_ERROR_MON_MAP_BUILD 1000
# define CEPHFS_ERROR_NEW_CLIENT 1002
# define CEPHFS_ERROR_MESSENGER_START 1003

/**
 * @defgroup libcephfs_h_init Setup and Teardown
 * These are the first and last functions that should be called
 * when using libcephfs.
 *
 * @{
 */

/**
 * Get the version of libcephfs.
 *
 * The version number is major.minor.patch.
 *
 * @param major where to store the major version number
 * @param minor where to store the minor version number
 * @param patch where to store the extra version number
 */
const char *ceph_version(int *major, int *minor, int *patch);

/**
 * Create a mount handle for interacting with Ceph.  All libcephfs
 * functions operate on a mount info handle.
 *
 * @param cmount the mount info handle to initialize
 * @param id the id of the client.  This can be a unique id that identifies
 *           this client, and will get appended onto "client.".  Callers can
 *           pass in NULL, and the id will be the process id of the client.
 * @returns 0 on success, negative error code on failure
 */
int ceph_create(struct ceph_mount_info **cmount, const char * const id);

/**
 * Create a mount handle from a CephContext, which holds the configuration
 * for the ceph cluster.  A CephContext can be acquired from an existing ceph_mount_info
 * handle, using the @ref ceph_get_mount_context call.  Note that using the same CephContext
 * for two different mount handles results in the same client entity id being used.
 *
 * @param cmount the mount info handle to initialize
 * @param conf reuse this pre-existing CephContext config
 * @returns 0 on success, negative error code on failure
 */
int ceph_create_with_context(struct ceph_mount_info **cmount, struct CephContext *conf);


typedef void *rados_t;

/**
 * Create a mount handle from a rados_t, for using libcephfs in the
 * same process as librados.
 *
 * @param cmount the mount info handle to initialize
 * @param cluster reference to already-initialized librados handle
 * @returns 0 on success, negative error code on failure
 */
int ceph_create_from_rados(struct ceph_mount_info **cmount, rados_t cluster);

/**
 * Initialize the filesystem client (but do not mount the filesystem yet)
 *
 * @returns 0 on success, negative error code on failure
 */
int ceph_init(struct ceph_mount_info *cmount);


/**
 * Perform a mount using the path for the root of the mount.
 *
 * It is optional to call ceph_init before this.  If ceph_init has
 * not already been called, it will be called in the course of this operation.
 *
 * @param cmount the mount info handle
 * @param root the path for the root of the mount.  This can be an existing
 *	       directory within the ceph cluster, but most likely it will
 * 	       be "/".  Passing in NULL is equivalent to "/".
 * @returns 0 on success, negative error code on failure
 */
int ceph_mount(struct ceph_mount_info *cmount, const char *root);


/**
 * Execute a management command remotely on an MDS.
 *
 * Must have called ceph_init or ceph_mount before calling this.
 *
 * @param mds_spec string representing rank, MDS name, GID or '*'
 * @param cmd array of null-terminated strings
 * @param cmdlen length of cmd array
 * @param inbuf non-null-terminated input data to command
 * @param inbuflen length in octets of inbuf
 * @param outbuf populated with pointer to buffer (command output data)
 * @param outbuflen length of allocated outbuf
 * @param outs populated with pointer to buffer (command error strings)
 * @param outslen length of allocated outs
 *
 * @return 0 on success, negative error code on failure
 *
 */
int ceph_mds_command(struct ceph_mount_info *cmount,
    const char *mds_spec,
    const char **cmd,
    size_t cmdlen,
    const char *inbuf, size_t inbuflen,
    char **outbuf, size_t *outbuflen,
    char **outs, size_t *outslen);

/**
 * Free a buffer, such as those used for output arrays from ceph_mds_command
 */
void ceph_buffer_free(char *buf);

/**
 * Unmount a mount handle.
 *
 * @param cmount the mount handle
 * @return 0 on success, negative error code on failure
 */
int ceph_unmount(struct ceph_mount_info *cmount);

/**
 * Destroy the mount handle.
 *
 * The handle should not be mounted. This should be called on completion of
 * all libcephfs functions.
 *
 * @param cmount the mount handle
 * @return 0 on success, negative error code on failure.
 */
int ceph_release(struct ceph_mount_info *cmount);

/**
 * Deprecated. Unmount and destroy the ceph mount handle. This should be
 * called on completion of all libcephfs functions.
 *
 * Equivalent to ceph_unmount() + ceph_release() without error handling.
 *
 * @param cmount the mount handle to shutdown
 */
void ceph_shutdown(struct ceph_mount_info *cmount);

/**
 * Extract the CephContext from the mount point handle.
 *
 * @param cmount the ceph mount handle to get the context from.
 * @returns the CephContext associated with the mount handle.
 */
struct CephContext *ceph_get_mount_context(struct ceph_mount_info *cmount);

/*
 * Check mount status.
 *
 * Return non-zero value if mounted. Otherwise, zero.
 */
int ceph_is_mounted(struct ceph_mount_info *cmount);

/** @} init */

/**
 * @defgroup libcephfs_h_config Config
 * Functions for manipulating the Ceph configuration at runtime.
 *
 * @{
 */

/**
 * Load the ceph configuration from the specified config file.
 *
 * @param cmount the mount handle to load the configuration into.
 * @param path_list the configuration file path
 * @returns 0 on success, negative error code on failure
 */
int ceph_conf_read_file(struct ceph_mount_info *cmount, const char *path_list);

/**
 * Parse the command line arguments and load the configuration parameters.
 *
 * @param cmount the mount handle to load the configuration parameters into.
 * @param argc count of the arguments in argv
 * @param argv the argument list
 * @returns 0 on success, negative error code on failure
 */
int ceph_conf_parse_argv(struct ceph_mount_info *cmount, int argc, const char **argv);

/**
 * Configure the cluster handle based on an environment variable
 *
 * The contents of the environment variable are parsed as if they were
 * Ceph command line options. If var is NULL, the CEPH_ARGS
 * environment variable is used.
 *
 * @pre ceph_mount() has not been called on the handle
 *
 * @note BUG: this is not threadsafe - it uses a static buffer
 *
 * @param cmount handle to configure
 * @param var name of the environment variable to read
 * @returns 0 on success, negative error code on failure
 */
int ceph_conf_parse_env(struct ceph_mount_info *cmount, const char *var);

/** Sets a configuration value from a string.
 *
 * @param cmount the mount handle to set the configuration value on
 * @param option the configuration option to set
 * @param value the value of the configuration option to set
 * 
 * @returns 0 on success, negative error code otherwise.
 */
int ceph_conf_set(struct ceph_mount_info *cmount, const char *option, const char *value);

/**
 * Gets the configuration value as a string.
 *
 * @param cmount the mount handle to set the configuration value on
 * @param option the config option to get
 * @param buf the buffer to fill with the value
 * @param len the length of the buffer.
 * @returns the size of the buffer filled in with the value, or negative error code on failure
 */
int ceph_conf_get(struct ceph_mount_info *cmount, const char *option, char *buf, size_t len);

/** @} config */

/**
 * @defgroup libcephfs_h_fsops File System Operations.
 * Functions for getting/setting file system wide information specific to a particular
 * mount handle.
 *
 * @{
 */

/**
 * Perform a statfs on the ceph file system.  This call fills in file system wide statistics
 * into the passed in buffer.
 *
 * @param cmount the ceph mount handle to use for performing the statfs.
 * @param path can be any path within the mounted filesystem
 * @param stbuf the file system statistics filled in by this function.
 * @return 0 on success, negative error code otherwise.
 */
int ceph_statfs(struct ceph_mount_info *cmount, const char *path, struct statvfs *stbuf);

/**
 * Synchronize all filesystem data to persistent media.
 *
 * @param cmount the ceph mount handle to use for performing the sync_fs.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_sync_fs(struct ceph_mount_info *cmount);

/**
 * Get the current working directory.
 *
 * @param cmount the ceph mount to get the current working directory for.
 * @returns the path to the current working directory
 */
const char* ceph_getcwd(struct ceph_mount_info *cmount);

/**
 * Change the current working directory.
 *
 * @param cmount the ceph mount to change the current working directory for.
 * @param path the path to the working directory to change into.
 * @returns 0 on success, negative error code otherwise.
 */
int ceph_chdir(struct ceph_mount_info *cmount, const char *path);

/** @} fsops */

/**
 * @defgroup libcephfs_h_dir Directory Operations.
 * Functions for manipulating and listing directories.
 *
 * @{
 */

/**
 * Open the given directory.
 *
 * @param cmount the ceph mount handle to use to open the directory
 * @param name the path name of the directory to open.  Must be either an absolute path
 *        or a path relative to the current working directory.
 * @param dirpp the directory result pointer structure to fill in.
 * @returns 0 on success or negative error code otherwise.
 */
int ceph_opendir(struct ceph_mount_info *cmount, const char *name, struct ceph_dir_result **dirpp);

/**
 * Close the open directory.
 *
 * @param cmount the ceph mount handle to use for closing the directory
 * @param dirp the directory result pointer (set by ceph_opendir) to close
 * @returns 0 on success or negative error code on failure.
 */
int ceph_closedir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);

/**
 * Get the next entry in an open directory.
 *
 * @param cmount the ceph mount handle to use for performing the readdir.
 * @param dirp the directory stream pointer from an opendir holding the state of the
 *        next entry to return.
 * @returns the next directory entry or NULL if at the end of the directory (or the directory
 *          is empty.  This pointer should not be freed by the caller, and is only safe to
 *          access between return and the next call to ceph_readdir or ceph_closedir.
 */
struct dirent * ceph_readdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);

/**
 * A safe version of ceph_readdir, where the directory entry struct is allocated by the caller.
 *
 * @param cmount the ceph mount handle to use for performing the readdir.
 * @param dirp the directory stream pointer from an opendir holding the state of the
 *        next entry to return.
 * @param de the directory entry pointer filled in with the next directory entry of the dirp state.
 * @returns 1 if the next entry was filled in, 0 if the end of the directory stream was reached,
 *          and a negative error code on failure.
 */
int ceph_readdir_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de);

/**
 * A safe version of ceph_readdir that also returns the file statistics (readdir+stat).
 *
 * @param cmount the ceph mount handle to use for performing the readdir_plus_r.
 * @param dirp the directory stream pointer from an opendir holding the state of the
 *        next entry to return.
 * @param de the directory entry pointer filled in with the next directory entry of the dirp state.
 * @param st the stats of the file/directory of the entry returned
 * @param stmask a mask that gets filled in with the stats fields that are being set in the st parameter.
 * @returns 1 if the next entry was filled in, 0 if the end of the directory stream was reached,
 *          and a negative error code on failure.
 */
int ceph_readdirplus_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de,
		       struct stat *st, int *stmask);

/**
 * Gets multiple directory entries.
 *
 * @param cmount the ceph mount handle to use for performing the getdents.
 * @param dirp the directory stream pointer from an opendir holding the state of the
 *        next entry/entries to return.
 * @param name an array of struct dirent that gets filled in with the  to fill returned directory entries into.
 * @param buflen the length of the buffer, which should be the number of dirent structs * sizeof(struct dirent).
 * @returns the length of the buffer that was filled in, will always be multiples of sizeof(struct dirent), or a
 *          negative error code.  If the buffer is not large enough for a single entry, -ERANGE is returned.
 */
int ceph_getdents(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, char *name, int buflen);

/**
 * Gets multiple directory names.
 * 
 * @param cmount the ceph mount handle to use for performing the getdents.
 * @param dirp the directory stream pointer from an opendir holding the state of the
 *        next entry/entries to return.
 * @param name a buffer to fill in with directory entry names.
 * @param buflen the length of the buffer that can be filled in.
 * @returns the length of the buffer filled in with entry names, or a negative error code on failure.
 *          If the buffer isn't large enough for a single entry, -ERANGE is returned.
 */
int ceph_getdnames(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, char *name, int buflen);

/**
 * Rewind the directory stream to the beginning of the directory.
 *
 * @param cmount the ceph mount handle to use for performing the rewinddir.
 * @param dirp the directory stream pointer to rewind.
 */
void ceph_rewinddir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);

/**
 * Get the current position of a directory stream.
 *
 * @param cmount the ceph mount handle to use for performing the telldir.
 * @param dirp the directory stream pointer to get the current position of.
 * @returns the position of the directory stream.  Note that the offsets returned
 *          by ceph_telldir do not have a particular order (cannot be compared with
 *          inequality).
 */
int64_t ceph_telldir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);

/**
 * Move the directory stream to a position specified by the given offset.
 *
 * @param cmount the ceph mount handle to use for performing the seekdir.
 * @param dirp the directory stream pointer to move.
 * @param offset the position to move the directory stream to.  This offset should be
 *        a value returned by seekdir.  Note that this value does not refer to the nth
 *        entry in a directory, and can not be manipulated with plus or minus.
 */
void ceph_seekdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, int64_t offset);

/**
 * Create a directory.
 *
 * @param cmount the ceph mount handle to use for making the directory.
 * @param path the path of the directory to create.  This must be either an
 *        absolute path or a relative path off of the current working directory.
 * @param mode the permissions the directory should have once created.
 * @returns 0 on success or a negative return code on error.
 */
int ceph_mkdir(struct ceph_mount_info *cmount, const char *path, mode_t mode);

/**
 * Create multiple directories at once.
 *
 * @param cmount the ceph mount handle to use for making the directories.
 * @param path the full path of directories and sub-directories that should
 *        be created.
 * @param mode the permissions the directory should have once created.
 * @returns 0 on success or a negative return code on error.
 */
int ceph_mkdirs(struct ceph_mount_info *cmount, const char *path, mode_t mode);

/**
 * Remove a directory.
 *
 * @param cmount the ceph mount handle to use for removing directories.
 * @param path the path of the directory to remove.
 * @returns 0 on success or a negative return code on error.
 */
int ceph_rmdir(struct ceph_mount_info *cmount, const char *path);

/** @} dir */

/**
 * @defgroup libcephfs_h_links Links and Link Handling.
 * Functions for creating and manipulating hard links and symbolic inks.
 *
 * @{
 */

/**
 * Create a link.
 *
 * @param cmount the ceph mount handle to use for creating the link.
 * @param existing the path to the existing file/directory to link to.
 * @param newname the path to the new file/directory to link from.
 * @returns 0 on success or a negative return code on error.
 */
int ceph_link(struct ceph_mount_info *cmount, const char *existing, const char *newname);

/**
 * Read a symbolic link.
 *
 * @param cmount the ceph mount handle to use for creating the link.
 * @param path the path to the symlink to read
 * @param buf the buffer to hold the the path of the file that the symlink points to.
 * @param size the length of the buffer
 * @returns number of bytes copied on success or negative error code on failure
 */
int ceph_readlink(struct ceph_mount_info *cmount, const char *path, char *buf, int64_t size);

/**
 * Creates a symbolic link.
 *
 * @param cmount the ceph mount handle to use for creating the symbolic link.
 * @param existing the path to the existing file/directory to link to.
 * @param newname the path to the new file/directory to link from.
 * @returns 0 on success or a negative return code on failure.
 */
int ceph_symlink(struct ceph_mount_info *cmount, const char *existing, const char *newname);

/** @} links */

/**
 * @defgroup libcephfs_h_files File manipulation and handling.
 * Functions for creating and manipulating files.
 *
 * @{
 */

/**
 * Removes a file, link, or symbolic link.  If the file/link has multiple links to it, the
 * file will not disappear from the namespace until all references to it are removed.
 * 
 * @param cmount the ceph mount handle to use for performing the unlink.
 * @param path the path of the file or link to unlink.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_unlink(struct ceph_mount_info *cmount, const char *path);

/**
 * Rename a file or directory.
 *
 * @param cmount the ceph mount handle to use for performing the rename.
 * @param from the path to the existing file or directory.
 * @param to the new name of the file or directory
 * @returns 0 on success or negative error code on failure.
 */
int ceph_rename(struct ceph_mount_info *cmount, const char *from, const char *to);

/**
 * Get a file's statistics and attributes.
 *
 * @param cmount the ceph mount handle to use for performing the stat.
 * @param path the file or directory to get the statistics of.
 * @param stbuf the stat struct that will be filled in with the file's statistics.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_stat(struct ceph_mount_info *cmount, const char *path, struct stat *stbuf);

/**
 * Get a file's statistics and attributes, without following symlinks.
 *
 * @param cmount the ceph mount handle to use for performing the stat.
 * @param path the file or directory to get the statistics of.
 * @param stbuf the stat struct that will be filled in with the file's statistics.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_lstat(struct ceph_mount_info *cmount, const char *path, struct stat *stbuf);

/**
 * Set a file's attributes.
 * 
 * @param cmount the ceph mount handle to use for performing the setattr.
 * @param relpath the path to the file/directory to set the attributes of.
 * @param attr the stat struct that must include attribute values to set on the file.
 * @param mask a mask of all the stat values that have been set on the stat struct.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_setattr(struct ceph_mount_info *cmount, const char *relpath, struct stat *attr, int mask);

/**
 * Change the mode bits (permissions) of a file/directory.
 *
 * @param cmount the ceph mount handle to use for performing the chmod.
 * @param path the path to the file/directory to change the mode bits on.
 * @param mode the new permissions to set.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_chmod(struct ceph_mount_info *cmount, const char *path, mode_t mode);

/**
 * Change the mode bits (permissions) of an open file.
 *
 * @param cmount the ceph mount handle to use for performing the chmod.
 * @param fd the open file descriptor to change the mode bits on.
 * @param mode the new permissions to set.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_fchmod(struct ceph_mount_info *cmount, int fd, mode_t mode);

/**
 * Change the ownership of a file/directory.
 * 
 * @param cmount the ceph mount handle to use for performing the chown.
 * @param path the path of the file/directory to change the ownership of.
 * @param uid the user id to set on the file/directory.
 * @param gid the group id to set on the file/directory.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_chown(struct ceph_mount_info *cmount, const char *path, int uid, int gid);

/**
 * Change the ownership of a file from an open file descriptor.
 *
 * @param cmount the ceph mount handle to use for performing the chown.
 * @param fd the fd of the open file/directory to change the ownership of.
 * @param uid the user id to set on the file/directory.
 * @param gid the group id to set on the file/directory.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_fchown(struct ceph_mount_info *cmount, int fd, int uid, int gid);

/**
 * Change the ownership of a file/directory, don't follow symlinks.
 * 
 * @param cmount the ceph mount handle to use for performing the chown.
 * @param path the path of the file/directory to change the ownership of.
 * @param uid the user id to set on the file/directory.
 * @param gid the group id to set on the file/directory.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_lchown(struct ceph_mount_info *cmount, const char *path, int uid, int gid);

/**
 * Change file/directory last access and modification times.
 *
 * @param cmount the ceph mount handle to use for performing the utime.
 * @param path the path to the file/directory to set the time values of.
 * @param buf holding the access and modification times to set on the file.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_utime(struct ceph_mount_info *cmount, const char *path, struct utimbuf *buf);

/**
 * Apply or remove an advisory lock.
 *
 * @param cmount the ceph mount handle to use for performing the lock.
 * @param fd the open file descriptor to change advisory lock.
 * @param operation the advisory lock operation to be performed on the file
 * descriptor among LOCK_SH (shared lock), LOCK_EX (exclusive lock),
 * or LOCK_UN (remove lock). The LOCK_NB value can be ORed to perform a
 * non-blocking operation.
 * @param owner the user-supplied owner identifier (an arbitrary integer)
 * @returns 0 on success or negative error code on failure.
 */
int ceph_flock(struct ceph_mount_info *cmount, int fd, int operation,
	       uint64_t owner);

/**
 * Truncate the file to the given size.  If this operation causes the
 * file to expand, the empty bytes will be filled in with zeros.
 *
 * @param cmount the ceph mount handle to use for performing the truncate.
 * @param path the path to the file to truncate.
 * @param size the new size of the file.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_truncate(struct ceph_mount_info *cmount, const char *path, int64_t size);

/**
 * Make a block or character special file.
 *
 * @param cmount the ceph mount handle to use for performing the mknod.
 * @param path the path to the special file.
 * @param mode the permissions to use and the type of special file.  The type can be
 *        one of S_IFREG, S_IFCHR, S_IFBLK, S_IFIFO.
 * @param rdev If the file type is S_IFCHR or S_IFBLK then this parameter specifies the
 *        major and minor numbers of the newly created device special file.  Otherwise, 
 *        it is ignored.
 * @returns 0 on success or negative error code on failure.
 */
int ceph_mknod(struct ceph_mount_info *cmount, const char *path, mode_t mode, dev_t rdev);
/**
 * Create and/or open a file.
 *
 * @param cmount the ceph mount handle to use for performing the open.
 * @param path the path of the file to open.  If the flags parameter includes O_CREAT,
 *        the file will first be created before opening.
 * @param flags a set of option masks that control how the file is created/opened.
 * @param mode the permissions to place on the file if the file does not exist and O_CREAT
 *        is specified in the flags.
 * @returns a non-negative file descriptor number on success or a negative error code on failure.
 */
int ceph_open(struct ceph_mount_info *cmount, const char *path, int flags, mode_t mode);

/**
 * Create and/or open a file with a specific file layout.
 *
 * @param cmount the ceph mount handle to use for performing the open.
 * @param path the path of the file to open.  If the flags parameter includes O_CREAT,
 *        the file will first be created before opening.
 * @param flags a set of option masks that control how the file is created/opened.
 * @param mode the permissions to place on the file if the file does not exist and O_CREAT
 *        is specified in the flags.
 * @param stripe_unit the stripe unit size (option, 0 for default)
 * @param stripe_count the stripe count (optional, 0 for default)
 * @param object_size the object size (optional, 0 for default)
 * @param data_pool name of target data pool name (optional, NULL or empty string for default)
 * @returns a non-negative file descriptor number on success or a negative error code on failure.
 */
int ceph_open_layout(struct ceph_mount_info *cmount, const char *path, int flags,
 		     mode_t mode, int stripe_unit, int stripe_count, int object_size,
 		     const char *data_pool);

/**
 * Close the open file.
 *
 * @param cmount the ceph mount handle to use for performing the close.
 * @param fd the file descriptor referring to the open file.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_close(struct ceph_mount_info *cmount, int fd);

/**
 * Reposition the open file stream based on the given offset.
 *
 * @param cmount the ceph mount handle to use for performing the lseek.
 * @param fd the open file descriptor referring to the open file and holding the
 *        current position of the stream.
 * @param offset the offset to set the stream to
 * @param whence the flag to indicate what type of seeking to perform:
 *	SEEK_SET: the offset is set to the given offset in the file.
 *      SEEK_CUR: the offset is set to the current location plus @e offset bytes.
 *      SEEK_END: the offset is set to the end of the file plus @e offset bytes.
 * @returns 0 on success or a negative error code on failure.
 */
int64_t ceph_lseek(struct ceph_mount_info *cmount, int fd, int64_t offset, int whence);
/**
 * Read data from the file.
 *
 * @param cmount the ceph mount handle to use for performing the read.
 * @param fd the file descriptor of the open file to read from.
 * @param buf the buffer to read data into
 * @param size the initial size of the buffer
 * @param offset the offset in the file to read from.  If this value is negative, the
 *        function reads from the current offset of the file descriptor.
 * @returns the number of bytes read into buf, or a negative error code on failure.
 */
int ceph_read(struct ceph_mount_info *cmount, int fd, char *buf, int64_t size, int64_t offset);

/**
 * Read data from the file.
 * @param cmount the ceph mount handle to use for performing the read.
 * @param fd the file descriptor of the open file to read from.
 * @param iov the iov structure to read data into
 * @param iovcnt the number of items that iov includes
 * @param offset the offset in the file to read from.  If this value is negative, the
 *        function reads from the current offset of the file descriptor.
 * @returns the number of bytes read into buf, or a negative error code on failure.
 */
int ceph_preadv(struct ceph_mount_info *cmount, int fd, const struct iovec *iov, int iovcnt,
           int64_t offset);

/**
 * Write data to a file.
 *
 * @param cmount the ceph mount handle to use for performing the write.
 * @param fd the file descriptor of the open file to write to
 * @param buf the bytes to write to the file
 * @param size the size of the buf array
 * @param offset the offset of the file write into.  If this value is negative, the
 *        function writes to the current offset of the file descriptor.
 * @returns the number of bytes written, or a negative error code
 */
int ceph_write(struct ceph_mount_info *cmount, int fd, const char *buf, int64_t size,
	       int64_t offset);

/**
 * Write data to a file.
 *
 * @param cmount the ceph mount handle to use for performing the write.
 * @param fd the file descriptor of the open file to write to
 * @param iov the iov structure to read data into
 * @param iovcnt the number of items that iov includes
 * @param offset the offset of the file write into.  If this value is negative, the
 *        function writes to the current offset of the file descriptor.
 * @returns the number of bytes written, or a negative error code
 */
int ceph_pwritev(struct ceph_mount_info *cmount, int fd, const struct iovec *iov, int iovcnt,
           int64_t offset);

/**
 * Truncate a file to the given size.
 *
 * @param cmount the ceph mount handle to use for performing the ftruncate.
 * @param fd the file descriptor of the file to truncate
 * @param size the new size of the file
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_ftruncate(struct ceph_mount_info *cmount, int fd, int64_t size);

/**
 * Synchronize an open file to persistent media.
 *
 * @param cmount the ceph mount handle to use for performing the fsync.
 * @param fd the file descriptor of the file to sync.
 * @param syncdataonly a boolean whether to synchronize metadata and data (0)
 *        or just data (1).
 * @return 0 on success or a negative error code on failure.
 */
int ceph_fsync(struct ceph_mount_info *cmount, int fd, int syncdataonly);

/**
 * Preallocate or release disk space for the file for the byte range.
 *
 * @param cmount the ceph mount handle to use for performing the fallocate.
 * @param fd the file descriptor of the file to fallocate.
 * @param mode the flags determines the operation to be performed on the given range.
 *        default operation (0) allocate and initialize to zero the file in the byte range,
 *        and the file size will be changed if offset + length is greater than
 *        the file size. if the FALLOC_FL_KEEP_SIZE flag is specified in the mode,
 *        the file size will not be changed. if the FALLOC_FL_PUNCH_HOLE flag is
 *        specified in the mode, the operation is deallocate space and zero the byte range.
 * @param offset the byte range starting.
 * @param length the length of the range.
 * @return 0 on success or a negative error code on failure.
 */
int ceph_fallocate(struct ceph_mount_info *cmount, int fd, int mode,
	                      int64_t offset, int64_t length);

/**
 * Get the open file's statistics.
 *
 * @param cmount the ceph mount handle to use for performing the fstat.
 * @param fd the file descriptor of the file to get statistics of.
 * @param stbuf the stat struct of the file's statistics, filled in by the
 *    function.
 * @returns 0 on success or a negative error code on failure
 */
int ceph_fstat(struct ceph_mount_info *cmount, int fd, struct stat *stbuf);

/** @} file */

/**
 * @defgroup libcephfs_h_xattr Extended Attribute manipulation and handling.
 * Functions for creating and manipulating extended attributes on files.
 *
 * @{
 */

/**
 * Get an extended attribute.
 *
 * @param cmount the ceph mount handle to use for performing the getxattr.
 * @param path the path to the file
 * @param name the name of the extended attribute to get
 * @param value a pre-allocated buffer to hold the xattr's value
 * @param size the size of the pre-allocated buffer
 * @returns the size of the value or a negative error code on failure.
 */
int ceph_getxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	void *value, size_t size);

/**
 * Get an extended attribute.
 *
 * @param cmount the ceph mount handle to use for performing the getxattr.
 * @param fd the open file descriptor referring to the file to get extended attribute from.
 * @param name the name of the extended attribute to get
 * @param value a pre-allocated buffer to hold the xattr's value
 * @param size the size of the pre-allocated buffer
 * @returns the size of the value or a negative error code on failure.
 */
int ceph_fgetxattr(struct ceph_mount_info *cmount, int fd, const char *name,
	void *value, size_t size);

/**
 * Get an extended attribute wihtout following symbolic links.  This function is
 * identical to ceph_getxattr, but if the path refers to a symbolic link,
 * we get the extended attributes of the symlink rather than the attributes
 * of the link itself.
 *
 * @param cmount the ceph mount handle to use for performing the lgetxattr.
 * @param path the path to the file
 * @param name the name of the extended attribute to get
 * @param value a pre-allocated buffer to hold the xattr's value
 * @param size the size of the pre-allocated buffer
 * @returns the size of the value or a negative error code on failure.
 */
int ceph_lgetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	void *value, size_t size);

/**
 * List the extended attribute keys on a file.
 *
 * @param cmount the ceph mount handle to use for performing the listxattr.
 * @param path the path to the file.
 * @param list a buffer to be filled in with the list of extended attributes keys.
 * @param size the size of the list buffer.
 * @returns the size of the resulting list filled in.
 */
int ceph_listxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size);

/**
 * List the extended attribute keys on a file.
 *
 * @param cmount the ceph mount handle to use for performing the listxattr.
 * @param fd the open file descriptor referring to the file to list extended attributes on.
 * @param list a buffer to be filled in with the list of extended attributes keys.
 * @param size the size of the list buffer.
 * @returns the size of the resulting list filled in.
 */
int ceph_flistxattr(struct ceph_mount_info *cmount, int fd, char *list, size_t size);

/**
 * Get the list of extended attribute keys on a file, but do not follow symbolic links.
 *
 * @param cmount the ceph mount handle to use for performing the llistxattr.
 * @param path the path to the file.
 * @param list a buffer to be filled in with the list of extended attributes keys.
 * @param size the size of the list buffer.
 * @returns the size of the resulting list filled in.
 */
int ceph_llistxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size);

/**
 * Remove an extended attribute from a file.
 *
 * @param cmount the ceph mount handle to use for performing the removexattr.
 * @param path the path to the file.
 * @param name the name of the extended attribute to remove.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_removexattr(struct ceph_mount_info *cmount, const char *path, const char *name);

/**
 * Remove an extended attribute from a file.
 *
 * @param cmount the ceph mount handle to use for performing the removexattr.
 * @param fd the open file descriptor referring to the file to remove extended attribute from.
 * @param name the name of the extended attribute to remove.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_fremovexattr(struct ceph_mount_info *cmount, int fd, const char *name);

/**
 * Remove the extended attribute from a file, do not follow symbolic links.
 *
 * @param cmount the ceph mount handle to use for performing the lremovexattr.
 * @param path the path to the file.
 * @param name the name of the extended attribute to remove.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_lremovexattr(struct ceph_mount_info *cmount, const char *path, const char *name);

/**
 * Set an extended attribute on a file.
 *
 * @param cmount the ceph mount handle to use for performing the setxattr.
 * @param path the path to the file.
 * @param name the name of the extended attribute to set.
 * @param value the bytes of the extended attribute value
 * @param size the size of the extended attribute value
 * @param flags the flags can be:
 *	CEPH_XATTR_CREATE: create the extended attribute.  Must not exist.
 *      CEPH_XATTR_REPLACE: replace the extended attribute, Must already exist.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_setxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	const void *value, size_t size, int flags);

/**
 * Set an extended attribute on a file.
 *
 * @param cmount the ceph mount handle to use for performing the setxattr.
 * @param fd the open file descriptor referring to the file to set extended attribute on.
 * @param name the name of the extended attribute to set.
 * @param value the bytes of the extended attribute value
 * @param size the size of the extended attribute value
 * @param flags the flags can be:
 *	CEPH_XATTR_CREATE: create the extended attribute.  Must not exist.
 *      CEPH_XATTR_REPLACE: replace the extended attribute, Must already exist.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_fsetxattr(struct ceph_mount_info *cmount, int fd, const char *name,
	const void *value, size_t size, int flags);

/**
 * Set an extended attribute on a file, do not follow symbolic links.
 *
 * @param cmount the ceph mount handle to use for performing the lsetxattr.
 * @param path the path to the file.
 * @param name the name of the extended attribute to set.
 * @param value the bytes of the extended attribute value
 * @param size the size of the extended attribute value
 * @param flags the flags can be:
 *	CEPH_XATTR_CREATE: create the extended attribute.  Must not exist.
 *      CEPH_XATTR_REPLACE: replace the extended attribute, Must already exist.
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_lsetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	const void *value, size_t size, int flags);

/** @} xattr */

/**
 * @defgroup libcephfs_h_filelayout Control File Layout.
 * Functions for setting and getting the file layout of existing files.
 *
 * @{
 */

/**
 * Get the file striping unit from an open file descriptor.
 *
 * @param cmount the ceph mount handle to use.
 * @param fh the open file descriptor referring to the file to get the striping unit of.
 * @returns the striping unit of the file or a negative error code on failure.
 */
int ceph_get_file_stripe_unit(struct ceph_mount_info *cmount, int fh);

/**
 * Get the file striping unit.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path of the file/directory get the striping unit of.
 * @returns the striping unit of the file or a negative error code on failure.
 */
int ceph_get_path_stripe_unit(struct ceph_mount_info *cmount, const char *path);

/**
 * Get the file striping count from an open file descriptor.
 *
 * @param cmount the ceph mount handle to use.
 * @param fh the open file descriptor referring to the file to get the striping count of.
 * @returns the striping count of the file or a negative error code on failure.
 */
int ceph_get_file_stripe_count(struct ceph_mount_info *cmount, int fh);

/**
 * Get the file striping count.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path of the file/directory get the striping count of.
 * @returns the striping count of the file or a negative error code on failure.
 */
int ceph_get_path_stripe_count(struct ceph_mount_info *cmount, const char *path);

/**
 * Get the file object size from an open file descriptor.
 *
 * @param cmount the ceph mount handle to use.
 * @param fh the open file descriptor referring to the file to get the object size of.
 * @returns the object size of the file or a negative error code on failure.
 */
int ceph_get_file_object_size(struct ceph_mount_info *cmount, int fh);

/**
 * Get the file object size.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path of the file/directory get the object size of.
 * @returns the object size of the file or a negative error code on failure.
 */
int ceph_get_path_object_size(struct ceph_mount_info *cmount, const char *path);

/**
 * Get the file pool information from an open file descriptor.
 *
 * @param cmount the ceph mount handle to use.
 * @param fh the open file descriptor referring to the file to get the pool information of.
 * @returns the ceph pool id that the file is in
 */
int ceph_get_file_pool(struct ceph_mount_info *cmount, int fh);

/**
 * Get the file pool information.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path of the file/directory get the pool information of.
 * @returns the ceph pool id that the file is in
 */
int ceph_get_path_pool(struct ceph_mount_info *cmount, const char *path);

/**
 * Get the name of the pool a opened file is stored in,
 *
 * Write the name of the file's pool to the buffer.  If buflen is 0, return
 * a suggested length for the buffer.
 *
 * @param cmount the ceph mount handle to use.
 * @param fh the open file descriptor referring to the file
 * @param buf buffer to store the name in
 * @param buflen size of the buffer
 * @returns length in bytes of the pool name, or -ERANGE if the buffer is not large enough.
 */
int ceph_get_file_pool_name(struct ceph_mount_info *cmount, int fh, char *buf, size_t buflen);

/**
 * get the name of a pool by id
 *
 * Given a pool's numeric identifier, get the pool's alphanumeric name.
 *
 * @param cmount the ceph mount handle to use
 * @param pool the numeric pool id
 * @param buf buffer to sore the name in
 * @param buflen size of the buffer
 * @returns length in bytes of the pool name, or -ERANGE if the buffer is not large enough
 */
int ceph_get_pool_name(struct ceph_mount_info *cmount, int pool, char *buf, size_t buflen);

/**
 * Get the name of the pool a file is stored in
 *
 * Write the name of the file's pool to the buffer.  If buflen is 0, return
 * a suggested length for the buffer.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path of the file/directory
 * @param buf buffer to store the name in
 * @param buflen size of the buffer
 * @returns length in bytes of the pool name, or -ERANGE if the buffer is not large enough.
 */
int ceph_get_path_pool_name(struct ceph_mount_info *cmount, const char *path, char *buf, size_t buflen);

/**
 * Get the file layout from an open file descriptor.
 *
 * @param cmount the ceph mount handle to use.
 * @param fh the open file descriptor referring to the file to get the layout of.
 * @param stripe_unit where to store the striping unit of the file
 * @param stripe_count where to store the striping count of the file
 * @param object_size where to store the object size of the file
 * @param pg_pool where to store the ceph pool id that the file is in
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_get_file_layout(struct ceph_mount_info *cmount, int fh, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool);

/**
 * Get the file layout.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path of the file/directory get the layout of.
 * @param stripe_unit where to store the striping unit of the file
 * @param stripe_count where to store the striping count of the file
 * @param object_size where to store the object size of the file
 * @param pg_pool where to store the ceph pool id that the file is in
 * @returns 0 on success or a negative error code on failure.
 */
int ceph_get_path_layout(struct ceph_mount_info *cmount, const char *path, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool);

/**
 * Get the file replication information from an open file descriptor.
 *
 * @param cmount the ceph mount handle to use.
 * @param fh the open file descriptor referring to the file to get the replication information of.
 * @returns the replication factor of the file.
 */
int ceph_get_file_replication(struct ceph_mount_info *cmount, int fh);

/**
 * Get the file replication information.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path of the file/directory get the replication information of.
 * @returns the replication factor of the file.
 */
int ceph_get_path_replication(struct ceph_mount_info *cmount, const char *path);

/**
 * Get the id of the named pool.
 *
 * @param cmount the ceph mount handle to use.
 * @param pool_name the name of the pool.
 * @returns the pool id, or a negative error code on failure.
 */
int ceph_get_pool_id(struct ceph_mount_info *cmount, const char *pool_name);

/**
 * Get the pool replication factor.
 *
 * @param cmount the ceph mount handle to use.
 * @param pool_id the pool id to look up
 * @returns the replication factor, or a negative error code on failure.
 */
int ceph_get_pool_replication(struct ceph_mount_info *cmount, int pool_id);

/**
 * Get the OSD address where the primary copy of a file stripe is located.
 *
 * @param cmount the ceph mount handle to use.
 * @param fd the open file descriptor referring to the file to get the striping unit of.
 * @param offset the offset into the file to specify the stripe.  The offset can be
 *	anywhere within the stripe unit.
 * @param addr the address of the OSD holding that stripe
 * @param naddr the capacity of the address passed in.
 * @returns the size of the addressed filled into the @e addr parameter, or a negative
 *	error code on failure.
 */
int ceph_get_file_stripe_address(struct ceph_mount_info *cmount, int fd, int64_t offset,
				 struct sockaddr_storage *addr, int naddr);

/**
 * Get the list of OSDs where the objects containing a file offset are located.
 *
 * @param cmount the ceph mount handle to use.
 * @param fd the open file descriptor referring to the file.
 * @param offset the offset within the file.
 * @param length return the number of bytes between the offset and the end of
 * the stripe unit (optional).
 * @param osds an integer array to hold the OSD ids.
 * @param nosds the size of the integer array.
 * @returns the number of items stored in the output array, or -ERANGE if the
 * array is not large enough.
 */
int ceph_get_file_extent_osds(struct ceph_mount_info *cmount, int fd,
                              int64_t offset, int64_t *length, int *osds, int nosds);

/**
 * Get the fully qualified CRUSH location of an OSD.
 *
 * Returns (type, name) string pairs for each device in the CRUSH bucket
 * hierarchy starting from the given osd to the root. Each pair element is
 * separated by a NULL character.
 *
 * @param cmount the ceph mount handle to use.
 * @param osd the OSD id.
 * @param path buffer to store location.
 * @param len size of buffer.
 * @returns the amount of bytes written into the buffer, or -ERANGE if the
 * array is not large enough.
 */
int ceph_get_osd_crush_location(struct ceph_mount_info *cmount,
    int osd, char *path, size_t len);

/**
 * Get the network address of an OSD.
 *
 * @param cmount the ceph mount handle.
 * @param osd the OSD id.
 * @param addr the OSD network address.
 * @returns zero on success, other returns a negative error code.
 */
int ceph_get_osd_addr(struct ceph_mount_info *cmount, int osd,
    struct sockaddr_storage *addr);

/**
 * Get the file layout stripe unit granularity.
 * @param cmount the ceph mount handle.
 * @returns the stripe unit granularity or a negative error code on failure.
 */
int ceph_get_stripe_unit_granularity(struct ceph_mount_info *cmount);

/** @} filelayout */

/**
 * No longer available.  Do not use.
 * These functions will return -EOPNOTSUPP.
 */
int ceph_set_default_file_stripe_unit(struct ceph_mount_info *cmount, int stripe);
int ceph_set_default_file_stripe_count(struct ceph_mount_info *cmount, int count);
int ceph_set_default_object_size(struct ceph_mount_info *cmount, int size);
int ceph_set_default_preferred_pg(struct ceph_mount_info *cmount, int osd);
int ceph_set_default_file_replication(struct ceph_mount_info *cmount, int replication);

/**
 * Read from local replicas when possible.
 *
 * @param cmount the ceph mount handle to use.
 * @param val a boolean to set (1) or clear (0) the option to favor local objects
 *     for reads.
 * @returns 0
 */
int ceph_localize_reads(struct ceph_mount_info *cmount, int val);

/**
 * Get the osd id of the local osd (if any)
 *
 * @param cmount the ceph mount handle to use.
 * @returns the osd (if any) local to the node where this call is made, otherwise
 *	-1 is returned.
 */
int ceph_get_local_osd(struct ceph_mount_info *cmount);

/** @} default_filelayout */

/**
 * Get the capabilities currently issued to the client.
 *
 * @param cmount the ceph mount handle to use.
 * @param fd the file descriptor to get issued
 * @returns the current capabilities issued to this client
 *       for the open file
 */
int ceph_debug_get_fd_caps(struct ceph_mount_info *cmount, int fd);

/**
 * Get the capabilities currently issued to the client.
 *
 * @param cmount the ceph mount handle to use.
 * @param path the path to the file
 * @returns the current capabilities issued to this client
 *       for the file
 */
int ceph_debug_get_file_caps(struct ceph_mount_info *cmount, const char *path);

/* Low Level */
struct Inode *ceph_ll_get_inode(struct ceph_mount_info *cmount,
				vinodeno_t vino);
int ceph_ll_lookup_inode(
    struct ceph_mount_info *cmount,
    struct inodeno_t ino,
    Inode **inode);

/**
 * Get the root inode of FS. Increase counter of references for root Inode. You must call ceph_ll_forget for it!
 *
 * @param cmount the ceph mount handle to use.
 * @param parent pointer to pointer to Inode struct. Pointer to root inode will be returned
 * @returns 0 if all good
 */
int ceph_ll_lookup_root(struct ceph_mount_info *cmount,
                  Inode **parent);
int ceph_ll_lookup(struct ceph_mount_info *cmount, struct Inode *parent,
		   const char *name, struct stat *attr,
		   Inode **out, int uid, int gid);
int ceph_ll_put(struct ceph_mount_info *cmount, struct Inode *in);
int ceph_ll_forget(struct ceph_mount_info *cmount, struct Inode *in,
		   int count);
int ceph_ll_walk(struct ceph_mount_info *cmount, const char *name,
		 struct Inode **i,
		 struct stat *attr);
int ceph_ll_getattr(struct ceph_mount_info *cmount, struct Inode *in,
		    struct stat *attr, int uid, int gid);
int ceph_ll_setattr(struct ceph_mount_info *cmount, struct Inode *in,
		    struct stat *st, int mask, int uid, int gid);
int ceph_ll_open(struct ceph_mount_info *cmount, struct Inode *in, int flags,
		 struct Fh **fh, int uid, int gid);
off_t ceph_ll_lseek(struct ceph_mount_info *cmount, struct Fh* filehandle,
		     off_t offset, int whence);
int ceph_ll_read(struct ceph_mount_info *cmount, struct Fh* filehandle,
		 int64_t off, uint64_t len, char* buf);
int ceph_ll_fsync(struct ceph_mount_info *cmount, struct Fh *fh,
		  int syncdataonly);
int ceph_ll_write(struct ceph_mount_info *cmount, struct Fh* filehandle,
		  int64_t off, uint64_t len, const char *data);
int64_t ceph_ll_readv(struct ceph_mount_info *cmount, struct Fh *fh,
		      const struct iovec *iov, int iovcnt, int64_t off);
int64_t ceph_ll_writev(struct ceph_mount_info *cmount, struct Fh *fh,
		       const struct iovec *iov, int iovcnt, int64_t off);
int ceph_ll_close(struct ceph_mount_info *cmount, struct Fh* filehandle);
int ceph_ll_iclose(struct ceph_mount_info *cmount, struct Inode *in, int mode);
/**
 * Get xattr value by xattr name.
 *
 * @param cmount the ceph mount handle to use.
 * @param in file handle
 * @param name name of attribute
 * @param value pointer to begin buffer
 * @param size buffer size
 * @param uid user ID
 * @param gid group ID
 * @returns size of returned buffer. Negative number in error case
 */
int ceph_ll_getxattr(struct ceph_mount_info *cmount, struct Inode *in,
		     const char *name, void *value, size_t size, int uid,
		     int gid);
int ceph_ll_setxattr(struct ceph_mount_info *cmount, struct Inode *in,
		     const char *name, const void *value, size_t size,
		     int flags, int uid, int gid);
int ceph_ll_listxattr(struct ceph_mount_info *cmount, struct Inode *in,
                      char *list, size_t buf_size, size_t *list_size, int uid, int gid);
int ceph_ll_removexattr(struct ceph_mount_info *cmount, struct Inode *in,
			const char *name, int uid, int gid);
int ceph_ll_create(struct ceph_mount_info *cmount, struct Inode *parent,
		   const char *name, mode_t mode, int flags,
		   struct stat *attr, struct Inode **out, Fh **fhp,
		   int uid, int gid);
int ceph_ll_mkdir(struct ceph_mount_info *cmount, struct Inode *parent,
		  const char *name, mode_t mode, struct stat *attr,
		  Inode **out, int uid, int gid);
int ceph_ll_link(struct ceph_mount_info *cmount, struct Inode *in,
		 struct Inode *newparrent, const char *name,
		 struct stat *attr, int uid, int gid);
int ceph_ll_truncate(struct ceph_mount_info *cmount, struct Inode *in,
		     uint64_t length, int uid, int gid);
int ceph_ll_opendir(struct ceph_mount_info *cmount, struct Inode *in,
		    struct ceph_dir_result **dirpp, int uid, int gid);
int ceph_ll_releasedir(struct ceph_mount_info *cmount,
		       struct ceph_dir_result* dir);
int ceph_ll_rename(struct ceph_mount_info *cmount, struct Inode *parent,
		   const char *name, struct Inode *newparent,
		   const char *newname, int uid, int gid);
int ceph_ll_unlink(struct ceph_mount_info *cmount, struct Inode *in,
		   const char *name, int uid, int gid);
int ceph_ll_statfs(struct ceph_mount_info *cmount, struct Inode *in,
		   struct statvfs *stbuf);
int ceph_ll_readlink(struct ceph_mount_info *cmount, struct Inode *in,
		     char *buf, size_t bufsize, int uid, int gid);
int ceph_ll_symlink(struct ceph_mount_info *cmount, struct Inode *parent,
		    const char *name, const char *value, struct stat *attr,
		    struct Inode **in, int uid, int gid);
int ceph_ll_rmdir(struct ceph_mount_info *cmount, struct Inode *in,
		  const char *name, int uid, int gid);
uint32_t ceph_ll_stripe_unit(struct ceph_mount_info *cmount,
			     struct Inode *in);
uint32_t ceph_ll_file_layout(struct ceph_mount_info *cmount,
			     struct Inode *in,
			     struct ceph_file_layout *layout);
uint64_t ceph_ll_snap_seq(struct ceph_mount_info *cmount,
			  struct Inode *in);
int ceph_ll_get_stripe_osd(struct ceph_mount_info *cmount,
			   struct Inode *in,
			   uint64_t blockno,
			   struct ceph_file_layout* layout);
int ceph_ll_num_osds(struct ceph_mount_info *cmount);
int ceph_ll_osdaddr(struct ceph_mount_info *cmount,
		    int osd, uint32_t *addr);
uint64_t ceph_ll_get_internal_offset(struct ceph_mount_info *cmount,
				     struct Inode *in, uint64_t blockno);
int ceph_ll_read_block(struct ceph_mount_info *cmount,
		       struct Inode *in, uint64_t blockid,
		       char* bl, uint64_t offset, uint64_t length,
		       struct ceph_file_layout* layout);
int ceph_ll_write_block(struct ceph_mount_info *cmount,
			struct Inode *in, uint64_t blockid,
			char* buf, uint64_t offset,
			uint64_t length, struct ceph_file_layout* layout,
			uint64_t snapseq, uint32_t sync);
int ceph_ll_commit_blocks(struct ceph_mount_info *cmount,
			  struct Inode *in, uint64_t offset, uint64_t range);


#ifdef __cplusplus
}
#endif

#endif
