// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "MDSUtility.h"
#include "include/rados/librados.hpp"

class InodeStore;
class MDSTable;

class RecoveryDriver {
  protected:
    // If true, overwrite structures that generate decoding errors.
    bool force_corrupt;

    // If true, overwrite root objects during init_roots even if they
    // exist
    bool force_init;

  public:
    virtual int init(
        librados::Rados &rados,
	std::string &metadata_pool_name,
        const FSMap *fsmap,
        fs_cluster_id_t fscid) = 0;

    void set_force_corrupt(const bool val)
    {
      force_corrupt = val;
    }

    void set_force_init(const bool val)
    {
      force_init = val;
    }


    /**
     * Inject an inode + dentry parents into the metadata pool,
     * based on a backtrace recovered from the data pool
     */
    virtual int inject_with_backtrace(
        const inode_backtrace_t &bt,
        const InodeStore &dentry) = 0;

    /**
     * Inject an inode + dentry into the lost+found directory,
     * when all we know about a file is its inode.
     */
    virtual int inject_lost_and_found(
        inodeno_t ino,
        const InodeStore &dentry) = 0;

    /**
     * Create any missing roots (i.e. mydir, strays, root inode)
     */
    virtual int init_roots(
        int64_t data_pool_id) = 0;

    /**
     * Pre-injection check that all the roots are present in
     * the metadata pool.  Used to avoid parallel workers interfering
     * with one another, by cueing the user to go run 'init' on a
     * single node before running a parallel scan.
     *
     * @param result: set to true if roots are present, else set to false
     * @returns 0 on no unexpected errors, else error code.  Missing objects
     *          are not considered an unexpected error: check *result for
     *          this case.
     */
    virtual int check_roots(bool *result) = 0;

    /**
     * Helper to compose dnames for links to lost+found
     * inodes.
     */
    std::string lost_found_dname(inodeno_t ino)
    {
      char s[20];
      snprintf(s, sizeof(s), "%llx", (unsigned long long)ino);
      return std::string(s);
    }

    RecoveryDriver()
      : force_corrupt(false),
	force_init(false)
    {}

    virtual ~RecoveryDriver() {}
};

class LocalFileDriver : public RecoveryDriver
{ 
  protected:
    const std::string path;
    librados::IoCtx &data_io;

  int inject_data(
      const std::string &file_path,
      uint64_t size,
      uint32_t chunk_size,
      inodeno_t ino);
  public:

    LocalFileDriver(const std::string &path_, librados::IoCtx &data_io_)
      : RecoveryDriver(), path(path_), data_io(data_io_)
    {}

    // Implement RecoveryDriver interface
    int init(
        librados::Rados &rados,
	std::string &metadata_pool_name,
        const FSMap *fsmap,
        fs_cluster_id_t fscid) override;

    int inject_with_backtrace(
        const inode_backtrace_t &bt,
        const InodeStore &dentry) override;

    int inject_lost_and_found(
        inodeno_t ino,
        const InodeStore &dentry) override;

    int init_roots(int64_t data_pool_id) override;

    int check_roots(bool *result) override;
};

/**
 * A class that knows how to work with objects in a CephFS
 * metadata pool.
 */
class MetadataTool
{
  protected:

  librados::IoCtx metadata_io;

  /**
   * Construct a synthetic InodeStore for a normal file
   */
  void build_file_dentry(
    inodeno_t ino, uint64_t file_size, time_t file_mtime,
    const file_layout_t &layout,
    InodeStore *out,
    std::string symlink);

  /**
   * Construct a synthetic InodeStore for a directory
   */
  void build_dir_dentry(
    inodeno_t ino,
    const frag_info_t &fragstat,
    const file_layout_t &layout,
    InodeStore *out);

  /**
   * Try and read an fnode from a dirfrag
   */
  int read_fnode(inodeno_t ino, frag_t frag,
                 fnode_t *fnode, uint64_t *read_version);

  /**
   * Try and read a dentry from a dirfrag
   */
  int read_dentry(inodeno_t parent_ino, frag_t frag,
		  const std::string &dname, InodeStore *inode, snapid_t *dnfirst=nullptr);
};

/**
 * A class that knows how to manipulate CephFS metadata pools
 */
class MetadataDriver : public RecoveryDriver, public MetadataTool
{
  protected:
    /**
     * Create a .inode object, i.e. root or mydir
     */
    int inject_unlinked_inode(inodeno_t inono, int mode, int64_t data_pool_id);

    /**
     * Check for existence of .inode objects, before
     * trying to go ahead and inject metadata.
     */
    int root_exists(inodeno_t ino, bool *result);
    int find_or_create_dirfrag(
        inodeno_t ino,
        frag_t fragment,
        bool *created);


    /**
     * Work out which fragment of a directory should contain a named
     * dentry, recursing up the trace as necessary to retrieve
     * fragtrees.
     */
    int get_frag_of(
        inodeno_t dirino,
        const std::string &dname,
        frag_t *result_ft);

  public:

    // Implement RecoveryDriver interface
    int init(
        librados::Rados &rados,
	std::string &metadata_pool_name,
        const FSMap *fsmap,
        fs_cluster_id_t fscid) override;

    int inject_linkage(
        inodeno_t dir_ino, const std::string &dname,
        const frag_t fragment, const InodeStore &inode, snapid_t dnfirst=CEPH_NOSNAP);

    int inject_with_backtrace(
        const inode_backtrace_t &bt,
        const InodeStore &dentry) override;

    int inject_lost_and_found(
        inodeno_t ino,
        const InodeStore &dentry) override;

    int init_roots(int64_t data_pool_id) override;

    int check_roots(bool *result) override;

    int load_table(MDSTable *table);
    int save_table(MDSTable *table);
};

class DataScan : public MDSUtility, public MetadataTool
{
  protected:
    RecoveryDriver *driver;
    fs_cluster_id_t fscid;

    std::string metadata_pool_name;
    std::vector<int64_t> data_pools;

    // IoCtx for data pool (where we scrape file backtraces from)
    librados::IoCtx data_io;
    // Remember the data pool ID for use in layouts
    int64_t data_pool_id;
    // IoCtxs for extra data pools
    std::vector<librados::IoCtx> extra_data_ios;

    uint32_t n;
    uint32_t m;

    /**
     * Scan data pool for backtraces, and inject inodes to metadata pool
     */
    int scan_inodes();

    /**
     * Scan data pool for file sizes and mtimes
     */
    int scan_extents();

    /**
     * Scan metadata pool for 0th dirfrags to link orphaned
     * directory inodes.
     */
    int scan_frags();

    /**
     * Cleanup xattrs from data pool
     */
    int cleanup();

    /**
     * Check if an inode number is in the permitted ranges
     */
    bool valid_ino(inodeno_t ino) const;


    int scan_links();

    // Accept pools which are not in the FSMap
    bool force_pool;
    // Respond to decode errors by overwriting
    bool force_corrupt;
    // Overwrite root objects even if they exist
    bool force_init;
    // Only scan inodes without this scrub tag
    std::string filter_tag;

    /**
     * @param r set to error on valid key with invalid value
     * @return true if argument consumed, else false
     */
    bool parse_kwarg(
        const std::vector<const char*> &args,
        std::vector<const char *>::const_iterator &i,
        int *r);

    /**
     * @return true if argument consumed, else false
     */
    bool parse_arg(
      const std::vector<const char*> &arg,
      std::vector<const char *>::const_iterator &i);

    int probe_filter(librados::IoCtx &ioctx);

    /**
     * Apply a function to all objects in an ioctx's pool, optionally
     * restricted to only those objects with a 00000000 offset and
     * no tag matching DataScan::scrub_tag.
     */
    int forall_objects(
        librados::IoCtx &ioctx,
        bool untagged_only,
        std::function<int(std::string, uint64_t, uint64_t)> handler);

  public:
    static void usage();
    int main(const std::vector<const char *> &args);

    DataScan()
      : driver(NULL), fscid(FS_CLUSTER_ID_NONE),
	data_pool_id(-1), n(0), m(1),
        force_pool(false), force_corrupt(false),
        force_init(false)
    {
    }

    ~DataScan() override
    {
      delete driver;
    }
};

