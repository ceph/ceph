// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_HASHINDEX_H
#define CEPH_HASHINDEX_H

#include "include/buffer.h"
#include "include/encoding.h"
#include "LFNIndex.h"


/**
 * Implements collection prehashing.
 *
 * @verbatim
 *     (root) - 0 - 0
 *                - 1
 *                - E
 *            - 1
 *            - 2 - D - 0
 *            .
 *            .
 *            .
 *            - F - 0
 * @endverbatim
 *
 * A file is located at the longest existing directory from the root 
 * given by the hex characters in the hash beginning with the least
 * significant.
 * 
 * ex: ghobject_t("object", CEPH_NO_SNAP, 0xA4CEE0D2)
 * would be located in (root)/2/D/0/
 * 
 * Subdirectories are created when the number of objects in a directory
 * exceed 32*merge_threshhold.  The number of objects in a directory 
 * is encoded as subdir_info_s in an xattr on the directory.
 */
class HashIndex : public LFNIndex {
private:
  /// Attribute name for storing subdir info @see subdir_info_s
  static const string SUBDIR_ATTR;
  /// Attribute name for storing in progress op tag
  static const string IN_PROGRESS_OP_TAG;
  /// Size (bits) in object hash
  static const int PATH_HASH_LEN = 32;
  /// Max length of hashed path
  static const int MAX_HASH_LEVEL = (PATH_HASH_LEN/4);

  /**
   * Merges occur when the number of object drops below
   * merge_threshold and splits occur when the number of objects
   * exceeds 16 * merge_threshold * split_multiplier.
   */
  int merge_threshold;
  int split_multiplier;

  /// Encodes current subdir state for determining when to split/merge.
  struct subdir_info_s {
    uint64_t objs;       ///< Objects in subdir.
    uint32_t subdirs;    ///< Subdirs in subdir.
    uint32_t hash_level; ///< Hashlevel of subdir.

    subdir_info_s() : objs(0), subdirs(0), hash_level(0) {}
    
    void encode(bufferlist &bl) const
    {
      __u8 v = 1;
      ::encode(v, bl);
      ::encode(objs, bl);
      ::encode(subdirs, bl);
      ::encode(hash_level, bl);
    }
    
    void decode(bufferlist::iterator &bl)
    {
      __u8 v;
      ::decode(v, bl);
      assert(v == 1);
      ::decode(objs, bl);
      ::decode(subdirs, bl);
      ::decode(hash_level, bl);
    }
  };

  /// Encodes in progress split or merge
  struct InProgressOp {
    static const int SPLIT = 0;
    static const int MERGE = 1;
    static const int COL_SPLIT = 2;
    int op;
    vector<string> path;

    InProgressOp(int op, const vector<string> &path) 
      : op(op), path(path) {}

    InProgressOp(bufferlist::iterator &bl) {
      decode(bl);
    }

    bool is_split() const { return op == SPLIT; }
    bool is_col_split() const { return op == COL_SPLIT; }
    bool is_merge() const { return op == MERGE; }

    void encode(bufferlist &bl) const {
      __u8 v = 1;
      ::encode(v, bl);
      ::encode(op, bl);
      ::encode(path, bl);
    }

    void decode(bufferlist::iterator &bl) {
      __u8 v;
      ::decode(v, bl);
      assert(v == 1);
      ::decode(op, bl);
      ::decode(path, bl);
    }
  };
    
    
public:
  /// Constructor.
  HashIndex(
    coll_t collection,     ///< [in] Collection
    const char *base_path, ///< [in] Path to the index root.
    int merge_at,          ///< [in] Merge threshhold.
    int split_multiple,	   ///< [in] Split threshhold.
    uint32_t index_version,///< [in] Index version
    double retry_probability=0) ///< [in] retry probability
    : LFNIndex(collection, base_path, index_version, retry_probability),
      merge_threshold(merge_at),
      split_multiplier(split_multiple) {}

  /// @see CollectionIndex
  uint32_t collection_version() { return index_version; }

  /// @see CollectionIndex
  int cleanup();

  /// @see CollectionIndex
  int prep_delete();

  /// @see CollectionIndex
  int _split(
    uint32_t match,
    uint32_t bits,
    ceph::shared_ptr<CollectionIndex> dest
    );
	
protected:
  int _init();

  int _created(
    const vector<string> &path,
    const ghobject_t &oid,
    const string &mangled_name
    );
  int _remove(
    const vector<string> &path,
    const ghobject_t &oid,
    const string &mangled_name
    );
  int _lookup(
    const ghobject_t &oid,
    vector<string> *path,
    string *mangled_name,
    int *exists
    );
  int _collection_list(
    vector<ghobject_t> *ls
    );
  int _collection_list_partial(
    const ghobject_t &start,
    int min_count,
    int max_count,
    snapid_t seq,
    vector<ghobject_t> *ls,
    ghobject_t *next
    );
private:
  /// Recursively remove path and its subdirs
  int recursive_remove(
    const vector<string> &path ///< [in] path to remove
    ); /// @return Error Code, 0 on success
  /// Tag root directory at beginning of col_split
  int start_col_split(
    const vector<string> &path ///< [in] path to split
    ); ///< @return Error Code, 0 on success
  /// Tag root directory at beginning of split
  int start_split(
    const vector<string> &path ///< [in] path to split
    ); ///< @return Error Code, 0 on success
  /// Tag root directory at beginning of split
  int start_merge(
    const vector<string> &path ///< [in] path to merge
    ); ///< @return Error Code, 0 on success
  /// Remove tag at end of split or merge
  int end_split_or_merge(
    const vector<string> &path ///< [in] path to split or merged
    ); ///< @return Error Code, 0 on success
  /// Gets info from the xattr on the subdir represented by path
  int get_info(
    const vector<string> &path, ///< [in] Path from which to read attribute.
    subdir_info_s *info		///< [out] Attribute value
    ); /// @return Error Code, 0 on success

  /// Sets info to the xattr on the subdir represented by path
  int set_info(
    const vector<string> &path, ///< [in] Path on which to set attribute.
    const subdir_info_s &info  	///< [in] Value to set
    ); /// @return Error Code, 0 on success

  /// Encapsulates logic for when to split.
  bool must_merge(
    const subdir_info_s &info ///< [in] Info to check
    ); /// @return True if info must be merged, False otherwise

  /// Encapsulates logic for when to merge.
  bool must_split(
    const subdir_info_s &info ///< [in] Info to check
    ); /// @return True if info must be split, False otherwise

  /// Initiates merge
  int initiate_merge(
    const vector<string> &path, ///< [in] Subdir to merge
    subdir_info_s info		///< [in] Info attached to path
    ); /// @return Error Code, 0 on success

  /// Completes merge
  int complete_merge(
    const vector<string> &path, ///< [in] Subdir to merge
    subdir_info_s info		///< [in] Info attached to path
    ); /// @return Error Code, 0 on success

  /// Resets attr to match actual subdir contents
  int reset_attr(
    const vector<string> &path ///< [in] path to cleanup
    );

  /// Initiate Split
  int initiate_split(
    const vector<string> &path, ///< [in] Subdir to split
    subdir_info_s info		///< [in] Info attached to path
    ); /// @return Error Code, 0 on success

  /// Completes Split
  int complete_split(
    const vector<string> &path, ///< [in] Subdir to split
    subdir_info_s info	       ///< [in] Info attached to path
    ); /// @return Error Code, 0 on success

  /// Determine path components from hoid hash
  void get_path_components(
    const ghobject_t &oid, ///< [in] Object for which to get path components
    vector<string> *path   ///< [out] Path components for hoid.
    );

  /// do collection split for path
  static int col_split_level(
    HashIndex &from,            ///< [in] from index
    HashIndex &dest,            ///< [in] to index
    const vector<string> &path, ///< [in] path to split
    uint32_t bits,              ///< [in] num bits to match
    uint32_t match,             ///< [in] bits to match
    unsigned *mkdirred          ///< [in,out] path[:mkdirred] has been mkdirred
    );
    

  /** 
   * Get string representation of ghobject_t/hash
   *
   * e.g: 0x01234567 -> "76543210"
   */
  static string get_path_str(
    const ghobject_t &oid ///< [in] Object to get hash string for
    ); ///< @return Hash string for hoid.

  /// Get string from hash, @see get_path_str
  static string get_hash_str(
    uint32_t hash ///< [in] Hash to convert to a string.
    ); ///< @return String representation of hash

  /// Get hash from hash prefix string e.g. "FFFFAB" -> 0xFFFFAB00
  static uint32_t hash_prefix_to_hash(
    string prefix ///< [in] string to convert
    ); ///< @return Hash

  /// Get hash mod from path
  static void path_to_hobject_hash_prefix(
    const vector<string> &path,///< [in] path to convert
    uint32_t *bits,            ///< [out] bits
    uint32_t *hash             ///< [out] hash
    ) {
    string hash_str;
    for (vector<string>::const_iterator i = path.begin();
	 i != path.end();
	 ++i) {
      hash_str.push_back(*i->begin());
    }
    uint32_t rev_hash = hash_prefix_to_hash(hash_str);
    if (hash)
      *hash = rev_hash;
    if (bits)
      *bits = path.size() * 4;
  }

  /// Get path contents by hash
  int get_path_contents_by_hash(
    const vector<string> &path,            /// [in] Path to list
    const string *lower_bound,             /// [in] list > *lower_bound
    const ghobject_t *next_object,          /// [in] list > *next_object
    const snapid_t *seq,                   /// [in] list >= *seq
    set<string> *hash_prefixes,            /// [out] prefixes in dir
    set<pair<string, ghobject_t> > *objects /// [out] objects
    );

  /// List objects in collection in ghobject_t order
  int list_by_hash(
    const vector<string> &path, /// [in] Path to list
    int min_count,              /// [in] List at least min_count
    int max_count,              /// [in] List at most max_count
    snapid_t seq,               /// [in] list only objects where snap >= seq
    ghobject_t *next,            /// [in,out] List objects >= *next
    vector<ghobject_t> *out      /// [out] Listed objects
    ); ///< @return Error Code, 0 on success
};

#endif
