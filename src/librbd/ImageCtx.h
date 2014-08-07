// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include "include/int_types.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/snap_types.h"
#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"
#include "include/types.h"
#include "osdc/ObjectCacher.h"

#include "cls/rbd/cls_rbd_client.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/SnapInfo.h"
#include "librbd/parent_types.h"

class CephContext;
class PerfCounters;

namespace librbd {

  class WatchCtx;

  struct ImageCtx {
    CephContext *cct;
    PerfCounters *perfcounter;
    struct rbd_obj_header_ondisk header;
    ::SnapContext snapc;
    std::vector<librados::snap_t> snaps; // this mirrors snapc.snaps, but is in
                                        // a format librados can understand
    std::map<std::string, SnapInfo> snaps_by_name;
    uint64_t snap_id;
    bool snap_exists; // false if our snap_id was deleted
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;
    bool flush_encountered;

    std::map<rados::cls::lock::locker_id_t,
	     rados::cls::lock::locker_info_t> lockers;
    bool exclusive_locked;
    std::string lock_tag;

    std::string name;
    std::string snap_name;
    IoCtx data_ctx, md_ctx;
    WatchCtx *wctx;
    int refresh_seq;    ///< sequence for refresh requests
    int last_refresh;   ///< last completed refresh

    /**
     * Lock ordering:
     * md_lock, cache_lock, snap_lock, parent_lock, refresh_lock
     */
    RWLock md_lock; // protects access to the mutable image metadata that
                   // isn't guarded by other locks below
                   // (size, features, image locks, etc)
    Mutex cache_lock; // used as client_lock for the ObjectCacher
    RWLock snap_lock; // protects snapshot-related member variables:
    RWLock parent_lock; // protects parent_md and parent
    Mutex refresh_lock; // protects refresh_seq and last_refresh

    unsigned extra_read_flags;

    bool old_format;
    uint8_t order;
    uint64_t size;
    uint64_t features;
    std::string object_prefix;
    char *format_string;
    std::string header_oid;
    std::string id; // only used for new-format images
    parent_info parent_md;
    ImageCtx *parent;
    uint64_t stripe_unit, stripe_count;
    uint64_t flags;

    ceph_file_layout layout;

    ObjectCacher *object_cacher;
    LibrbdWriteback *writeback_handler;
    ObjectCacher::ObjectSet *object_set;

    /**
     * Either image_name or image_id must be set.
     * If id is not known, pass the empty std::string,
     * and init() will look it up.
     */
    ImageCtx(const std::string &image_name, const std::string &image_id,
	     const char *snap, IoCtx& p, bool read_only);
    ~ImageCtx();
    int init();
    void init_layout();
    void perf_start(std::string name);
    void perf_stop();
    void set_read_flag(unsigned flag);
    int get_read_flags(librados::snap_t snap_id);
    int snap_set(std::string in_snap_name);
    void snap_unset();
    librados::snap_t get_snap_id(std::string in_snap_name) const;
    int get_snap_name(snapid_t snap_id, std::string *out_snap_name) const;
    int get_parent_spec(snapid_t snap_id, parent_spec *pspec);
    int is_snap_protected(string in_snap_name, bool *is_protected) const;
    int is_snap_unprotected(string in_snap_name, bool *is_unprotected) const;

    uint64_t get_current_size() const;
    uint64_t get_object_size() const;
    string get_object_name(uint64_t num) const;
    uint64_t get_num_objects() const;
    uint64_t get_stripe_unit() const;
    uint64_t get_stripe_count() const;
    uint64_t get_stripe_period() const;

    void add_snap(std::string in_snap_name, librados::snap_t id,
		  uint64_t in_size, uint64_t features,
		  parent_info parent, uint8_t protection_status);
    uint64_t get_image_size(librados::snap_t in_snap_id) const;
    int get_features(librados::snap_t in_snap_id,
		     uint64_t *out_features) const;
    int64_t get_parent_pool_id(librados::snap_t in_snap_id) const;
    std::string get_parent_image_id(librados::snap_t in_snap_id) const;
    uint64_t get_parent_snap_id(librados::snap_t in_snap_id) const;
    int get_parent_overlap(librados::snap_t in_snap_id,
			   uint64_t *overlap) const;
    void aio_read_from_cache(object_t o, bufferlist *bl, size_t len,
			     uint64_t off, Context *onfinish);
    void write_to_cache(object_t o, bufferlist& bl, size_t len, uint64_t off,
			Context *onfinish);
    int read_from_cache(object_t o, bufferlist *bl, size_t len, uint64_t off);
    void user_flushed();
    void flush_cache_aio(Context *onfinish);
    int flush_cache();
    void shutdown_cache();
    int invalidate_cache();
    void clear_nonexistence_cache();
    int register_watch();
    void unregister_watch();
    size_t parent_io_len(uint64_t offset, size_t length,
			 librados::snap_t in_snap_id);
    uint64_t prune_parent_extents(vector<pair<uint64_t,uint64_t> >& objectx,
				  uint64_t overlap);


    // -- ImageIndex related --
    //
    // ImageIndex is aimed to hold each object's location info which avoid
    // extra checking for none-existing object. It's only used when image flags
    // exists LIBRBD_CREATE_SHARED. Otherwise, ImageIndex will become gawp and
    // has no effect.
    //
    // Each object has three state:
    // 1. UNKNOWN: default value, it will follow origin path
    // 2. LOCAL: imply this object is local, don't need to lookup parent image
    // 3. PARENT: imply this object is in the parent image, don't need to read
    // from local image
    //
    // Note: ImageIndex isn't full sync to real data all the time. Because the
    // transformation {"unknown" -> "local", "unknown" -> "parent"} are safe.
    // So We only need to handle with the exception when ImageIndex implies
    // this object is "parent" but the real data is "local". There exists three
    // methods to solve it:
    // 1. flush `state_map` every time when "parent" -> "local" happened
    // 2. as we know, parent image is frozen and never change `state_map`, so
    // we will read current image object all the time in sprint of the object
    // state and trust the parent image's state. In other word, when opening
    // the *current* image, all "parent" objects will be transformed to
    // "unknown".
    //
    // Here choose to implement method 2. This method only allow 2 read ops
    // in one read request at max and without overhead.
    //
    // 1. When creating new image, it will mark all objects as "local"
    // 2. When clone from image, it will mark all objects as "parent"
    // 3. When write(or modified op) a object, it will mark this object as
    // "local"
    // 4. When creating snapshot, image index will be freeze and save it as the
    // index of the snapshot
    // 5. When reading object, the current image object will be always read in
    // spite of the state. And the parent image's object will be checked and
    // trust the state.

    class ImageIndex {
    private:
      static const int OBJ_UNKNOWN = 0;
      static const int OBJ_PARENT = 1;
      static const int OBJ_LOCAL = 2;

      Mutex state_lock;   // protects image_index
      vector<int> state_map;
      uint64_t num_obj;
      bool enable;

      ImageCtx *image;
      CephContext *cct;

    public:
      ImageIndex(ImageCtx *image): state_lock("librbd::ImageIndex::state_lock"),
                                   enable(false), image(image),
                                   cct(image->cct) {

      }
      ~ImageIndex() {}
      int load();
      void encode(bufferlist &bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(state_map, bl);
        ::encode(num_obj, bl);
        ENCODE_FINISH(bl);
      }
      void decode(bufferlist::iterator &bl) {
        DECODE_START(1, bl);
        ::decode(state_map, bl);
        ::decode(num_obj, bl);
        DECODE_FINISH(bl);
      }

      void invalidate_image_index() {
        Mutex::Locker l(state_lock);
        for (uint64_t i = 0; i < num_obj; ++i)
          state_map[i] = OBJ_UNKNOWN;
      }

      void resize_image_index(uint64_t size) {
        Mutex::Locker l(state_lock);
        uint64_t origin = num_obj;
        state_map.resize(size);
        num_obj = size;
        if (!enable)
          return ;

        for (uint64_t i = origin; i < size; i++) {
          state_map[i] = OBJ_UNKNOWN;
        }
      }

      bool is_full_local() {
        Mutex::Locker l(state_lock);
        if (!enable)
          return true;

        for (uint64_t i = 0; i < num_obj; ++i)
          if (state_map[i] != OBJ_LOCAL)
            return false;

        return true;
      }

      bool is_unknown(uint64_t objno) {
        assert(objno < num_obj);
        Mutex::Locker l(state_lock);
        return state_map[objno] == OBJ_UNKNOWN;
      }
      bool is_local(uint64_t objno) {
        assert(objno < num_obj);
        Mutex::Locker l(state_lock);
        return state_map[objno] == OBJ_LOCAL;
      }
      bool is_parent(uint64_t objno) {
        assert(objno < num_obj);
        Mutex::Locker l(state_lock);
        return state_map[objno] == OBJ_PARENT;
      }

      void mark_all_parent() {
        Mutex::Locker l(state_lock);
        if (!enable)
          return;

        for (uint64_t i = 0; i < num_obj; ++i)
          state_map[i] = OBJ_PARENT;
      }
      void mark_all_local() {
        Mutex::Locker l(state_lock);
        if (!enable)
          return;

        for (uint64_t i = 0; i < num_obj; ++i)
          state_map[i] = OBJ_LOCAL;
      }
      void mark_local(uint64_t objno) {
        assert(objno < num_obj);
        Mutex::Locker l(state_lock);
        if (enable)
          state_map[objno] = OBJ_LOCAL;
      }
      void mark_parent(uint64_t objno) {
        assert(objno < num_obj);
        Mutex::Locker l(state_lock);
        if (enable)
          state_map[objno] = OBJ_PARENT;
      }
    } image_index;
    WRITE_CLASS_ENCODER(ImageIndex)

  };
}

#endif
