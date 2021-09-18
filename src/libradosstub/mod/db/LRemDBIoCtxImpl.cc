// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemDBIoCtxImpl.h"
#include "LRemDBRadosClient.h"
#include "common/Clock.h"
#include "include/err.h"
#include <functional>
#include <boost/algorithm/string/predicate.hpp>
#include <errno.h>
#include <include/compat.h>

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "LRemDBIoCtxImpl: " << this << " " << __func__ \
                           << ": " << oid << " "

static void to_vector(const interval_set<uint64_t> &set,
                      std::vector<std::pair<uint64_t, uint64_t> > *vec) {
  vec->clear();
  for (interval_set<uint64_t>::const_iterator it = set.begin();
      it != set.end(); ++it) {
    vec->push_back(*it);
  }
}

// see PrimaryLogPG::finish_extent_cmp()
static int cmpext_compare(const bufferlist &bl, const bufferlist &read_bl) {
  for (uint64_t idx = 0; idx < bl.length(); ++idx) {
    char read_byte = (idx < read_bl.length() ? read_bl[idx] : 0);
    if (bl[idx] != read_byte) {
      return -MAX_ERRNO - idx;
    }
  }
  return 0;
}

namespace librados {

LRemDBIoCtxImpl::LRemDBIoCtxImpl() {
}

LRemDBIoCtxImpl::LRemDBIoCtxImpl(const LRemDBIoCtxImpl& rhs)
    : LRemIoCtxImpl(rhs), m_client(rhs.m_client), m_pool(rhs.m_pool) {
  auto uuid = m_client->cct()->_conf.get_val<uuid_d>("fsid");
  m_dbc = std::make_shared<LRemDBStore::Cluster>(uuid.to_string());
  m_dbc->get_pool(m_pool_name, &m_pool_db);

}

LRemDBIoCtxImpl::LRemDBIoCtxImpl(LRemDBRadosClient *client, int64_t pool_id,
                                   const std::string& pool_name,
                                   LRemDBCluster::PoolRef pool)
    : LRemIoCtxImpl(client, pool_id, pool_name), m_client(client),
      m_pool(pool) {
  auto uuid = client->cct()->_conf.get_val<uuid_d>("fsid");
  m_dbc = std::make_shared<LRemDBStore::Cluster>(uuid.to_string());
  m_dbc->get_pool(pool_name, &m_pool_db);

#warning check all ops for m_pool_db not null
}

LRemDBIoCtxImpl::~LRemDBIoCtxImpl() {
}

LRemIoCtxImpl *LRemDBIoCtxImpl::clone() {
  return new LRemDBIoCtxImpl(*this);
}

int LRemDBIoCtxImpl::aio_append(LRemTransactionStateRef& trans, AioCompletionImpl *c,
                                 const bufferlist& bl, size_t len) {
  bufferlist newbl;
  newbl.substr_of(bl, 0, len);
  m_client->add_aio_operation(trans->oid(), true,
                              std::bind(&LRemDBIoCtxImpl::append, this, trans,
                                        newbl,
					get_snap_context()),
                              c);
  return 0;
}

int LRemDBIoCtxImpl::aio_remove(const std::string& oid, AioCompletionImpl *c, int flags) {
  m_client->add_aio_operation(oid, true,
                              std::bind(&LRemDBIoCtxImpl::remove, this, oid,
					get_snap_context()),
                              c);
  return 0;
}

int LRemDBIoCtxImpl::append(LRemTransactionStateRef& trans, const bufferlist &bl,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();
  ldout(cct, 20) << "length=" << bl.length() << ", snapc=" << snapc << dendl;

  uint64_t epoch;

  ObjFileRef file;
  file = get_file_safe(trans, true, CEPH_NOSNAP, snapc, &epoch);

  auto objh = m_pool_db->get_obj_handler(trans->nspace(), trans->oid());

  std::unique_lock l{*file->lock};

  int r = objh->append(bl, epoch);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBIoCtxImpl::assert_exists(const std::string &oid, uint64_t snap_id) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::shared_lock l{m_pool->file_lock};
  ObjFileRef file = get_file(oid, false, snap_id, {});
  if (file == NULL) {
    return -ENOENT;
  }
  return 0;
}

int LRemDBIoCtxImpl::assert_version(const std::string &oid, uint64_t ver) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::shared_lock l{m_pool->file_lock};
  ObjFileRef file = get_file(oid, false, CEPH_NOSNAP, {});
  if (file == NULL || !file->exists) {
    return -ENOENT;
  }
  if (ver < file->meta.objver) {
    return -ERANGE;
  }
  if (ver > file->meta.objver) {
    return -EOVERFLOW;
  }

  return 0;
}

int LRemDBIoCtxImpl::create(const std::string& oid, bool exclusive,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  ldout(cct, 20) << "snapc=" << snapc << dendl;

  std::unique_lock l{m_pool->file_lock};
  ObjFileRef file = get_file(oid, false, CEPH_NOSNAP, {});
  bool exists = (file != NULL && file->exists);
  if (exists) {
    return (exclusive ? -EEXIST : 0);
  }

  auto new_file = get_file(oid, true, CEPH_NOSNAP, snapc);
  new_file->meta.epoch = ++m_pool->epoch;

  int r = new_file->obj->write_meta(new_file->meta);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBIoCtxImpl::list_snaps(const std::string& oid, snap_set_t *out_snaps) {
#warning implement me
#if 0
  auto cct = m_client->cct();
  ldout(cct, 20) << dendl;

  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  out_snaps->seq = 0;
  out_snaps->clones.clear();

  std::shared_lock l{m_pool->file_lock};
  LRemDBCluster::Files::iterator it = m_pool->files.find(
    {get_namespace(), oid});
  if (it == m_pool->files.end()) {
    return -ENOENT;
  }

  bool include_head = false;
  LRemDBCluster::FileSnapshots &file_snaps = it->second;
  for (LRemDBCluster::FileSnapshots::iterator s_it = file_snaps.begin();
       s_it != file_snaps.end(); ++s_it) {
    LRemDBCluster::File &file = *s_it->get();

    if (file_snaps.size() > 1) {
      out_snaps->seq = file.snap_id;
      LRemDBCluster::FileSnapshots::iterator next_it(s_it);
      ++next_it;
      if (next_it == file_snaps.end()) {
        include_head = true;
        break;
      }

      ++out_snaps->seq;
      if (!file.exists) {
        continue;
      }

      // update the overlap with the next version's overlap metadata
      LRemDBCluster::File &next_file = *next_it->get();
      interval_set<uint64_t> overlap;
      if (next_file.exists) {
        overlap = next_file.snap_overlap;
      }

      clone_info_t clone;
      clone.cloneid = file.snap_id;
      clone.snaps = file.snaps;
      to_vector(overlap, &clone.overlap);
      clone.size = file.data.length();
      out_snaps->clones.push_back(clone);
    }
  }

  if ((file_snaps.size() == 1 && file_snaps.back()->data.length() > 0) ||
      include_head)
  {
    // Include the SNAP_HEAD
    LRemDBCluster::File &file = *file_snaps.back();
    if (file.exists) {
      std::shared_lock l2{file.lock};
      if (out_snaps->seq == 0 && !include_head) {
        out_snaps->seq = file.snap_id;
      }
      clone_info_t head_clone;
      head_clone.cloneid = librados::SNAP_HEAD;
      head_clone.size = file.data.length();
      out_snaps->clones.push_back(head_clone);
    }
  }

  ldout(cct, 20) << "seq=" << out_snaps->seq << ", "
                 << "clones=[";
  bool first_clone = true;
  for (auto& clone : out_snaps->clones) {
    *_dout << "{"
           << "cloneid=" << clone.cloneid << ", "
           << "snaps=" << clone.snaps << ", "
           << "overlap=" << clone.overlap << ", "
           << "size=" << clone.size << "}";
    if (!first_clone) {
      *_dout << ", ";
    } else {
      first_clone = false;
    }
  }
  *_dout << "]" << dendl;
#endif
  return 0;

}

int LRemDBIoCtxImpl::omap_get_vals2(const std::string& oid,
                                    const std::string& start_after,
                                    const std::string &filter_prefix,
                                    uint64_t max_return,
                                    std::map<std::string, bufferlist> *out_vals,
                                    bool *pmore) {
  if (out_vals == NULL) {
    return -EINVAL;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  out_vals->clear();

  std::shared_lock l{*file->lock};

  int r = file->omap->get_vals(start_after, filter_prefix, max_return, out_vals, pmore);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBIoCtxImpl::omap_get_vals(const std::string& oid,
                                    const std::string& start_after,
                                    const std::string &filter_prefix,
                                    uint64_t max_return,
                                    std::map<std::string, bufferlist> *out_vals) {
  return omap_get_vals2(oid, start_after, filter_prefix, max_return, out_vals, nullptr);
}

int LRemDBIoCtxImpl::omap_get_vals_by_keys(const std::string& oid,
                                            const std::set<std::string>& keys,
                                            std::map<std::string, bufferlist> *vals) {
  if (vals == NULL) {
    return -EINVAL;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  vals->clear();

  std::shared_lock l{*file->lock};

  int r = file->omap->get_vals_by_keys(keys, vals);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBIoCtxImpl::omap_rm_keys(const std::string& oid,
                                   const std::set<std::string>& keys) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};

  int r = file->omap->rm_keys(keys);
  if (r < 0) {
    return r;
  }

  file->modify_meta().epoch = epoch;

  return file->flush();
}

int LRemDBIoCtxImpl::omap_rm_range(const std::string& oid,
                                    const string& key_begin,
                                    const string& key_end) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};

  int r = file->omap->rm_range(key_begin, key_end);
  if (r < 0) {
    return r;
  }

  file->modify_meta().epoch = epoch;

  return file->flush();
}

int LRemDBIoCtxImpl::omap_clear(const std::string& oid) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};

  int r = file->omap->clear();
  if (r < 0) {
    return r;
  }

  file->modify_meta().epoch = epoch;

  return file->flush();
}

int LRemDBIoCtxImpl::omap_set(const std::string& oid,
                               const std::map<std::string, bufferlist> &map) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};

  int r = file->omap->set(map);
  if (r < 0) {
    return r;
  }

  file->modify_meta().epoch = epoch;

  return file->flush();
}

int LRemDBIoCtxImpl::omap_get_header(LRemTransactionStateRef& trans,
                                      bufferlist *bl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();
  ldout(cct, 20) << ": <noargs>" << dendl;

  ObjFileRef file;
  file = get_file_safe(trans, false, CEPH_NOSNAP, {});
  if (file == NULL) {
    return -ENOENT;
  }

  std::shared_lock l{*file->lock};

  int r = file->omap->get_header(bl);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBIoCtxImpl::omap_set_header(const std::string& oid,
                                      const bufferlist& bl) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};

  int r = file->omap->set_header(bl);
  if (r < 0) {
    return r;
  }

  file->modify_meta().epoch = epoch;

  return file->flush();
}

int LRemDBIoCtxImpl::read(const std::string& oid, size_t len, uint64_t off,
                           bufferlist *bl, uint64_t snap_id,
                           uint64_t* objver) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, snap_id, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{*file->lock};

  int r = file->obj->read_data(off, len, bl);
  if (r < 0) {
    return r;
  }

  len = r;

  if (objver != nullptr) {
    *objver = file->meta.objver;
  }
  return len;
}

int LRemDBIoCtxImpl::remove(const std::string& oid, const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  ldout(cct, 20) << "snapc=" << snapc << dendl;

  ObjFileRef file;

#warning check locking
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, snapc);
    if (file == NULL) {
      return -ENOENT;
    }
    ++m_pool->epoch;
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
  }

  std::unique_lock l{*file->lock};

  int r = file->omap->clear();
  if (r < 0) {
    return r;
  }

  r = file->xattrs->clear();
  if (r < 0) {
    return r;
  }

  return file->obj->remove();
}

int LRemDBIoCtxImpl::selfmanaged_snap_create(uint64_t *snapid) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  *snapid = ++m_pool->snap_id;
  m_pool->snap_seqs.insert(*snapid);
  ++m_pool->epoch;
  return 0;
}

int LRemDBIoCtxImpl::selfmanaged_snap_remove(uint64_t snapid) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  LRemDBCluster::SnapSeqs::iterator it =
    m_pool->snap_seqs.find(snapid);
  if (it == m_pool->snap_seqs.end()) {
    return -ENOENT;
  }

  // TODO clean up all file snapshots
  m_pool->snap_seqs.erase(it);
  ++m_pool->epoch;
  return 0;
}

int LRemDBIoCtxImpl::selfmanaged_snap_rollback(const std::string& oid,
                                                uint64_t snapid) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

#warning implement snaps
#if 0
  std::unique_lock l{m_pool->file_lock};

  ObjFileRef file;
  LRemDBCluster::Files::iterator f_it = m_pool->files.find(
    {get_namespace(), oid});
  if (f_it == m_pool->files.end()) {
    return 0;
  }

  LRemDBCluster::FileSnapshots &snaps = f_it->second;
  file = snaps.back();

  size_t versions = 0;
  for (LRemDBCluster::FileSnapshots::reverse_iterator it = snaps.rbegin();
      it != snaps.rend(); ++it) {
    ObjFileRef file = *it;
    if (file->snap_id < get_snap_read()) {
      if (versions == 0) {
        // already at the snapshot version
        return 0;
      } else if (file->snap_id == CEPH_NOSNAP) {
        if (versions == 1) {
          // delete it current HEAD, next one is correct version
          snaps.erase(it.base());
        } else {
          // overwrite contents of current HEAD
          file = LRemDBCluster::SharedFile (new LRemDBCluster::File(**it));
          file->snap_id = CEPH_NOSNAP;
          *it = file;
        }
      } else {
        // create new head version
        file = LRemDBCluster::SharedFile (new LRemDBCluster::File(**it));
        file->snap_id = m_pool->snap_id;
        snaps.push_back(file);
      }
      return 0;
    }
    ++versions;
  }
  ++m_pool->epoch;
#endif
  return 0;
}

int LRemDBIoCtxImpl::set_alloc_hint(const std::string& oid,
                                     uint64_t expected_object_size,
                                     uint64_t expected_write_size,
                                     uint32_t flags,
                                     const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  {
    std::unique_lock l{m_pool->file_lock};
    get_file(oid, true, CEPH_NOSNAP, snapc);
  }

  /* this one doesn't really do anything, so not updating pool/file epoch */

  return 0;
}

int LRemDBIoCtxImpl::sparse_read(const std::string& oid, uint64_t off,
                                  uint64_t len,
                                  std::map<uint64_t,uint64_t> *m,
                                  bufferlist *data_bl, uint64_t snap_id) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  // TODO verify correctness
  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, snap_id, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{*file->lock};
  // TODO support sparse read
  bufferlist bl;
  int read_len = read(oid, off, len, &bl, snap_id, nullptr);
  if (m != NULL) {
    m->clear();
    if (read_len > 0) {
      (*m)[off] = read_len;
    }
  }
  if (data_bl != NULL && read_len > 0) {
    data_bl->claim_append(bl);
  }
  return read_len > 0 ? 1 : 0;
}

int LRemDBIoCtxImpl::stat2(LRemTransactionStateRef& trans, uint64_t *psize,
                            struct timespec *pts) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();
  ldout(cct, 20) << ": <noargs>" << dendl;

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans->oid(), false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{*file->lock};
  if (psize != NULL) {
    *psize = file->meta.size;
  }
  if (pts != NULL) {
    *pts = real_clock::to_timespec(file->meta.mtime);
  }
  return 0;
}

int LRemDBIoCtxImpl::mtime2(const string& oid, const struct timespec& ts,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};
  file->modify_meta().mtime =  real_clock::from_timespec(ts);
  file->modify_meta().epoch = epoch;

  return 0;
}

int LRemDBIoCtxImpl::truncate(const std::string& oid, uint64_t size,
                               const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  ldout(cct, 20) << "size=" << size << ", snapc=" << snapc << dendl;

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};
  bufferlist bl(size);

  interval_set<uint64_t> is;
  is.intersection_of(file->meta.snap_overlap);
  file->modify_meta().epoch = epoch;
  file->meta.snap_overlap.subtract(is);
  file->obj->truncate(size, file->meta);
  return file->flush();
}

int LRemDBIoCtxImpl::write(const std::string& oid, bufferlist& bl, size_t len,
                            uint64_t off, const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  ldout(cct, 20) << "extent=" << off << "~" << len << ", snapc=" << snapc
                 << dendl;
  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};
  if (len > 0) {
    interval_set<uint64_t> is;
    is.insert(off, len);
    is.intersection_of(file->meta.snap_overlap);
    file->meta.snap_overlap.subtract(is);
  }

  file->modify_meta().epoch = epoch;
  file->obj->write(off, len, bl, file->meta);
  return file->flush();
}

int LRemDBIoCtxImpl::write_full(const std::string& oid, bufferlist& bl,
                                 const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  ldout(cct, 20) << "length=" << bl.length() << ", snapc=" << snapc << dendl;
  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};
  if (bl.length() > 0) {
    interval_set<uint64_t> is;
    is.insert(0, bl.length());
    is.intersection_of(file->meta.snap_overlap);
    file->meta.snap_overlap.subtract(is);
  }

  file->modify_meta().epoch = epoch;

  file->obj->truncate(0, file->meta);
  file->obj->write(0, bl.length(), bl, file->meta);
  return file->flush();
}

int LRemDBIoCtxImpl::writesame(const std::string& oid, bufferlist& bl,
                                size_t len, uint64_t off,
                                const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  if (len == 0 || (len % bl.length())) {
    return -EINVAL;
  }

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};
  if (len > 0) {
    interval_set<uint64_t> is;
    is.insert(off, len);
    is.intersection_of(file->meta.snap_overlap);
    file->meta.snap_overlap.subtract(is);
  }

  while (len > 0) {
    int r = file->obj->write(off, len, bl, file->meta);
    if (r < 0) {
      return r;
    }
    off += bl.length();
    len -= bl.length();
  }

  file->modify_meta().epoch = epoch;

  return file->flush();;
}

int LRemDBIoCtxImpl::cmpext(const std::string& oid, uint64_t off,
                             bufferlist& cmp_bl, uint64_t snap_id) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  bufferlist read_bl;
  uint64_t len = cmp_bl.length();

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, snap_id, {});
    if (file == NULL) {
      return cmpext_compare(cmp_bl, read_bl);
    }
  }

  std::shared_lock l{*file->lock};
  if (off >= file->meta.size) {
    len = 0;
  } else if (off + len > file->meta.size) {
    len = file->meta.size - off;
  }

  int r = file->obj->read_data(off, len, &read_bl);
  if (r < 0) {
    return r;
  }

  return cmpext_compare(cmp_bl, read_bl);
}

int LRemDBIoCtxImpl::cmpxattr_str(const string& oid,
                                   const char *name, uint8_t op, const bufferlist& bl)
{
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{m_pool->file_lock};

  bufferlist attr_bl;
  int r = file->xattrs->get_val(name, &attr_bl);
  if (r < 0) {
    return r;
  }

  bool cmp;

  switch (op) {
    case CEPH_OSD_CMPXATTR_OP_EQ:
      cmp = (bl == attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_NE:
      cmp = (bl != attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_GT:
      cmp = (bl > attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_GTE:
      cmp = (bl >= attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_LT:
      cmp = (bl < attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_LTE:
      cmp = (bl <= attr_bl);
      break;
    default:
      return -EINVAL;
  }

  if (!cmp) {
    return -ECANCELED;
  }

  return 0;
}

int LRemDBIoCtxImpl::cmpxattr(const string& oid,
                               const char *name, uint8_t op, uint64_t v)
{
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{m_pool->file_lock};

  bufferlist attr_bl;
  int r = file->xattrs->get_val(name, &attr_bl);
  if (r < 0) {
    return r;
  }

  string s = attr_bl.to_str();
  string err;

  auto cct = m_client->cct();
  ldout(cct, 20) << "cmpxattr name=" << name << " s=" << s << " v=" << v << dendl;

  uint64_t attr_val = (s.empty() ? 0 : static_cast<int64_t>(strict_strtoll(s, 10, &err)));
  if (!err.empty()) {
    return -EINVAL;
  }

  bool cmp;

  switch (op) {
    case CEPH_OSD_CMPXATTR_OP_EQ:
      cmp = (v == attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_NE:
      cmp = (v != attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_GT:
      cmp = (v > attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_GTE:
      cmp = (v >= attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_LT:
      cmp = (v < attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_LTE:
      cmp = (v <= attr_val);
      break;
    default:
      return -EINVAL;
  }

  if (!cmp) {
    return -ECANCELED;
  }

  return 0;
}


int LRemDBIoCtxImpl::xattr_get(LRemTransactionStateRef& trans,
                                std::map<std::string, bufferlist>* attrset) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{m_pool->file_lock};

  int r = file->xattrs->get_all_vals(attrset);
  if (r  < 0) {
    return r;
  }

  ldout(cct, 20) << ": -> attrset=" << *attrset << dendl;

  return 0;
}

int LRemDBIoCtxImpl::setxattr(LRemTransactionStateRef& trans, const char *name,
                               bufferlist& bl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, {});
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};

  map<string, bufferlist> m;
  m[name] = bl;

  int r = file->xattrs->set(m);
  if (r < 0) {
    return r;
  }

  file->modify_meta().epoch = epoch;

  return file->flush();;
}

int LRemDBIoCtxImpl::rmxattr(LRemTransactionStateRef& trans, const char *name) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();

  uint64_t epoch;

  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, {});
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{*file->lock};

  std::set<string> keys;
  keys.insert(name);

  int r = file->xattrs->rm_keys(keys);
  if (r < 0) {
    return r;
  }

  file->modify_meta().epoch = epoch;

  return file->flush();;
}

int LRemDBIoCtxImpl::zero(const std::string& oid, uint64_t off, uint64_t len,
                           const SnapContext &snapc) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  ldout(cct, 20) << "extent=" << off << "~" << len << ", snapc=" << snapc
                 << dendl;

  bool truncate_redirect = false;
  ObjFileRef file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, snapc);
    if (!file) {
      return 0;
    }
    file = get_file(oid, true, CEPH_NOSNAP, snapc);

    std::shared_lock l2{*file->lock};
    if (len > 0 && off + len >= file->meta.size) {
      // Zero -> Truncate logic embedded in OSD
      truncate_redirect = true;
    }
    file->modify_meta().epoch = ++m_pool->epoch;
  }
  if (truncate_redirect) {
    return truncate(oid, off, snapc);
  }

  bufferlist bl;
  bl.append_zero(len);
  return write(oid, bl, len, off, snapc);
}

int LRemDBIoCtxImpl::get_current_ver(const std::string& oid, uint64_t *ver) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  ObjFileRef file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }

    *ver = file->meta.epoch;
  }

  return 0;
}

void LRemDBIoCtxImpl::append_clone(bufferlist& src, bufferlist* dest) {
  // deep-copy the src to ensure our memory-based mock RADOS data cannot
  // be modified by callers
  if (src.length() > 0) {
    bufferlist::iterator iter = src.begin();
    buffer::ptr ptr;
    iter.copy_deep(src.length(), ptr);
    dest->append(ptr);
  }
}

size_t LRemDBIoCtxImpl::clip_io(size_t off, size_t len, size_t bl_len) {
  if (off >= bl_len) {
    len = 0;
  } else if (off + len > bl_len) {
    len = bl_len - off;
  }
  return len;
}

void LRemDBIoCtxImpl::ensure_minimum_length(size_t len, bufferlist *bl) {
  if (len > bl->length()) {
    bufferptr ptr(buffer::create(len - bl->length()));
    ptr.zero();
    bl->append(ptr);
  }
}

LRemDBIoCtxImpl::ObjFileRef LRemDBIoCtxImpl::get_file(
    const std::string &oid, bool write, uint64_t snap_id,
    const SnapContext &snapc) {
  ceph_assert(ceph_mutex_is_locked(m_pool->file_lock) ||
	      ceph_mutex_is_wlocked(m_pool->file_lock));
  ceph_assert(!write || ceph_mutex_is_wlocked(m_pool->file_lock));


  auto of = make_shared<ObjFile>();

  of->obj = m_pool_db->get_obj_handler(get_namespace(), oid);
  of->omap = m_pool_db->get_omap_handler(get_namespace(), oid);
  of->xattrs = m_pool_db->get_xattrs_handler(get_namespace(), oid);

  int r = of->obj->read_meta(&of->meta);
  if (r < 0 && r != -ENOENT) {
    return ObjFileRef();
  }
  of->exists = (r == 0);

  if (!of->exists && !write) {
    return ObjFileRef();
  }


  auto& meta = of->meta;
#if 0
  LRemDBCluster::SharedFile file;
  LRemDBCluster::Files::iterator it = m_pool->files.find(
    {get_namespace(), oid});
  if (it != m_pool->files.end()) {
    file = it->second.back();
  } else if (!write) {
    return ObjFileRef();
  }
#endif

#warning FIXME lock
  if (of->exists || write) {
    of->lock = &m_pool->file_locks[{get_namespace(), oid}].lock;
  }

  if (write) {
    bool new_version = false;
    if (!of->exists) {
      new_version = true;
    } else {
      if (!snapc.snaps.empty() && meta.snap_id < snapc.seq) {
        for (std::vector<snapid_t>::const_reverse_iterator seq_it =
            snapc.snaps.rbegin();
            seq_it != snapc.snaps.rend(); ++seq_it) {
          if (*seq_it > meta.snap_id && *seq_it <= snapc.seq) {
            meta.snaps.push_back(*seq_it);
          }
        }

#warning clone snap
#if 0
        bufferlist prev_data = file->data;
        file = LRemDBCluster::SharedFile(
          new LRemDBCluster::File(*file));
        file->data.clear();
        append_clone(prev_data, &file->data);
        if (prev_data.length() > 0) {
          meta.snap_overlap.insert(0, prev_data.length());
        }
#endif
        new_version = true;
      }
    }

    if (new_version) {
      meta.snap_id = snapc.seq;
      meta.mtime = real_clock::now();
    }

    meta.objver++;
    return of;
  }

  if (snap_id == CEPH_NOSNAP) {
    if (!of->exists) {
#if 0
      ceph_assert(it->second.size() > 1);
#endif
      return ObjFileRef();
    }
    return of;
  }

#if 0
  LRemDBCluster::FileSnapshots &snaps = it->second;
  for (LRemDBCluster::FileSnapshots::reverse_iterator it = snaps.rbegin();
      it != snaps.rend(); ++it) {
    ObjFileRef file = *it;
    if (meta.snap_id < snap_id) {
      if (!of->exists) {
        return ObjFileRef();
      }
      return of;
    }
  }
#endif
  return ObjFileRef();
}

LRemDBIoCtxImpl::ObjFileRef LRemDBIoCtxImpl::get_file_safe(
    LRemTransactionStateRef& trans, bool write, uint64_t snap_id,
    const SnapContext &snapc,
    uint64_t *pepoch) {
  write |= trans->write;
  if (write) {
    std::unique_lock l{m_pool->file_lock};
    uint64_t epoch = ++m_pool->epoch;
    if (pepoch) {
      *pepoch = epoch;
    }
    return get_file(trans->oid(), true, snap_id, snapc);
  }

  std::shared_lock l{m_pool->file_lock};
  return get_file(trans->oid(), false, snap_id, snapc);
}

int LRemDBIoCtxImpl::pool_op(LRemTransactionStateRef& trans,
                              bool write,
                              PoolOperation op) {
  auto cct = m_client->cct();
  auto& oid = trans->oid();
  bool _write = (write | trans->write);
  ldout(cct, 20) << "pool_op() trans->write=" << trans->write << " write=" << write << " -> " << _write << dendl;

  if (_write) {
    std::unique_lock l{m_pool->file_lock};
    ++m_pool->epoch;
    return op(m_pool, true);
  }

  std::shared_lock l{m_pool->file_lock};
  return op(m_pool, false);
}

int LRemDBIoCtxImpl::ObjFile::flush() {
  int r;

  if (flags & ModFlags::Meta) {
    r = obj->write_meta(meta);
    if (r < 0) {
      return r;
    }
  }

  flags = 0;

  return 0;
}

LRemTransactionStateRef LRemDBIoCtxImpl::init_transaction(const std::string& oid) {
  return make_op_transaction({get_namespace(), oid});
}

} // namespace librados
