// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestMemIoCtxImpl.h"
#include "test/librados_test_stub/TestMemRadosClient.h"
#include "common/Clock.h"
#include "common/RWLock.h"
#include "include/err.h"
#include <functional>
#include <boost/algorithm/string/predicate.hpp>
#include <errno.h>
#include <include/compat.h>

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

TestMemIoCtxImpl::TestMemIoCtxImpl() {
}

TestMemIoCtxImpl::TestMemIoCtxImpl(const TestMemIoCtxImpl& rhs)
    : TestIoCtxImpl(rhs), m_client(rhs.m_client), m_pool(rhs.m_pool) {
  m_pool->get();
}

TestMemIoCtxImpl::TestMemIoCtxImpl(TestMemRadosClient *client, int64_t pool_id,
                                   const std::string& pool_name,
                                   TestMemCluster::Pool *pool)
    : TestIoCtxImpl(client, pool_id, pool_name), m_client(client),
      m_pool(pool) {
  m_pool->get();
}

TestMemIoCtxImpl::~TestMemIoCtxImpl() {
  m_pool->put();
}

TestIoCtxImpl *TestMemIoCtxImpl::clone() {
  return new TestMemIoCtxImpl(*this);
}

int TestMemIoCtxImpl::aio_remove(const std::string& oid, AioCompletionImpl *c, int flags) {
  m_client->add_aio_operation(oid, true,
                              std::bind(&TestMemIoCtxImpl::remove, this, oid,
					get_snap_context()),
                              c);
  return 0;
}

int TestMemIoCtxImpl::append(const std::string& oid, const bufferlist &bl,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
  }

  std::unique_lock l{file->lock};
  auto off = file->data.length();
  ensure_minimum_length(off + bl.length(), &file->data);
  file->data.begin(off).copy_in(bl.length(), bl);
  return 0;
}

int TestMemIoCtxImpl::assert_exists(const std::string &oid, uint64_t snap_id) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  std::shared_lock l{m_pool->file_lock};
  TestMemCluster::SharedFile file = get_file(oid, false, snap_id, {});
  if (file == NULL) {
    return -ENOENT;
  }
  return 0;
}

int TestMemIoCtxImpl::create(const std::string& oid, bool exclusive,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  if (exclusive) {
    TestMemCluster::SharedFile file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file != NULL && file->exists) {
      return -EEXIST;
    }
  }

  get_file(oid, true, CEPH_NOSNAP, snapc);
  return 0;
}

int TestMemIoCtxImpl::list_snaps(const std::string& oid, snap_set_t *out_snaps) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  out_snaps->seq = 0;
  out_snaps->clones.clear();

  std::shared_lock l{m_pool->file_lock};
  TestMemCluster::Files::iterator it = m_pool->files.find(
    {get_namespace(), oid});
  if (it == m_pool->files.end()) {
    return -ENOENT;
  }

  bool include_head = false;
  TestMemCluster::FileSnapshots &file_snaps = it->second;
  for (TestMemCluster::FileSnapshots::iterator s_it = file_snaps.begin();
       s_it != file_snaps.end(); ++s_it) {
    TestMemCluster::File &file = *s_it->get();

    if (file_snaps.size() > 1) {
      out_snaps->seq = file.snap_id;
      TestMemCluster::FileSnapshots::iterator next_it(s_it);
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
      TestMemCluster::File &next_file = *next_it->get();
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
    TestMemCluster::File &file = *file_snaps.back();
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
  return 0;

}

int TestMemIoCtxImpl::omap_get_vals2(const std::string& oid,
                                    const std::string& start_after,
                                    const std::string &filter_prefix,
                                    uint64_t max_return,
                                    std::map<std::string, bufferlist> *out_vals,
                                    bool *pmore) {
  if (out_vals == NULL) {
    return -EINVAL;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  out_vals->clear();

  std::shared_lock l{file->lock};
  TestMemCluster::FileOMaps::iterator o_it = m_pool->file_omaps.find(
    {get_namespace(), oid});
  if (o_it == m_pool->file_omaps.end()) {
    if (pmore) {
      *pmore = false;
    }
    return 0;
  }

  TestMemCluster::OMap &omap = o_it->second;
  TestMemCluster::OMap::iterator it = omap.begin();
  if (!start_after.empty()) {
    it = omap.upper_bound(start_after);
  }

  while (it != omap.end() && max_return > 0) {
    if (filter_prefix.empty() ||
        boost::algorithm::starts_with(it->first, filter_prefix)) {
      (*out_vals)[it->first] = it->second;
      --max_return;
    }
    ++it;
  }
  if (pmore) {
    *pmore = (it != omap.end());
  }
  return 0;
}

int TestMemIoCtxImpl::omap_get_vals(const std::string& oid,
                                    const std::string& start_after,
                                    const std::string &filter_prefix,
                                    uint64_t max_return,
                                    std::map<std::string, bufferlist> *out_vals) {
  return omap_get_vals2(oid, start_after, filter_prefix, max_return, out_vals, nullptr);
}

int TestMemIoCtxImpl::omap_rm_keys(const std::string& oid,
                                   const std::set<std::string>& keys) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::unique_lock l{file->lock};
  for (std::set<std::string>::iterator it = keys.begin();
       it != keys.end(); ++it) {
    m_pool->file_omaps[{get_namespace(), oid}].erase(*it);
  }
  return 0;
}

int TestMemIoCtxImpl::omap_set(const std::string& oid,
                               const std::map<std::string, bufferlist> &map) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::unique_lock l{file->lock};
  for (std::map<std::string, bufferlist>::const_iterator it = map.begin();
      it != map.end(); ++it) {
    bufferlist bl;
    bl.append(it->second);
    m_pool->file_omaps[{get_namespace(), oid}][it->first] = bl;
  }

  return 0;
}

int TestMemIoCtxImpl::read(const std::string& oid, size_t len, uint64_t off,
                           bufferlist *bl, uint64_t snap_id) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, snap_id, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{file->lock};
  if (len == 0) {
    len = file->data.length();
  }
  len = clip_io(off, len, file->data.length());
  if (bl != NULL && len > 0) {
    bufferlist bit;
    bit.substr_of(file->data, off, len);
    append_clone(bit, bl);
  }
  return len;
}

int TestMemIoCtxImpl::remove(const std::string& oid, const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  TestMemCluster::SharedFile file = get_file(oid, false, CEPH_NOSNAP, snapc);
  if (file == NULL) {
    return -ENOENT;
  }
  file = get_file(oid, true, CEPH_NOSNAP, snapc);

  {
    std::unique_lock l2{file->lock};
    file->exists = false;
  }

  TestCluster::ObjectLocator locator(get_namespace(), oid);
  TestMemCluster::Files::iterator it = m_pool->files.find(locator);
  ceph_assert(it != m_pool->files.end());

  if (*it->second.rbegin() == file) {
    TestMemCluster::ObjectHandlers object_handlers;
    std::swap(object_handlers, m_pool->file_handlers[locator]);
    m_pool->file_handlers.erase(locator);

    for (auto object_handler : object_handlers) {
      object_handler->handle_removed(m_client);
    }
  }

  if (it->second.size() == 1) {
    m_pool->files.erase(it);
    m_pool->file_omaps.erase(locator);
  }
  return 0;
}

int TestMemIoCtxImpl::selfmanaged_snap_create(uint64_t *snapid) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  *snapid = ++m_pool->snap_id;
  m_pool->snap_seqs.insert(*snapid);
  return 0;
}

int TestMemIoCtxImpl::selfmanaged_snap_remove(uint64_t snapid) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  TestMemCluster::SnapSeqs::iterator it =
    m_pool->snap_seqs.find(snapid);
  if (it == m_pool->snap_seqs.end()) {
    return -ENOENT;
  }

  // TODO clean up all file snapshots
  m_pool->snap_seqs.erase(it);
  return 0;
}

int TestMemIoCtxImpl::selfmanaged_snap_rollback(const std::string& oid,
                                                uint64_t snapid) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};

  TestMemCluster::SharedFile file;
  TestMemCluster::Files::iterator f_it = m_pool->files.find(
    {get_namespace(), oid});
  if (f_it == m_pool->files.end()) {
    return 0;
  }

  TestMemCluster::FileSnapshots &snaps = f_it->second;
  file = snaps.back();

  size_t versions = 0;
  for (TestMemCluster::FileSnapshots::reverse_iterator it = snaps.rbegin();
      it != snaps.rend(); ++it) {
    TestMemCluster::SharedFile file = *it;
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
          file = TestMemCluster::SharedFile (new TestMemCluster::File(**it));
          file->snap_id = CEPH_NOSNAP;
          *it = file;
        }
      } else {
        // create new head version
        file = TestMemCluster::SharedFile (new TestMemCluster::File(**it));
        file->snap_id = m_pool->snap_id;
        snaps.push_back(file);
      }
      return 0;
    }
    ++versions;
  }
  return 0;
}

int TestMemIoCtxImpl::set_alloc_hint(const std::string& oid,
                                     uint64_t expected_object_size,
                                     uint64_t expected_write_size,
                                     uint32_t flags,
                                     const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  {
    std::unique_lock l{m_pool->file_lock};
    get_file(oid, true, CEPH_NOSNAP, snapc);
  }

  return 0;
}

int TestMemIoCtxImpl::sparse_read(const std::string& oid, uint64_t off,
                                  uint64_t len,
                                  std::map<uint64_t,uint64_t> *m,
                                  bufferlist *data_bl, uint64_t snap_id) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  // TODO verify correctness
  TestMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, snap_id, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{file->lock};
  len = clip_io(off, len, file->data.length());
  // TODO support sparse read
  if (m != NULL) {
    m->clear();
    if (len > 0) {
      (*m)[off] = len;
    }
  }
  if (data_bl != NULL && len > 0) {
    bufferlist bit;
    bit.substr_of(file->data, off, len);
    append_clone(bit, data_bl);
  }
  return len > 0 ? 1 : 0;
}

int TestMemIoCtxImpl::stat(const std::string& oid, uint64_t *psize,
                           time_t *pmtime) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{file->lock};
  if (psize != NULL) {
    *psize = file->data.length();
  }
  if (pmtime != NULL) {
    *pmtime = file->mtime;
  }
  return 0;
}

int TestMemIoCtxImpl::truncate(const std::string& oid, uint64_t size,
                               const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
  }

  std::unique_lock l{file->lock};
  bufferlist bl(size);

  interval_set<uint64_t> is;
  if (file->data.length() > size) {
    is.insert(size, file->data.length() - size);

    bl.substr_of(file->data, 0, size);
    file->data.swap(bl);
  } else if (file->data.length() != size) {
    if (size == 0) {
      bl.clear();
    } else {
      is.insert(0, size);

      bl.append_zero(size - file->data.length());
      file->data.append(bl);
    }
  }
  is.intersection_of(file->snap_overlap);
  file->snap_overlap.subtract(is);
  return 0;
}

int TestMemIoCtxImpl::write(const std::string& oid, bufferlist& bl, size_t len,
                            uint64_t off, const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
  }

  std::unique_lock l{file->lock};
  if (len > 0) {
    interval_set<uint64_t> is;
    is.insert(off, len);
    is.intersection_of(file->snap_overlap);
    file->snap_overlap.subtract(is);
  }

  ensure_minimum_length(off + len, &file->data);
  file->data.begin(off).copy_in(len, bl);
  return 0;
}

int TestMemIoCtxImpl::write_full(const std::string& oid, bufferlist& bl,
                                 const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::unique_lock l{file->lock};
  if (bl.length() > 0) {
    interval_set<uint64_t> is;
    is.insert(0, bl.length());
    is.intersection_of(file->snap_overlap);
    file->snap_overlap.subtract(is);
  }

  file->data.clear();
  ensure_minimum_length(bl.length(), &file->data);
  file->data.begin().copy_in(bl.length(), bl);
  return 0;
}

int TestMemIoCtxImpl::writesame(const std::string& oid, bufferlist& bl,
                                size_t len, uint64_t off,
                                const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  if (len == 0 || (len % bl.length())) {
    return -EINVAL;
  }

  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, true, CEPH_NOSNAP, snapc);
  }

  std::unique_lock l{file->lock};
  if (len > 0) {
    interval_set<uint64_t> is;
    is.insert(off, len);
    is.intersection_of(file->snap_overlap);
    file->snap_overlap.subtract(is);
  }

  ensure_minimum_length(off + len, &file->data);
  while (len > 0) {
    file->data.begin(off).copy_in(bl.length(), bl);
    off += bl.length();
    len -= bl.length();
  }
  return 0;
}

int TestMemIoCtxImpl::cmpext(const std::string& oid, uint64_t off,
                             bufferlist& cmp_bl, uint64_t snap_id) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  bufferlist read_bl;
  uint64_t len = cmp_bl.length();

  TestMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(oid, false, snap_id, {});
    if (file == NULL) {
      return cmpext_compare(cmp_bl, read_bl);
    }
  }

  std::shared_lock l{file->lock};
  if (off >= file->data.length()) {
    len = 0;
  } else if (off + len > file->data.length()) {
    len = file->data.length() - off;
  }
  read_bl.substr_of(file->data, off, len);
  return cmpext_compare(cmp_bl, read_bl);
}

int TestMemIoCtxImpl::xattr_get(const std::string& oid,
                                std::map<std::string, bufferlist>* attrset) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  TestMemCluster::SharedFile file;
  std::shared_lock l{m_pool->file_lock};
  TestMemCluster::FileXAttrs::iterator it = m_pool->file_xattrs.find(
    {get_namespace(), oid});
  if (it == m_pool->file_xattrs.end()) {
    return -ENODATA;
  }
  *attrset = it->second;
  return 0;
}

int TestMemIoCtxImpl::xattr_set(const std::string& oid, const std::string &name,
                                bufferlist& bl) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  m_pool->file_xattrs[{get_namespace(), oid}][name] = bl;
  return 0;
}

int TestMemIoCtxImpl::zero(const std::string& oid, uint64_t off, uint64_t len,
                           const SnapContext &snapc) {
  if (m_client->is_blacklisted()) {
    return -EBLACKLISTED;
  }

  bool truncate_redirect = false;
  TestMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(oid, false, CEPH_NOSNAP, snapc);
    if (!file) {
      return 0;
    }
    file = get_file(oid, true, CEPH_NOSNAP, snapc);

    std::shared_lock l2{file->lock};
    if (len > 0 && off + len >= file->data.length()) {
      // Zero -> Truncate logic embedded in OSD
      truncate_redirect = true;
    }
  }
  if (truncate_redirect) {
    return truncate(oid, off, snapc);
  }

  bufferlist bl;
  bl.append_zero(len);
  return write(oid, bl, len, off, snapc);
}

void TestMemIoCtxImpl::append_clone(bufferlist& src, bufferlist* dest) {
  // deep-copy the src to ensure our memory-based mock RADOS data cannot
  // be modified by callers
  if (src.length() > 0) {
    bufferlist::iterator iter = src.begin();
    buffer::ptr ptr;
    iter.copy_deep(src.length(), ptr);
    dest->append(ptr);
  }
}

size_t TestMemIoCtxImpl::clip_io(size_t off, size_t len, size_t bl_len) {
  if (off >= bl_len) {
    len = 0;
  } else if (off + len > bl_len) {
    len = bl_len - off;
  }
  return len;
}

void TestMemIoCtxImpl::ensure_minimum_length(size_t len, bufferlist *bl) {
  if (len > bl->length()) {
    bufferptr ptr(buffer::create(len - bl->length()));
    ptr.zero();
    bl->append(ptr);
  }
}

TestMemCluster::SharedFile TestMemIoCtxImpl::get_file(
    const std::string &oid, bool write, uint64_t snap_id,
    const SnapContext &snapc) {
  ceph_assert(ceph_mutex_is_locked(m_pool->file_lock) ||
	      ceph_mutex_is_wlocked(m_pool->file_lock));
  ceph_assert(!write || ceph_mutex_is_wlocked(m_pool->file_lock));

  TestMemCluster::SharedFile file;
  TestMemCluster::Files::iterator it = m_pool->files.find(
    {get_namespace(), oid});
  if (it != m_pool->files.end()) {
    file = it->second.back();
  } else if (!write) {
    return TestMemCluster::SharedFile();
  }

  if (write) {
    bool new_version = false;
    if (!file || !file->exists) {
      file = TestMemCluster::SharedFile(new TestMemCluster::File());
      new_version = true;
    } else {
      if (!snapc.snaps.empty() && file->snap_id < snapc.seq) {
        for (std::vector<snapid_t>::const_reverse_iterator seq_it =
            snapc.snaps.rbegin();
            seq_it != snapc.snaps.rend(); ++seq_it) {
          if (*seq_it > file->snap_id && *seq_it <= snapc.seq) {
            file->snaps.push_back(*seq_it);
          }
        }

        bufferlist prev_data = file->data;
        file = TestMemCluster::SharedFile(
          new TestMemCluster::File(*file));
        file->data.clear();
        append_clone(prev_data, &file->data);
        if (prev_data.length() > 0) {
          file->snap_overlap.insert(0, prev_data.length());
        }
        new_version = true;
      }
    }

    if (new_version) {
      file->snap_id = snapc.seq;
      file->mtime = ceph_clock_now().sec();
      m_pool->files[{get_namespace(), oid}].push_back(file);
    }

    file->objver++;
    return file;
  }

  if (snap_id == CEPH_NOSNAP) {
    if (!file->exists) {
      ceph_assert(it->second.size() > 1);
      return TestMemCluster::SharedFile();
    }
    return file;
  }

  TestMemCluster::FileSnapshots &snaps = it->second;
  for (TestMemCluster::FileSnapshots::reverse_iterator it = snaps.rbegin();
      it != snaps.rend(); ++it) {
    TestMemCluster::SharedFile file = *it;
    if (file->snap_id < snap_id) {
      if (!file->exists) {
        return TestMemCluster::SharedFile();
      }
      return file;
    }
  }
  return TestMemCluster::SharedFile();
}

} // namespace librados
