// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd_cuobj_gather.h"

#include <cuobjclient.h>

#include <algorithm>
#include <cstdlib>
#include <sys/mman.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/Formatter.h"

#define dout_context m_cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd.cuobj.gather "

namespace {

// libcuobjclient registers at most this much in one MR
constexpr size_t MAX_REGISTRATION = (4ull << 30) - (64 << 10);

// the token flow never uses the library's own get/put callbacks
CUObjIOOps empty_ops{};

} // namespace

OSDCuObjGather::OSDCuObjGather(CephContext *cct)
  : m_cct(cct)
{
  int r = do_init();
  if (r < 0) {
    do_shutdown();
  } else {
    m_available = true;
  }
}

OSDCuObjGather::~OSDCuObjGather()
{
  do_shutdown();
}

int OSDCuObjGather::do_init()
{
  m_slot_size = m_cct->_conf.get_val<Option::size_t>("osd_cuobj_gather_slot_size");
  m_slot_count = m_cct->_conf.get_val<uint64_t>("osd_cuobj_gather_slots");
  m_quarantine = std::chrono::milliseconds(static_cast<int64_t>(
    m_cct->_conf.get_val<double>("osd_cuobj_gather_quarantine") * 1000));

  if (!m_slot_size || !m_slot_count) {
    derr << "gather disabled: slot size or count is zero" << dendl;
    return -EINVAL;
  }
  m_arena_size = m_slot_size * m_slot_count;
  if (m_arena_size > MAX_REGISTRATION) {
    derr << "gather arena " << m_arena_size
         << " exceeds the largest registrable window " << MAX_REGISTRATION
         << "; lower osd_cuobj_gather_slots" << dendl;
    return -EINVAL;
  }

  // the client library reads its NIC selection from the cufile json;
  // point it there for this process if the deployment says where
  auto json = m_cct->_conf.get_val<std::string>("osd_cuobj_gather_config");
  if (!json.empty() && !getenv("CUFILE_ENV_PATH_JSON")) {
    setenv("CUFILE_ENV_PATH_JSON", json.c_str(), 0);
  }

  try {
    m_client = std::make_unique<cuObjClient>(empty_ops, CUOBJ_PROTO_RDMA_DC_V1);
  } catch (const std::exception& e) {
    derr << "cuObjClient init failed: " << e.what() << dendl;
    return -EIO;
  }
  if (!m_client->isConnected()) {
    derr << "cuObjClient did not connect to the RDMA fabric" << dendl;
    return -EIO;
  }

  // 2 MiB alignment so the arena can be backed by huge pages, which
  // also keeps the registration's page-table footprint small
  void* p = nullptr;
  if (posix_memalign(&p, 2 << 20, m_arena_size) != 0) {
    derr << "gather arena allocation of " << m_arena_size << " failed" << dendl;
    return -ENOMEM;
  }
  m_arena = static_cast<char*>(p);
  madvise(m_arena, m_arena_size, MADV_HUGEPAGE);

  if (m_client->cuMemObjGetDescriptor(m_arena, m_arena_size) != CU_OBJ_SUCCESS) {
    derr << "gather arena registration of " << m_arena_size << " failed"
         << dendl;
    return -EIO;
  }

  // one token for the whole arena, held for the life of the OSD; peers
  // address slots by offset within it
  if (m_client->cuMemObjGetRDMAToken(m_arena, m_arena_size, 0, CUOBJ_GET,
                                     &m_token_raw) != CU_OBJ_SUCCESS ||
      !m_token_raw) {
    derr << "gather token mint failed" << dendl;
    m_token_raw = nullptr;
    return -EIO;
  }
  m_token = m_token_raw;

  m_free.reserve(m_slot_count);
  for (uint32_t i = m_slot_count; i > 0; --i) {
    m_free.push_back(i - 1);
  }

  dout(1) << "arena " << m_arena_size << " bytes, " << m_slot_count
          << " slots of " << m_slot_size << ", quarantine "
          << m_quarantine.count() << " ms" << dendl;
  return 0;
}

void OSDCuObjGather::do_shutdown()
{
  // the library's lifetime rules: token before registration before client
  if (m_client) {
    if (m_token_raw) {
      m_client->cuMemObjPutRDMAToken(m_token_raw);
      m_token_raw = nullptr;
    }
    if (m_arena) {
      m_client->cuMemObjPutDescriptor(m_arena);
    }
    m_client.reset();
  }
  free(m_arena);
  m_arena = nullptr;
  m_available = false;
}

void OSDCuObjGather::reap_quarantine(ceph::coarse_mono_clock::time_point now)
{
  // called with m_lock held
  while (!m_quarantined.empty() && m_quarantined.front().until <= now) {
    m_free.push_back(m_quarantined.front().idx);
    m_quarantined.pop_front();
  }
}

std::optional<OSDCuObjGather::slot> OSDCuObjGather::acquire(size_t len)
{
  if (!m_available) {
    return std::nullopt;
  }
  if (len > m_slot_size) {
    m_oversized++;
    return std::nullopt;
  }

  uint32_t idx;
  {
    std::lock_guard l(m_lock);
    if (m_free.empty()) {
      reap_quarantine(ceph::coarse_mono_clock::now());
    }
    if (m_free.empty()) {
      m_exhausted++;
      return std::nullopt;
    }
    idx = m_free.back();
    m_free.pop_back();
  }

  m_acquired++;
  m_in_use++;
  slot s;
  s.ofs = static_cast<uint64_t>(idx) * m_slot_size;
  s.len = m_slot_size;
  s.ptr = m_arena + s.ofs;
  return s;
}

void OSDCuObjGather::release(const slot& s, bool quarantine)
{
  const uint32_t idx = s.ofs / m_slot_size;
  ceph_assert(idx < m_slot_count);
  m_in_use--;

  std::lock_guard l(m_lock);
  if (quarantine && m_quarantine.count() > 0) {
    m_quarantines++;
    m_quarantined.push_back({idx, ceph::coarse_mono_clock::now() + m_quarantine});
  } else {
    m_free.push_back(idx);
  }
}

void OSDCuObjGather::dump_stats(ceph::Formatter* f) const
{
  f->open_object_section("gather");
  f->dump_bool("available", m_available);
  f->dump_unsigned("arena_bytes", m_arena_size);
  f->dump_unsigned("slot_size", m_slot_size);
  f->dump_unsigned("slots", m_slot_count);
  f->dump_unsigned("slots_in_use", m_in_use.load());
  f->dump_unsigned("slots_acquired", m_acquired.load());
  f->dump_unsigned("acquire_exhausted", m_exhausted.load());
  f->dump_unsigned("acquire_oversized", m_oversized.load());
  f->dump_unsigned("quarantines", m_quarantines.load());
  {
    auto& self = const_cast<OSDCuObjGather&>(*this);
    std::lock_guard l(self.m_lock);
    f->dump_unsigned("slots_quarantined", m_quarantined.size());
  }
  f->close_section();
}
