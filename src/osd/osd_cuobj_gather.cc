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
  // and as a source, for peers that read a write payload out of it
  if (m_client->cuMemObjGetRDMAToken(m_arena, m_arena_size, 0, CUOBJ_PUT,
                                     &m_put_token_raw) != CU_OBJ_SUCCESS ||
      !m_put_token_raw) {
    derr << "gather PUT token mint failed" << dendl;
    m_put_token_raw = nullptr;
    return -EIO;
  }
  m_put_token = m_put_token_raw;

  m_free.reserve(m_slot_count);
  m_backing.assign(m_slot_count, std::nullopt);
  for (uint32_t i = m_slot_count; i > 0; --i) {
    m_free.push_back(i - 1);
  }

  dout(1) << "arena " << m_arena_size << " bytes, " << m_slot_count
          << " slots of " << m_slot_size << ", quarantine "
          << m_quarantine.count() << " ms" << dendl;
  return 0;
}

std::unique_ptr<OSDCuObjGather::source>
OSDCuObjGather::register_source(void* ptr, size_t len, ceph::buffer::list keep)
{
  if (!m_available || !len) {
    return nullptr;
  }
  auto s = std::make_unique<source>();
  s->ptr = ptr;
  s->len = len;
  s->keep = std::move(keep);
  {
    std::lock_guard l(m_client_lock);
    reap_sources(ceph::coarse_mono_clock::now());
    if (m_client->cuMemObjGetDescriptor(ptr, len) != CU_OBJ_SUCCESS) {
      m_source_failures++;
      dout(10) << "source registration of " << len << " bytes at " << ptr
               << " failed" << dendl;
      return nullptr;
    }
    if (m_client->cuMemObjGetRDMAToken(ptr, len, 0, CUOBJ_PUT,
                                       &s->token_raw) != CU_OBJ_SUCCESS ||
        !s->token_raw) {
      s->token_raw = nullptr;
      m_client->cuMemObjPutDescriptor(ptr);
      m_source_failures++;
      dout(10) << "source token mint for " << ptr << " failed" << dendl;
      return nullptr;
    }
  }
  s->token = s->token_raw;
  m_sources++;
  return s;
}

std::unique_ptr<OSDCuObjGather::source>
OSDCuObjGather::arena_source(ceph::buffer::list keep)
{
  if (!m_available || m_put_token.empty()) {
    return nullptr;
  }
  auto s = std::make_unique<source>();
  s->ptr = m_arena;
  s->len = m_arena_size;
  s->token = m_put_token;
  s->keep = std::move(keep);
  return s;
}

void OSDCuObjGather::drop_source(source& s)
{
  // called with m_client_lock held; an arena source holds no
  // registration of its own
  if (s.token_raw) {
    m_client->cuMemObjPutRDMAToken(s.token_raw);
    s.token_raw = nullptr;
    m_client->cuMemObjPutDescriptor(s.ptr);
  }
  s.keep.clear();
}

void OSDCuObjGather::reap_sources(ceph::coarse_mono_clock::time_point now)
{
  // called with m_client_lock held
  while (!m_quarantined_sources.empty() &&
         m_quarantined_sources.front().until <= now) {
    drop_source(*m_quarantined_sources.front().s);
    m_quarantined_sources.pop_front();
  }
}

void OSDCuObjGather::release_source(std::unique_ptr<source> s, bool quarantine)
{
  if (!s || !m_client) {
    return;
  }
  std::lock_guard l(m_client_lock);
  const auto now = ceph::coarse_mono_clock::now();
  reap_sources(now);
  if (quarantine && m_quarantine.count() > 0) {
    m_source_quarantines++;
    m_quarantined_sources.push_back({std::move(s), now + m_quarantine});
  } else {
    drop_source(*s);
  }
}

void OSDCuObjGather::do_shutdown()
{
  // the library's lifetime rules: token before registration before client
  if (m_client) {
    for (auto& h : m_quarantined_sources) {
      drop_source(*h.s);
    }
    m_quarantined_sources.clear();
  }
  if (m_client) {
    if (m_put_token_raw) {
      m_client->cuMemObjPutRDMAToken(m_put_token_raw);
      m_put_token_raw = nullptr;
    }
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
  m_backing[idx].reset();
  if (quarantine && m_quarantine.count() > 0) {
    m_quarantines++;
    m_quarantined.push_back({idx, ceph::coarse_mono_clock::now() + m_quarantine});
  } else {
    m_free.push_back(idx);
  }
}

void OSDCuObjGather::set_client_backing(const slot& s, const std::string& token,
                                        uint64_t remote_ofs)
{
  const uint32_t idx = s.ofs / m_slot_size;
  ceph_assert(idx < m_slot_count);
  std::lock_guard l(m_lock);
  m_backing[idx] = client_backing{token, remote_ofs};
}

std::optional<OSDCuObjGather::client_backing>
OSDCuObjGather::client_backing_of(const void* ptr) const
{
  const char* p = static_cast<const char*>(ptr);
  if (!contains(p, 1)) {
    return std::nullopt;
  }
  const uint32_t idx = (p - m_arena) / m_slot_size;
  auto& self = const_cast<OSDCuObjGather&>(*this);
  std::lock_guard l(self.m_lock);
  const auto& b = m_backing[idx];
  if (!b) {
    return std::nullopt;
  }
  client_backing r = *b;
  r.remote_ofs += p - (m_arena + uint64_t(idx) * m_slot_size);
  return r;
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
  f->dump_unsigned("direct_pull_pieces", m_direct_pieces.load());
  f->dump_unsigned("direct_pull_bytes", m_direct_bytes.load());
  f->dump_unsigned("sources_registered", m_sources.load());
  f->dump_unsigned("source_failures", m_source_failures.load());
  f->dump_unsigned("source_quarantines", m_source_quarantines.load());
  f->close_section();
}
