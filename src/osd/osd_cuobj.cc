// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd_cuobj.h"

#include <cuobjserver.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <thread>

#include "common/ceph_context.h"
#include "common/Clock.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "common/rdma_token.h"

#define dout_context m_cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_cuobj "

thread_local uint16_t OSDCuObj::tls_channel_id = 0;
thread_local bool OSDCuObj::tls_channel_valid = false;

// per-call limit of the cuObj API
static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30;

OSDCuObj::OSDCuObj(CephContext *cct, const std::string& rdma_ip,
		   uint16_t rdma_port)
  : m_cct(cct)
{
  if (do_init(rdma_ip, rdma_port) < 0) {
    do_shutdown();
  }
}

OSDCuObj::~OSDCuObj()
{
  do_shutdown();
}

int OSDCuObj::do_init(const std::string& rdma_ip, uint16_t rdma_port)
{
  auto num_dcis = static_cast<int>(
    m_cct->_conf.get_val<uint64_t>("osd_cuobj_num_dcis"));
  auto dc_key = m_cct->_conf.get_val<uint64_t>("osd_cuobj_dc_key");
  auto buf_size = static_cast<size_t>(
    m_cct->_conf.get_val<Option::size_t>("osd_cuobj_buffer_size"));
  auto buf_count = static_cast<size_t>(
    m_cct->_conf.get_val<uint64_t>("osd_cuobj_buffer_count"));

  cuObjRDMATunable params;
  params.setNumDcis(num_dcis);
  params.setDcKey(dc_key);

  dout(1) << "initializing cuObjServer on " << rdma_ip << ":" << rdma_port
	  << " dcis=" << num_dcis
	  << " bufs=" << buf_count << "x" << buf_size << dendl;

  try {
    m_server = std::make_unique<cuObjServer>(
      rdma_ip.c_str(), rdma_port, CUOBJ_PROTO_RDMA_DC_V1, params);
  } catch (const std::exception& e) {
    derr << "ERROR: cuObjServer construction failed: " << e.what() << dendl;
    return -EIO;
  }

  if (!m_server->isConnected()) {
    derr << "ERROR: cuObjServer RDMA session failed to start" << dendl;
    m_server.reset();
    return -ECONNREFUSED;
  }

  m_buf_size = buf_size;
  m_pool = std::make_unique<BufEntry[]>(buf_count);
  for (size_t i = 0; i < buf_count; i++) {
    auto& entry = m_pool[i];
    entry.ptr = m_server->allocHostBuffer(buf_size);
    if (!entry.ptr) {
      derr << "ERROR: allocHostBuffer failed for buffer " << i << dendl;
      return -ENOMEM;
    }
    entry.size = buf_size;
    entry.handle = m_server->registerBuffer(entry.ptr, buf_size);
    if (!entry.handle) {
      derr << "ERROR: registerBuffer failed for buffer " << i << dendl;
      free(entry.ptr);
      entry.ptr = nullptr;
      return -EIO;
    }
    entry.in_use.store(false, std::memory_order_relaxed);
    m_pool_count = i + 1;
  }

  dout(1) << "initialized with " << m_pool_count << " RDMA buffers of "
	  << buf_size << " bytes" << dendl;
  return 0;
}

void OSDCuObj::do_shutdown()
{
  for (size_t i = 0; i < m_pool_count; i++) {
    auto& entry = m_pool[i];
    if (entry.handle && m_server) {
      m_server->deRegisterBuffer(entry.handle);
      entry.handle = nullptr;
    }
    if (entry.ptr) {
      free(entry.ptr);
      entry.ptr = nullptr;
    }
  }
  m_pool.reset();
  m_pool_count = 0;
  m_server.reset();
}

bool OSDCuObj::is_available() const
{
  return m_server && m_server->isConnected();
}

uint16_t OSDCuObj::get_channel_id()
{
  if (!tls_channel_valid) {
    uint16_t id = m_server->allocateChannelId();
    if (id == invalid_channel) {
      derr << "ERROR: cuObject channel allocation failed"
	   << " (raise osd_cuobj_num_dcis?)" << dendl;
      return invalid_channel;
    }
    tls_channel_id = id;
    tls_channel_valid = true;
    dout(20) << "allocated channel " << id << " for this thread" << dendl;
  }
  return tls_channel_id;
}

OSDCuObj::BufEntry* OSDCuObj::acquire_buffer(size_t needed, bool* transient)
{
  for (size_t i = 0; i < m_pool_count; i++) {
    auto& entry = m_pool[i];
    if (entry.size >= needed) {
      bool expected = false;
      if (entry.in_use.compare_exchange_strong(expected, true,
					       std::memory_order_acquire)) {
	*transient = false;
	return &entry;
      }
    }
  }
  // pool exhausted (or request larger than any pooled buffer):
  // register a one-shot buffer rather than failing the op, but bound
  // it - registration pins memory and the request size is
  // client-controlled up to osd_max_object_size
  if (needed > m_buf_size * 4) {
    derr << "ERROR: " << needed << " bytes exceeds the transient RDMA "
	 << "registration cap (" << m_buf_size * 4
	 << "); raise osd_cuobj_buffer_size" << dendl;
    return nullptr;
  }
  dout(10) << "buffer pool exhausted, registering transient buffer of "
	   << needed << " bytes" << dendl;
  auto entry = new BufEntry;
  entry->ptr = m_server->allocHostBuffer(needed);
  if (!entry->ptr) {
    delete entry;
    return nullptr;
  }
  entry->size = needed;
  entry->handle = m_server->registerBuffer(entry->ptr, needed);
  if (!entry->handle) {
    free(entry->ptr);
    delete entry;
    return nullptr;
  }
  *transient = true;
  return entry;
}

void OSDCuObj::release_buffer(BufEntry* buf, bool transient)
{
  if (!buf) {
    return;
  }
  if (transient) {
    m_server->deRegisterBuffer(buf->handle);
    free(buf->ptr);
    delete buf;
  } else {
    buf->in_use.store(false, std::memory_order_release);
  }
}

ssize_t OSDCuObj::rdma_write(const std::string& key,
			     const ceph::buffer::list& bl,
			     const std::string& token,
			     uint64_t client_offset)
{
  if (!is_available()) {
    return -EOPNOTSUPP;
  }
  auto window = ceph::rdma::parse_rdma_token(token);
  if (!window) {
    dout(5) << "malformed RDMA token for " << key << dendl;
    return -EINVAL;
  }
  const size_t len = bl.length();
  if (len == 0) {
    return 0;
  }
  if (client_offset > window->size || len > window->size - client_offset) {
    dout(5) << "target range " << client_offset << "~" << len
	    << " outside client window of " << window->size
	    << " bytes for " << key << dendl;
    return -EINVAL;
  }
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    return -EIO;
  }
  bool transient = false;
  BufEntry* buf = acquire_buffer(len, &transient);
  if (!buf) {
    derr << "ERROR: no RDMA buffer available for " << len << " bytes" << dendl;
    return -ENOMEM;
  }
  auto it = bl.begin();
  it.copy(len, static_cast<char*>(buf->ptr));

  const uint64_t remote_addr = window->addr + client_offset;
  size_t total = 0;
  ssize_t ret = 0;
  dout(20) << "handleGetObject key=" << key << " len=" << len
	   << " client_offset=" << client_offset
	   << " channel=" << channel << dendl;
  while (total < len) {
    size_t chunk = std::min(len - total, MAX_RDMA_OP_SIZE);
    ibv_wc_status wc_status = IBV_WC_SUCCESS;
    ret = m_server->handleGetObject(key, buf->handle, remote_addr + total,
				    chunk, token, channel, total, &wc_status);
    if (ret < 0) {
      derr << "ERROR: handleGetObject failed for " << key
	   << ": ret=" << ret
	   << " wc_status=" << static_cast<int>(wc_status)
	   << " chunk=" << chunk << " local_offset=" << total << dendl;
      break;
    }
    total += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }
  release_buffer(buf, transient);
  if (ret < 0) {
    // normalize the library's errno-style returns
    return ret == -EOPNOTSUPP ? -EIO : ret;
  }
  dout(20) << "RDMA wrote " << total << " bytes for " << key << dendl;
  return static_cast<ssize_t>(total);
}

ssize_t OSDCuObj::execute_plan(const std::string& key,
			       const std::string& token,
			       const ceph::buffer::list& data,
			       const ceph::osd::oob::placement_plan& plan)
{
  if (!is_available()) {
    return -EOPNOTSUPP;
  }
  auto window = ceph::rdma::parse_rdma_token(token);
  if (!window) {
    dout(5) << "malformed RDMA token for " << key << dendl;
    return -EINVAL;
  }
  // expand triples into <=1 GiB work items, validating up front
  struct work_item {
    uint64_t local_ofs;   // offset into the staged buffer
    uint64_t remote_ofs;  // offset into the client window
    uint64_t len;
  };
  std::vector<work_item> items;
  uint64_t total = 0;
  for (const auto& t : plan) {
    if (t.len == 0) {
      continue;
    }
    if (t.client_ofs > window->size || t.len > window->size - t.client_ofs ||
	t.reply_data_ofs > data.length() ||
	t.len > data.length() - t.reply_data_ofs) {
      dout(5) << "placement triple " << t.reply_data_ofs << "/" << t.client_ofs
	      << "~" << t.len << " outside window (" << window->size
	      << ") or data (" << data.length() << ") for " << key << dendl;
      return -EINVAL;
    }
    for (uint64_t done = 0; done < t.len; ) {
      const uint64_t chunk = std::min(t.len - done, MAX_RDMA_OP_SIZE);
      items.push_back({t.reply_data_ofs + done, t.client_ofs + done, chunk});
      done += chunk;
    }
    total += t.len;
  }
  if (items.empty()) {
    return 0;
  }
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    return -EIO;
  }
  bool transient = false;
  BufEntry* buf = acquire_buffer(data.length(), &transient);
  if (!buf) {
    derr << "ERROR: no RDMA buffer available for " << data.length()
	 << " bytes" << dendl;
    return -ENOMEM;
  }
  {
    auto it = data.begin();
    it.copy(data.length(), static_cast<char*>(buf->ptr));
  }

  m_plans_started++;
  dout(20) << "executing plan for " << key << ": " << items.size()
	   << " writes, " << total << " bytes, channel " << channel << dendl;

  // batched async submission: at most POLL_BATCH outstanding, polled
  // to completion on the same channel (the library caps poll() at 16
  // events and documents no larger per-channel bound)
  constexpr int POLL_BATCH = 16;
  const utime_t deadline = ceph_clock_now() + utime_t(60, 0);
  size_t next = 0;
  size_t outstanding = 0;
  size_t completed = 0;
  ssize_t err = 0;
  while (completed < items.size()) {
    while (err == 0 && next < items.size() && outstanding < POLL_BATCH) {
      auto& w = items[next];
      ssize_t r = m_server->handleGetObject(
	key, buf->handle, window->addr + w.remote_ofs, w.len, token, channel,
	w.local_ofs, nullptr, /*async_handle=*/&items[next]);
      if (r < 0) {
	derr << "ERROR: async handleGetObject submission failed for " << key
	     << ": " << r << dendl;
	err = r;
	break;
      }
      next++;
      outstanding++;
      m_writes_inflight++;
    }
    if (outstanding == 0) {
      break;  // submission failed before anything went out
    }
    cuObjAsyncEvent_t events[POLL_BATCH];
    for (auto& e : events) {
      e.async_handle = nullptr;
    }
    int n = m_server->poll(events, POLL_BATCH, channel);
    if (n == 0) {
      // nothing completed yet; don't hot-spin the op worker
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      if (ceph_clock_now() > deadline) {
	derr << "ERROR: plan for " << key << " timed out with " << outstanding
	     << " writes outstanding; leaking the staging buffer" << dendl;
	m_writes_inflight -= outstanding;
	m_buffers_leaked++;
	m_plans_failed++;
	return -ETIMEDOUT;
      }
      continue;
    }
    if (n < 0) {
      // on -EIO the return is NOT a completion count and the library
      // has reset the QP, flushing the remaining writes; scan for the
      // events that were filled, then abandon the plan
      for (const auto& e : events) {
	if (e.async_handle) {
	  outstanding--;
	  m_writes_inflight--;
	  completed++;
	}
      }
      derr << "ERROR: poll failed for " << key << ": " << n << dendl;
      err = err ? err : -EIO;
      // after a QP reset nothing more will complete; count the rest
      // as flushed
      m_writes_inflight -= outstanding;
      completed += outstanding;
      outstanding = 0;
      break;
    }
    for (int i = 0; i < n; i++) {
      if (!events[i].async_handle) {
	continue;
      }
      outstanding--;
      m_writes_inflight--;
      completed++;
      if (events[i].status != 0 /* IBV_WC_SUCCESS */) {
	derr << "ERROR: RDMA write completion failed for " << key
	     << ": wc_status=" << events[i].status << dendl;
	err = err ? err : -EIO;
      }
    }
    if (ceph_clock_now() > deadline) {
      // wedged transport: we cannot release the staged buffer while
      // writes may still reference it - leak it deliberately
      derr << "ERROR: plan for " << key << " timed out with " << outstanding
	   << " writes outstanding; leaking the staging buffer" << dendl;
      m_writes_inflight -= outstanding;
      m_buffers_leaked++;
      m_plans_failed++;
      return -ETIMEDOUT;
    }
  }
  release_buffer(buf, transient);
  if (err < 0) {
    m_plans_failed++;
    return err == -EOPNOTSUPP ? -EIO : err;
  }
  m_plans_completed++;
  m_bytes_pushed += total;
  dout(20) << "plan for " << key << " pushed " << total << " bytes" << dendl;
  return static_cast<ssize_t>(total);
}

void OSDCuObj::dump_stats(ceph::Formatter* f) const
{
  f->dump_bool("available", is_available());
  f->dump_unsigned("plans_started", m_plans_started.load());
  f->dump_unsigned("plans_completed", m_plans_completed.load());
  f->dump_unsigned("plans_failed", m_plans_failed.load());
  f->dump_unsigned("bytes_pushed", m_bytes_pushed.load());
  f->dump_unsigned("writes_inflight", m_writes_inflight.load());
  f->dump_unsigned("buffers_leaked", m_buffers_leaked.load());
}
