// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd_cuobj.h"

#include <cuobjserver.h>

#include <algorithm>
#include <cstdlib>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
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
