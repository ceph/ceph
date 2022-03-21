/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corporation
 *
 * Author: Qiaowei Ren <qiaowei.ren@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <qatzip.h>

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "QatAccel.h"

// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "QatAccel: ";
}
// -----------------------------------------------------------------------------

void QzSessionDeleter::operator() (struct QzSession_S *session) {
  qzTeardownSession(session);
  delete session;
}

/* Estimate data expansion after decompression */
static const unsigned int expansion_ratio[] = {5, 20, 50, 100, 200};

static bool get_qz_params(const std::string &alg, QzSessionParams_T &params) {
  int rc;
  rc = qzGetDefaults(&params);
  if (rc != QZ_OK)
    return false;
  params.direction = QZ_DIR_BOTH;
  params.is_busy_polling = true;
  if (alg == "zlib") {
    params.comp_algorithm = QZ_DEFLATE;
    params.data_fmt = QZ_DEFLATE_GZIP_EXT;
  }
  else {
    // later, there also has lz4.
    return false;
  }

  rc = qzSetDefaults(&params);
  if (rc != QZ_OK)
      return false;
  return true;
}

static bool setup_session(QatAccel::session_ptr &session, QzSessionParams_T &params) {
  int rc;
  rc = qzInit(session.get(), QZ_SW_BACKUP_DEFAULT);
  if (rc != QZ_OK && rc != QZ_DUPLICATE)
    return false;
  rc = qzSetupSession(session.get(), &params);
  if (rc != QZ_OK) {
    return false;
  }
  return true;
}

// put the session back to the session pool in a RAII manner
struct cached_session_t {
  cached_session_t(QatAccel* accel, QatAccel::session_ptr&& sess)
    : accel{accel}, session{std::move(sess)} {}

  ~cached_session_t() {
    std::scoped_lock lock{accel->mutex};
    // if the cache size is still under its upper bound, the current session is put into
    // accel->sessions. otherwise it's released right
    uint64_t sessions_num = g_ceph_context->_conf.get_val<uint64_t>("qat_compressor_session_max_number");
    if (accel->sessions.size() < sessions_num) {
      accel->sessions.push_back(std::move(session));
    }
  }

  struct QzSession_S* get() {
    assert(static_cast<bool>(session));
    return session.get();
  }

  QatAccel* accel;
  QatAccel::session_ptr session;
};

QatAccel::session_ptr QatAccel::get_session() {
  {
    std::scoped_lock lock{mutex};
    if (!sessions.empty()) {
      auto session = std::move(sessions.back());
      sessions.pop_back();
      return session;
    }
  }

  // If there are no available session to use, we try allocate a new
  // session.
  QzSessionParams_T params = {(QzHuffmanHdr_T)0,};
  session_ptr session(new struct QzSession_S());
  memset(session.get(), 0, sizeof(struct QzSession_S));
  if (get_qz_params(alg_name, params) && setup_session(session, params)) {
    return session;
  } else {
    return nullptr;
  }
}

QatAccel::QatAccel() {}

QatAccel::~QatAccel() {
  // First, we should uninitialize all QATzip session that disconnects all session
  // from a hardware instance and deallocates buffers.
  sessions.clear();
  // Then we close the connection with QAT.
  // where the value of the parameter passed to qzClose() does not matter. as long as
  // it is not nullptr.
  qzClose((QzSession_T*)1);
}

bool QatAccel::init(const std::string &alg) {
  std::scoped_lock lock(mutex);
  if (!alg_name.empty()) {
    return true;
  }

  dout(15) << "First use for QAT compressor" << dendl;
  if (alg != "zlib") {
    return false;
  }

  alg_name = alg;
  return true;
}

int QatAccel::compress(const bufferlist &in, bufferlist &out, boost::optional<int32_t> &compressor_message) {
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction

  for (auto &i : in.buffers()) {
    const unsigned char* c_in = (unsigned char*) i.c_str();
    unsigned int len = i.length();
    unsigned int out_len = qzMaxCompressedLength(len, session.get());

    bufferptr ptr = buffer::create_small_page_aligned(out_len);
    int rc = qzCompress(session.get(), c_in, &len, (unsigned char *)ptr.c_str(), &out_len, 1);
    if (rc != QZ_OK)
      return -1;
    out.append(ptr, 0, out_len);
  }

  return 0;
}

int QatAccel::decompress(const bufferlist &in, bufferlist &out, boost::optional<int32_t> compressor_message) {
  auto i = in.begin();
  return decompress(i, in.length(), out, compressor_message);
}

int QatAccel::decompress(bufferlist::const_iterator &p,
		 size_t compressed_len,
		 bufferlist &dst,
		 boost::optional<int32_t> compressor_message) {
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction

  unsigned int ratio_idx = 0;
  bool read_more = false;
  bool joint = false;
  int rc = 0;
  bufferlist tmp;
  size_t remaining = std::min<size_t>(p.get_remaining(), compressed_len);

  while (remaining) {
    if (p.end()) {
      return -1;
    }

    bufferptr cur_ptr = p.get_current_ptr();
    unsigned int len = cur_ptr.length();
    if (joint) {
      if (read_more)
        tmp.append(cur_ptr.c_str(), len);
      len = tmp.length();
      tmp.rebuild_page_aligned();
    }

    unsigned int out_len = len * expansion_ratio[ratio_idx];
    bufferptr ptr = buffer::create_small_page_aligned(out_len);

    if (joint)
      rc = qzDecompress(session.get(), (const unsigned char*)tmp.c_str(), &len, (unsigned char*)ptr.c_str(), &out_len);
    else
      rc = qzDecompress(session.get(), (const unsigned char*)cur_ptr.c_str(), &len, (unsigned char*)ptr.c_str(), &out_len);
    if (rc == QZ_DATA_ERROR) {
      if (!joint) {
        tmp.append(cur_ptr.c_str(), cur_ptr.length());
        p += cur_ptr.length();
        remaining -= cur_ptr.length();
        joint = true;
      }
      read_more = true;
      continue;
    } else if (rc == QZ_BUF_ERROR) {
      if (ratio_idx == std::size(expansion_ratio))
        return -1;
      if (joint)
        read_more = false;
      ratio_idx++;
      continue;
    } else if (rc != QZ_OK) {
      return -1;
    } else {
      ratio_idx = 0;
      joint = false;
      read_more = false;
    }

    p += cur_ptr.length();
    remaining -= cur_ptr.length();
    dst.append(ptr, 0, out_len);
  }

  return 0;
}
