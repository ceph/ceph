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
#include "zlib.h"

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
// default window size for Zlib 1.2.8, negated for raw deflate
#define ZLIB_DEFAULT_WIN_SIZE -15
#define GZIP_WRAPPER 16

/* Estimate data expansion after decompression */
static const unsigned int expansion_ratio[] = {5, 20, 50, 100, 200, 1000, 10000};

void QzSessionDeleter::operator() (struct QzSession_S *session) {
  qzTeardownSession(session);
  delete session;
}

QzPollingMode_T busy_polling(bool isSet) {
  return isSet ? QZ_BUSY_POLLING : QZ_PERIODICAL_POLLING;
}

static bool setup_session(const std::string &alg, QatAccel::session_ptr &session) {
  int rc;
  rc = qzInit(session.get(), QZ_SW_BACKUP_DEFAULT);
  if (rc != QZ_OK && rc != QZ_DUPLICATE)
    return false;
  if (alg == "zlib") {
    QzSessionParamsDeflate_T params;
    rc = qzGetDefaultsDeflate(&params);
    if (rc != QZ_OK)
      return false;

    params.data_fmt = QZ_DEFLATE_GZIP_EXT;
    params.common_params.comp_algorithm = QZ_DEFLATE;
    params.common_params.comp_lvl = g_ceph_context->_conf->compressor_zlib_level;
    params.common_params.direction = QZ_DIR_BOTH;
    params.common_params.polling_mode = busy_polling(g_ceph_context->_conf.get_val<bool>("qat_compressor_busy_polling"));
    rc = qzSetupSessionDeflate(session.get(), &params);
    if (rc != QZ_OK)
      return false;
  }
  else {
    // later, there also has lz4.
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
  session_ptr session(new struct QzSession_S());
  memset(session.get(), 0, sizeof(struct QzSession_S));
  if (setup_session(alg_name, session)) {
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
  windowBits = GZIP_WRAPPER + MAX_WBITS;

  return true;
}

int QatAccel::compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message) {
  dout(20) << "QAT compress" << dendl;
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction
  compressor_message = windowBits;

  int begin = 1;
  for (auto &i : in.buffers()) {
    const unsigned char* c_in = (unsigned char*) i.c_str();
    unsigned int len = i.length();
    unsigned int out_len = qzMaxCompressedLength(len, session.get()) + begin;

    bufferptr ptr = buffer::create_small_page_aligned(out_len);
    unsigned char* c_out = (unsigned char*)ptr.c_str() + begin;
    QzSession_T *sess = session.get();
    int rc = qzCompress(sess, c_in, &len, c_out, &out_len, 1);
    if(sess->hw_session_stat != QZ_OK) {
      if(sess->hw_session_stat == QZ_NO_HW) {
        dout(1) << "QAT compressor NOT OK - Using SW: No QAT HW detected" << dendl;
      } else {
        dout(1) << "QAT compressor NOT OK - session state=" << sess->hw_session_stat << dendl;
      }
    }
    if (rc != QZ_OK)
      return -1;
    if (begin) {
      // put a compressor variation mark in front of compressed stream, not used at the moment
      ptr.c_str()[0] = 0;
      out_len += begin;
      begin = 0;
    }
    out.append(ptr, 0, out_len);

  }

  return 0;
}

int QatAccel::decompress(const bufferlist &in, bufferlist &out, std::optional<int32_t> compressor_message) {
  auto i = in.begin();
  return decompress(i, in.length(), out, compressor_message);
}

int QatAccel::decompress(bufferlist::const_iterator &p,
		 size_t compressed_len,
		 bufferlist &dst,
		 std::optional<int32_t> compressor_message) {
  dout(20) << "QAT decompress" << dendl;
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction
  int begin = 1;

  int rc = 0;
  bufferlist tmp;
  unsigned int ratio_idx = 0;
  const char* c_in = nullptr;
  p.copy_all(tmp);
  c_in = tmp.c_str();
  unsigned int len = std::min<unsigned int>(tmp.length(), compressed_len);

  len -= begin;
  c_in += begin;
  begin = 0;

  bufferptr ptr;
  do {
    unsigned int out_len = QZ_HW_BUFF_SZ;
    unsigned int len_current = len;
    do {
      while (out_len <= len_current * expansion_ratio[ratio_idx]) {
        out_len *= 2;
      }

      ptr = buffer::create_small_page_aligned(out_len);
      QzSession_T *sess = session.get();
      rc = qzDecompress(sess, (const unsigned char*)c_in, &len_current, (unsigned char*)ptr.c_str(), &out_len);
      if(sess->hw_session_stat != QZ_OK) {
        if(sess->hw_session_stat == QZ_NO_HW) {
          dout(1) << "QAT decompress NOT OK - Using SW: No QAT HW detected" << dendl;
        } else {
          dout(1) << "QAT decompress NOT OK - session state=" << sess->hw_session_stat << dendl;
        }
      }
      ratio_idx++;
    } while (rc == QZ_BUF_ERROR && ratio_idx < std::size(expansion_ratio));
    c_in += len_current;
    len -= len_current;

    if (rc == QZ_OK) {
      dst.append(ptr, 0, out_len);
    } else if (rc == QZ_DATA_ERROR) {
      dout(1) << "QAT compressor DATA ERROR" << dendl;
      return -1;
    } else if (rc == QZ_BUF_ERROR) {
      dout(1) << "QAT compressor BUF ERROR" << dendl;
      return -1;
    } else if (rc != QZ_OK) {
      dout(1) << "QAT compressor NOT OK" << dendl;
      return -1;
    }

  } while (len != 0);
  return 0;
}
