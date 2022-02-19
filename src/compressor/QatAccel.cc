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
#include "QatAccel.h"

void QzSessionDeleter::operator() (struct QzSession_S *session) {
  if (NULL != session->internal) {
    qzTeardownSession(session);
    qzClose(session);
  }
  delete session;
}

/* Estimate data expansion after decompression */
static const unsigned int expansion_ratio[] = {5, 20, 50, 100, 200};

QatAccel::QatAccel() {
  session.reset(new struct QzSession_S);
  memset(session.get(), 0, sizeof(struct QzSession_S));
}

QatAccel::~QatAccel() {}

bool QatAccel::init(const std::string &alg) {
  QzSessionParams_T params = {(QzHuffmanHdr_T)0,};
  int rc;

  rc = qzGetDefaults(&params);
  if (rc != QZ_OK)
    return false;
  params.direction = QZ_DIR_BOTH;
  if (alg == "zlib")
    params.comp_algorithm = QZ_DEFLATE;
  else
    return false;

  rc = qzSetDefaults(&params);
  if (rc != QZ_OK)
      return false;

  rc = qzInit(session.get(), QZ_SW_BACKUP_DEFAULT);
  if (rc != QZ_OK && rc != QZ_DUPLICATE && rc != QZ_NO_HW)
    return false;

  rc = qzSetupSession(session.get(), &params);
  if (rc != QZ_OK && rc != QZ_DUPLICATE && rc != QZ_NO_HW ) {
    qzTeardownSession(session.get());
    qzClose(session.get());
    return false;
  }

  return true;
}

int QatAccel::compress(const bufferlist &in, bufferlist &out, boost::optional<int32_t> &compressor_message) {
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
  unsigned int ratio_idx = 0;
  bool read_more = false;
  bool joint = false;
  int rc = 0;
  bufferlist tmp;
  size_t remaining = MIN(p.get_remaining(), compressed_len);

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
        p += remaining;
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

    p += remaining;
    remaining -= len;
    dst.append(ptr, 0, out_len);
  }

  return 0;
}
