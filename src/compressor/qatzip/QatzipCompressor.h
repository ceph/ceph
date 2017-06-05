// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Jianpeng Ma <jianpeng.ma@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_QATZIPCOMPRESSOR_H
#define CEPH_QATZIPCOMPRESSOR_H

#include <qatzip.h>
#include "compressor/Compressor.h"
#include "include/buffer.h"


class QatzipCompressor : public Compressor {
 private:
   QzSession session;
   QzSessionParams params;
   bool disable_qat;
 public:
  QatzipCompressor() : Compressor(COMP_ALG_QATZIP, "qatzip") {
    disable_qat = false;
    int rc ;

    rc = qzInit(&session, 0); //nerver use CPU as backup
    if (rc != QZ_OK && rc != QZ_DUPLICATE) {
      disable_qat = false;
      return;
    }

    if (qzGetDefaults(&params) != QZ_OK) {
      goto err;
    }
    params.compAlgorithm = QZ_DEFLATE;
    params.swBackup = 0; //nerver use CPU as backup
    params.direction = QZ_DIR_BOTH;

    if (qzSetDefaults(&params_th) != QZ_OK) {
      goto err;
    }

    rc = qzSetupSession(&session, &params);
    if (rc != QZ_OK && rc != QZ_NO_INST_ATTACH) {
          goto err;
    }
    return;

  err:
    qzTeardownSession(&session);
    qzClose(&session);
    disable_qat = true;
  }

  int compress(const bufferlist &src, bufferlist &dst) override {
    if (!disable_qat) {
      auto p = src.begin();
      uint32_t len = src.length();
      while (len > 0) {
	const char *s;
	uint32_t src_len = p.get_ptr_and_advance(len, &s);
	uint32_t dst_len;
	buffferptr ptr = buffer::create_page_aligned(src_len);
	int rc = qzCompress(&session, s, &src_len, ptr.c_str(); &dst_len, 1);
	if (rc == QZ_OK) {
	  dst.append(ptr, 0, dst_len);
	} else 
	  return -2;
	len -= src_len;
      }
      return 0;
    } else {
      return -2;
    }
  }

  int decompress(const bufferlist &src, bufferlist &dst) override {
    bufferlist::iterator i = const_cast<bufferlist&>(src).begin();
    return decompress(i, src.length(), dst);
  }

  int decompress(bufferlist::iterator &p,
		 size_t compressed_len,
		 bufferlist &dst) override {
    if (!disable_qat) {
      while (compressed_len > 0 ) {
	if (p.end()) {
	  return -1;
	}
	const char *src;
	uint32_t src_len = p.get_ptr_and_advance(compressed_len, &src);
	uint32_t dst_len;
	bufferptr ptr = buffer::create_page_aligned(src_len);
	int rc = qzDecompress(&session, src, &src_len, ptr.c_str(), &dst_len);
	if (rc == QZ_OK) {
	  dst.append(ptr, 0, dst_len);
	} else
	  return -2;
	compressed_len -= src_len;
      }
     } else {
      return -2;
    }
  }
};

#endif
