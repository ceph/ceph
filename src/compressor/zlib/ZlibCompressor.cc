/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Alyona Kiseleva <akiselyova@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

// -----------------------------------------------------------------------------
#include "common/debug.h"
#include "ZlibCompressor.h"
#include "osd/osd_types.h"
#include "isa-l/include/igzip_lib.h"
#ifdef HAVE_QATZIP
  #include "compressor/QatAccel.h"
#endif
// -----------------------------------------------------------------------------

#include <zlib.h>

// -----------------------------------------------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix _prefix(_dout)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

using std::ostream;

using ceph::bufferlist;
using ceph::bufferptr;

static ostream&
_prefix(std::ostream* _dout)
{
  return *_dout << "ZlibCompressor: ";
}
// -----------------------------------------------------------------------------

#define MAX_LEN (CEPH_PAGE_SIZE)

// default window size for Zlib 1.2.8, negated for raw deflate
#define ZLIB_DEFAULT_WIN_SIZE -15

// desired memory usage level. increasing to 9 doesn't speed things up
// significantly (helps only on >=16K blocks) and sometimes degrades
// compression ratio.
#define ZLIB_MEMORY_LEVEL 8

#ifdef HAVE_QATZIP
QatAccel ZlibCompressor::qat_accel;
#endif

ZlibCompressor::ZlibCompressor(CephContext *cct, bool isal)
  : Compressor(COMP_ALG_ZLIB, "zlib"), isal_enabled(isal), cct(cct)
{
#ifdef HAVE_QATZIP
  if (cct->_conf->qat_compressor_enabled && qat_accel.init("zlib"))
    qat_enabled = true;
  else
    qat_enabled = false;
#endif
}

int ZlibCompressor::zlib_compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message)
{
  int ret;
  unsigned have;
  z_stream strm;
  unsigned char* c_in;
  int begin = 1;

  /* allocate deflate state */
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  ret = deflateInit2(&strm, cct->_conf->compressor_zlib_level, Z_DEFLATED, cct->_conf->compressor_zlib_winsize, ZLIB_MEMORY_LEVEL, Z_DEFAULT_STRATEGY);
  if (ret != Z_OK) {
    dout(1) << "Compression init error: init return "
         << ret << " instead of Z_OK" << dendl;
    return -1;
  }
  compressor_message = cct->_conf->compressor_zlib_winsize;

  for (ceph::bufferlist::buffers_t::const_iterator i = in.buffers().begin();
      i != in.buffers().end();) {

    c_in = (unsigned char*) (*i).c_str();
    long unsigned int len = (*i).length();
    ++i;

    strm.avail_in = len;
    int flush = i != in.buffers().end() ? Z_NO_FLUSH : Z_FINISH;

    strm.next_in = c_in;
    do {
      bufferptr ptr = ceph::buffer::create_page_aligned(MAX_LEN);
      strm.next_out = (unsigned char*)ptr.c_str() + begin;
      strm.avail_out = MAX_LEN - begin;
      if (begin) {
        // put a compressor variation mark in front of compressed stream, not used at the moment
        ptr.c_str()[0] = 0;
        begin = 0;
      }
      ret = deflate(&strm, flush);    /* no bad return value */
      if (ret == Z_STREAM_ERROR) {
         dout(1) << "Compression error: compress return Z_STREAM_ERROR("
              << ret << ")" << dendl;
         deflateEnd(&strm);
         return -1;
      }
      have = MAX_LEN - strm.avail_out;
      out.append(ptr, 0, have);
    } while (strm.avail_out == 0);
    if (strm.avail_in != 0) {
      dout(10) << "Compression error: unused input" << dendl;
      deflateEnd(&strm);
      return -1;
    }
  }

  deflateEnd(&strm);
  return 0;
}

#if (__x86_64__ && defined(HAVE_NASM_X64_AVX2)) || defined(__aarch64__)
int ZlibCompressor::isal_compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message)
{
  int ret;
  unsigned have;
  isal_zstream strm;
  unsigned char* c_in;
  int begin = 1;

  /* allocate deflate state */
  isal_deflate_init(&strm);
  strm.end_of_stream = 0;
  compressor_message = ZLIB_DEFAULT_WIN_SIZE;

  for (ceph::bufferlist::buffers_t::const_iterator i = in.buffers().begin();
      i != in.buffers().end();) {

    c_in = (unsigned char*) (*i).c_str();
    long unsigned int len = (*i).length();
    ++i;

    strm.avail_in = len;
    strm.end_of_stream = (i == in.buffers().end());
    strm.flush = FINISH_FLUSH;

    strm.next_in = c_in;

    do {
      bufferptr ptr = ceph::buffer::create_page_aligned(MAX_LEN);
      strm.next_out = (unsigned char*)ptr.c_str() + begin;
      strm.avail_out = MAX_LEN - begin;
      if (begin) {
        // put a compressor variation mark in front of compressed stream, not used at the moment
        ptr.c_str()[0] = 1;
        begin = 0;
      }
      ret = isal_deflate(&strm);
      if (ret != COMP_OK) {
         dout(1) << "Compression error: isal_deflate return error ("
              << ret << ")" << dendl;
         return -1;
      }
      have = MAX_LEN - strm.avail_out;
      out.append(ptr, 0, have);
    } while (strm.avail_out == 0);
    if (strm.avail_in != 0) {
      dout(10) << "Compression error: unused input" << dendl;
      return -1;
    }
  }

  return 0;  
}
#endif

int ZlibCompressor::compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message)
{
#ifdef HAVE_QATZIP
  if (qat_enabled)
    return qat_accel.compress(in, out, compressor_message);
#endif
#if (__x86_64__ && defined(HAVE_NASM_X64_AVX2)) || defined(__aarch64__)
  if (isal_enabled)
    return isal_compress(in, out, compressor_message);
  else
    return zlib_compress(in, out, compressor_message);
#else
  return zlib_compress(in, out, compressor_message);
#endif
}

int ZlibCompressor::decompress(bufferlist::const_iterator &p, size_t compressed_size, bufferlist &out, std::optional<int32_t> compressor_message)
{
#ifdef HAVE_QATZIP
  // QAT can only decompress with the default window size
  if (qat_enabled && (!compressor_message || *compressor_message == ZLIB_DEFAULT_WIN_SIZE))
    return qat_accel.decompress(p, compressed_size, out, compressor_message);
#endif

  int ret;
  unsigned have;
  z_stream strm;
  const char* c_in;
  int begin = 1;

  /* allocate inflate state */
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = 0;
  strm.next_in = Z_NULL;

  // choose the variation of compressor
  if (!compressor_message)
    compressor_message = ZLIB_DEFAULT_WIN_SIZE;
  ret = inflateInit2(&strm, *compressor_message);
  if (ret != Z_OK) {
    dout(1) << "Decompression init error: init return "
         << ret << " instead of Z_OK" << dendl;
    return -1;
  }

  size_t remaining = std::min<size_t>(p.get_remaining(), compressed_size);

  while(remaining) {
    long unsigned int len = p.get_ptr_and_advance(remaining, &c_in);
    remaining -= len;
    strm.avail_in = len - begin;
    strm.next_in = (unsigned char*)c_in + begin;
    begin = 0;

    do {
      strm.avail_out = MAX_LEN;
      bufferptr ptr = ceph::buffer::create_page_aligned(MAX_LEN);
      strm.next_out = (unsigned char*)ptr.c_str();
      ret = inflate(&strm, Z_NO_FLUSH);
      if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
       dout(1) << "Decompression error: decompress return "
            << ret << dendl;
       inflateEnd(&strm);
       return -1;
      }
      have = MAX_LEN - strm.avail_out;
      out.append(ptr, 0, have);
    } while (strm.avail_out == 0);
  }

  /* clean up and return */
  (void)inflateEnd(&strm);
  return 0;
}

int ZlibCompressor::decompress(const bufferlist &in, bufferlist &out, std::optional<int32_t> compressor_message)
{
  auto i = std::cbegin(in);
  return decompress(i, in.length(), out, compressor_message);
}
