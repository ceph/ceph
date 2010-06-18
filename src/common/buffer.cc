// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "config.h"
#include "include/types.h"
#include "armor.h"
#include "include/Spinlock.h"

#include <errno.h>
#include <fstream>

namespace ceph {

Spinlock buffer_lock("buffer_lock");
atomic_t buffer_total_alloc;

void buffer::list::encode_base64(buffer::list& o)
{
  bufferptr bp(length() * 4 / 3 + 3);
  int l = ceph_armor(bp.c_str(), c_str(), c_str() + length());
  bp.set_length(l);
  o.push_back(bp);
}

void buffer::list::decode_base64(buffer::list& e)
{
  bufferptr bp(e.length() * 3 / 4 + 4);
  int l = ceph_unarmor(bp.c_str(), e.c_str(), e.c_str() + e.length());
  assert(l <= (int)bp.length());
  bp.set_length(l);
  push_back(bp);
}

void buffer::list::rebuild_page_aligned()
{
  std::list<ptr>::iterator p = _buffers.begin();
  while (p != _buffers.end()) {
    // keep anything that's already page sized+aligned
    if (p->is_page_aligned() && p->is_n_page_sized()) {
      /*generic_dout(0) << " segment " << (void*)p->c_str()
		      << " offset " << ((unsigned long)p->c_str() & ~PAGE_MASK)
		      << " length " << p->length()
		      << " " << (p->length() & ~PAGE_MASK) << " ok" << dendl;
      */
      p++;
      continue;
    }
    
    // consolidate unaligned items, until we get something that is sized+aligned
    list unaligned;
    unsigned offset = 0;
    do {
      /*generic_dout(0) << " segment " << (void*)p->c_str()
		      << " offset " << ((unsigned long)p->c_str() & ~PAGE_MASK)
		      << " length " << p->length() << " " << (p->length() & ~PAGE_MASK)
		      << " overall offset " << offset << " " << (offset & ~PAGE_MASK)
		      << " not ok" << dendl;
      */
      offset += p->length();
      unaligned.push_back(*p);
      _buffers.erase(p++);
    } while (p != _buffers.end() &&
	     (!p->is_page_aligned() ||
	      !p->is_n_page_sized() ||
	      (offset & ~PAGE_MASK)));
    unaligned.rebuild();
    _buffers.insert(p, unaligned._buffers.front());
  }
}
  

int buffer::list::read_file(const char *fn, bool silent)
{
  struct stat st;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    if (!silent) {
      char buf[80];
      cerr << "can't open " << fn << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
    }
    return -errno;
  }
  ::fstat(fd, &st);
  int s = ROUND_UP_TO(st.st_size, PAGE_SIZE);
  bufferptr bp = buffer::create_page_aligned(s);
  int left = st.st_size;
  int got = 0;
  while (left > 0) {
    int r = ::read(fd, (void *)(bp.c_str() + got), left);
    if (r <= 0)
      break;
    got += r;
    left -= r;
  }
  ::close(fd);
  bp.set_length(got);
  append(bp);
  return 0;
}

int buffer::list::write_file(const char *fn, int mode)
{
  int fd = ::open(fn, O_WRONLY|O_CREAT|O_TRUNC, mode);
  if (fd < 0) {
    char buf[80];
    cerr << "can't write " << fn << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
    return -errno;
  }
  int err = write_fd(fd);
  ::close(fd);
  return err;
}

int buffer::list::write_fd(int fd)
{
  // write buffer piecewise
  if (false) {
    for (std::list<ptr>::const_iterator it = _buffers.begin(); 
	 it != _buffers.end(); 
	 it++) {
      const char *c = it->c_str();
      int left = it->length();
      while (left > 0) {
	int r = ::write(fd, c, left);
	if (r < 0)
	  return -errno;
	c += r;
	left -= r;
      }
    }
    return 0;
  }

  // use writev!
  iovec iov[IOV_MAX];
  int iovlen = 0;
  ssize_t bytes = 0;

  std::list<ptr>::const_iterator p = _buffers.begin(); 
  while (true) {
    iov[iovlen].iov_base = (void *)p->c_str();
    iov[iovlen].iov_len = p->length();
    bytes += p->length();
    iovlen++;
    p++;

    if (iovlen == IOV_MAX-1 ||
	p == _buffers.end()) {
      iovec *start = iov;
      int num = iovlen;
      ssize_t wrote;
    retry:
      wrote = ::writev(fd, start, num);
      if (wrote < 0)
	return -errno;
      if (wrote < bytes) {
	// partial write, recover!
	while ((size_t)wrote >= start[0].iov_len) {
	  wrote -= start[0].iov_len;
	  bytes -= start[0].iov_len;
	  start++;
	  num--;
	}
	if (wrote > 0) {
	  start[0].iov_len -= wrote;
	  start[0].iov_base = (char *)start[0].iov_base + wrote;
	  bytes -= wrote;
	}
	goto retry;
      }
    }
    if (p == _buffers.end())
      break;
  }
  return 0;
}


void buffer::list::hexdump(std::ostream &out) const
{
  out.setf(std::ios::right);
  out.fill('0');

  unsigned per = 16;

  for (unsigned o=0; o<length(); o += per) {
    out << std::hex << std::setw(4) << o << " :";

    unsigned i;
    for (i=0; i<per && o+i<length(); i++) {
      out << " " << std::setw(2) << ((unsigned)(*this)[o+i] & 0xff);
    }
    for (; i<per; i++)
      out << "   ";
    
    out << " : ";
    for (i=0; i<per && o+i<length(); i++) {
      char c = (*this)[o+i];
      if (isupper(c) || islower(c) || isdigit(c) || c == ' ' || ispunct(c))
	out << c;
      else
	out << '.';
    }
    out << std::dec << std::endl;
  }
  out.unsetf(std::ios::right);
}

}
