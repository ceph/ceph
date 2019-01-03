// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "numa.h"

#include <cstring>
#include <errno.h>
#include <iostream>

#include "include/stringify.h"
#include "common/safe_io.h"


// list

int parse_cpu_set_list(const char *s,
		       size_t *cpu_set_size,
		       cpu_set_t *cpu_set)
{
  CPU_ZERO(cpu_set);
  while (*s) {
    char *end;
    int a = strtol(s, &end, 10);
    if (end == s) {
      return -EINVAL;
    }
    if (*end == '-') {
      s = end + 1;
      int b = strtol(s, &end, 10);
      if (end == s) {
	return -EINVAL;
      }
      for (; a <= b; ++a) {
	CPU_SET(a, cpu_set);
      }
      *cpu_set_size = a;
    } else {
      CPU_SET(a, cpu_set);
      *cpu_set_size = a + 1;
    }
    if (*end == 0) {
      break;
    }
    if (*end != ',') {
      return -EINVAL;
    }
    s = end + 1;
  }
  return 0;
}

std::string cpu_set_to_str_list(size_t cpu_set_size,
				const cpu_set_t *cpu_set)
{
  std::string r;
  unsigned a = 0;
  while (true) {
    while (a < cpu_set_size && !CPU_ISSET(a, cpu_set)) {
      ++a;
    }
    if (a >= cpu_set_size) {
      break;
    }
    unsigned b = a + 1;
    while (b < cpu_set_size && CPU_ISSET(b, cpu_set)) {
      ++b;
    }
    if (r.size()) {
      r += ",";
    }
    if (b > a + 1) {
      r += stringify(a) + "-" + stringify(b - 1);
    } else {
      r += stringify(a);
    }
    a = b;
  }
  return r;
}

std::set<int> cpu_set_to_set(size_t cpu_set_size,
			     const cpu_set_t *cpu_set)
{
  set<int> r;
  unsigned a = 0;
  while (true) {
    while (a < cpu_set_size && !CPU_ISSET(a, cpu_set)) {
      ++a;
    }
    if (a >= cpu_set_size) {
      break;
    }
    unsigned b = a + 1;
    while (b < cpu_set_size && CPU_ISSET(b, cpu_set)) {
      ++b;
    }
    while (a < b) {
      r.insert(a);
      ++a;
    }
  }
  return r;
}

// mask
static int from_hex(char c)
{
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  return -1;
}

static char to_hex(int v)
{
  if (v < 10) {
    return '0' + v;
  }
  return 'a' + (v - 10);
}

// FIXME: this is assuming the local_cpus value is little endian... is
// that right?

// FIXME: this assumes the cpu count is a multiple of 4 (a nibble)

int parse_cpu_set_mask(
  const char *s,
  size_t *cpu_set_size,
  cpu_set_t *cpu_set)
{
  char *b = new char[CPU_SETSIZE];
  memset(b, 0, CPU_SETSIZE);
  int r = -EINVAL;
  unsigned cpu = 0;
  int pos;
  for (pos = 0; s[pos]; ++pos) {
    if (pos/2 >= CPU_SETSIZE) {
      goto out;
    }
  }
  CPU_ZERO(cpu_set);
  *cpu_set_size = pos * 4;
  for (--pos; pos >= 0; --pos) {
    int v = from_hex(s[pos]);
    if (v < 0) {
      goto out;
    }
    for (unsigned i = 0; i < 4; ++i, ++cpu) {
      if (v & (1 << (cpu & 3))) {
	CPU_SET(cpu, cpu_set);
      }
    }
  }
  r = 0;

 out:
  delete b;
  return r;
}

std::string cpu_set_to_str_mask(
  size_t cpu_set_size,
  const cpu_set_t *cpu_set)
{
  std::string r;
  unsigned v = 0;
  for (int i = cpu_set_size - 1; i >= 0; --i) {
    if (CPU_ISSET(i, cpu_set)) {
      v |= 1 << (i & 7);
    }
    if ((i & 7) == 0) {
      if (i + 4 < (int)cpu_set_size) {
	r += to_hex((v & 0xf0) >> 4);
      }
      r += to_hex(v & 0xf);
      v = 0;
    }
  }
  return r;
}

int get_numa_node_cpu_set(
  int node,
  size_t *cpu_set_size,
  cpu_set_t *cpu_set)
{
  std::string fn = "/sys/devices/system/node/node";
  fn += stringify(node);
  fn += "/cpulist";
  std::cout << __func__ << " fn " << fn << std::endl;
  int fd = ::open(fn.c_str(), O_RDONLY);
  if (fd < 0) {
    std::cout << __func__ << " fail to open " << errno << std::endl;
    return -errno;
  }
  char buf[1024];
  int r = safe_read(fd, &buf, sizeof(buf));
  if (r < 0) {
    std::cout << __func__ << " fail to read " << errno << std::endl;
    goto out;
  }
  buf[r] = 0;
  while (r > 0 && ::isspace(buf[--r])) {
    buf[r] = 0;
  }
  r = parse_cpu_set_list(buf, cpu_set_size, cpu_set);
  if (r < 0) {
    std::cout << __func__ << " fail to parse " << r << std::endl;
    goto out;
  }
  r = 0;
 out:
  ::close(fd);
  return r;
}
