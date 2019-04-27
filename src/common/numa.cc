// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "numa.h"

#include <cstring>
#include <errno.h>
#include <iostream>

#include "include/stringify.h"
#include "common/safe_io.h"


// list
#if defined(__linux__)
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


int get_numa_node_cpu_set(
  int node,
  size_t *cpu_set_size,
  cpu_set_t *cpu_set)
{
  std::string fn = "/sys/devices/system/node/node";
  fn += stringify(node);
  fn += "/cpulist";
  int fd = ::open(fn.c_str(), O_RDONLY);
  if (fd < 0) {
    return -errno;
  }
  char buf[1024];
  int r = safe_read(fd, &buf, sizeof(buf));
  if (r < 0) {
    goto out;
  }
  buf[r] = 0;
  while (r > 0 && ::isspace(buf[--r])) {
    buf[r] = 0;
  }
  r = parse_cpu_set_list(buf, cpu_set_size, cpu_set);
  if (r < 0) {
    goto out;
  }
  r = 0;
 out:
  ::close(fd);
  return r;
}

#elif defined(__FreeBSD__)

int parse_cpu_set_list(const char *s,
		       size_t *cpu_set_size,
		       cpu_set_t *cpu_set)
{
  return -ENOTSUP;
}

std::string cpu_set_to_str_list(size_t cpu_set_size,
				const cpu_set_t *cpu_set)
{
  return {};
}

std::set<int> cpu_set_to_set(size_t cpu_set_size,
			     const cpu_set_t *cpu_set)
{
  return {};
}

int get_numa_node_cpu_set(int node,
                          size_t *cpu_set_size,
                          cpu_set_t *cpu_set)
{
  return -ENOTSUP;
}

#endif
