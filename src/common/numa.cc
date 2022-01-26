// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "numa.h"

#include <cstring>
#include <errno.h>
#include <iostream>

#include "include/stringify.h"
#include "common/safe_io.h"

using namespace std::literals;

using std::set;


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

static int easy_readdir(const std::string& dir, std::set<std::string> *out)
{
  DIR *h = ::opendir(dir.c_str());
  if (!h) {
    return -errno;
  }
  struct dirent *de = nullptr;
  while ((de = ::readdir(h))) {
    if (strcmp(de->d_name, ".") == 0 ||
	strcmp(de->d_name, "..") == 0) {
      continue;
    }
    out->insert(de->d_name);
  }
  closedir(h);
  return 0;
}

#ifdef HAVE_DPDK
static std::string get_task_comm(pid_t tid)
{
  static const char* comm_fmt = "/proc/self/task/%d/comm";
  char comm_name[strlen(comm_fmt) + 8];
  snprintf(comm_name, sizeof(comm_name), comm_fmt, tid);
  int fd = open(comm_name, O_CLOEXEC | O_RDONLY);
  if (fd == -1) {
    return "";
  }
  // see linux/sched.h
  static constexpr int TASK_COMM_LEN = 16;
  char name[TASK_COMM_LEN];
  ssize_t n = safe_read(fd, name, sizeof(name));
  close(fd);
  if (n < 0) {
    return "";
  }
  assert(static_cast<size_t>(n) <= sizeof(name));
  if (name[n - 1] == '\n') {
    name[n - 1] = '\0';
  } else {
    name[n] = '\0';
  }
  return name;
}
#endif

int set_cpu_affinity_all_threads(size_t cpu_set_size, cpu_set_t *cpu_set)
{
  // first set my affinity
  int r = sched_setaffinity(getpid(), cpu_set_size, cpu_set);
  if (r < 0) {
    return -errno;
  }

  // make 2 passes here so that we (hopefully) catch racing threads creating
  // threads.
  for (unsigned pass = 0; pass < 2; ++pass) {
    // enumerate all child threads from /proc
    std::set<std::string> ls;
    std::string path = "/proc/"s + stringify(getpid()) + "/task";
    r = easy_readdir(path, &ls);
    if (r < 0) {
      return r;
    }
    for (auto& i : ls) {
      pid_t tid = atoll(i.c_str());
      if (!tid) {
	continue;  // wtf
      }
      #ifdef HAVE_DPDK
      std::string thread_name = get_task_comm(tid);
      static const char *dpdk_worker_name = "lcore-worker";
      if (!thread_name.compare(0, strlen(dpdk_worker_name), dpdk_worker_name)) {
	// ignore dpdk reactor thread, as it takes case of numa by itself
        continue;
      }
      #endif
      r = sched_setaffinity(tid, cpu_set_size, cpu_set);
      if (r < 0) {
	return -errno;
      }
    }
  }
  return 0;
}

#else
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

int set_cpu_affinity_all_threads(size_t cpu_set_size,
				 cpu_set_t *cpu_set)
{
  return -ENOTSUP;
}

#endif
