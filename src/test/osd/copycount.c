// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=c
/*
 * libceph_test_copycount: an LD_PRELOAD library that counts the bytes a
 * process copies with memcpy, memmove and mempcpy, and with their _chk
 * variants, for qa/standalone/osd/osd-oob-copies.sh.
 *
 * It counts only calls that copy at least COPYCOUNT_MIN bytes (4096 when
 * unset), so that the counts show copies of data and not the many small
 * copies of metadata. When COPYCOUNT_DIR is set, each process keeps its
 * counts in COPYCOUNT_DIR/copycount.<pid>, a file that it maps shared and
 * updates in place: a reader sees the counts as they are, with no help
 * from the process. The child of a fork() starts a file of its own, so a
 * daemon that forks to run in the background counts under its own pid.
 * The layout, in native byte order:
 *
 *   char magic[8]                        "COPYCNT2"
 *   uint64_t min, bytes, calls, dropped
 *   struct { uint64_t bytes, calls; char where[240]; } site[256]
 *
 * bytes and calls cover every counted copy. A site is the call stack
 * that led to the copy, COPYCOUNT_DEPTH frames of it (1 when unset, at
 * most 8), innermost first, each as "<object file>+0x<offset>" and
 * separated by " < ". addr2line turns each frame into a function and a
 * line. dropped counts the copies of sites that no longer fit in the
 * table (they are still in bytes and calls).
 *
 * Build it with -fno-builtin and -fno-tree-loop-distribute-patterns:
 * the copy loops below run before the real functions are found, and the
 * compiler must not turn them back into calls to memcpy.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

// the copy functions must be visible, whatever the default visibility
#define EXPORT __attribute__((visibility("default")))

#define NSITES 256
#define WHERE_LEN 240
#define MAX_DEPTH 8

struct site {
  uint64_t bytes;
  uint64_t calls;
  char where[WHERE_LEN];
};

struct counts {
  char magic[8];
  uint64_t min;
  uint64_t bytes;
  uint64_t calls;
  uint64_t dropped;
  struct site site[NSITES];
};

static struct counts *cc;
static uint64_t cc_min = 4096;
static int cc_depth = 1;
// the callers of each slot of cc->site, in this process only
static uintptr_t site_key[NSITES];
static __thread int in_hook;

static void *(*real_memcpy)(void *, const void *, size_t);
static void *(*real_memmove)(void *, const void *, size_t);
static void *(*real_mempcpy)(void *, const void *, size_t);
static void *(*real_memcpy_chk)(void *, const void *, size_t, size_t);
static void *(*real_memmove_chk)(void *, const void *, size_t, size_t);
static void *(*real_mempcpy_chk)(void *, const void *, size_t, size_t);

// copies for the calls that come before the real functions are known
static void *copy_forward(void *d, const void *s, size_t n)
{
  unsigned char *dp = d;
  const unsigned char *sp = s;
  for (size_t i = 0; i < n; i++) {
    dp[i] = sp[i];
  }
  return d;
}

static void *copy_move(void *d, const void *s, size_t n)
{
  unsigned char *dp = d;
  const unsigned char *sp = s;
  if (dp <= sp || dp >= sp + n) {
    return copy_forward(d, s, n);
  }
  for (size_t i = n; i > 0; i--) {
    dp[i - 1] = sp[i - 1];
  }
  return d;
}

// appends frame **ra** to out at **i** as "<name>+0x<hex>", and returns
// the new length; stops short of the end of out
static size_t put_frame(char *out, size_t i, const void *ra)
{
  Dl_info info;
  const char *name = "?";
  uintptr_t off = (uintptr_t)ra;
  if (dladdr(ra, &info) && info.dli_fname) {
    name = info.dli_fname;
    const char *slash = strrchr(name, '/');
    if (slash) {
      name = slash + 1;
    }
    off = (uintptr_t)ra - (uintptr_t)info.dli_fbase;
  }
  // without snprintf, which can copy too
  char hex[16];
  int h = 0;
  do {
    hex[h++] = "0123456789abcdef"[off & 0xf];
    off >>= 4;
  } while (off);
  size_t len = 0;
  while (name[len]) {
    len++;
  }
  if (i + len + 3 + h + 1 > WHERE_LEN) {
    return i;
  }
  while (*name) {
    out[i++] = *name++;
  }
  out[i++] = '+';
  out[i++] = '0';
  out[i++] = 'x';
  while (h) {
    out[i++] = hex[--h];
  }
  out[i] = '\0';
  return i;
}

static void put_where(char *out, void *const *frames, int n)
{
  size_t i = 0;
  for (int f = 0; f < n; f++) {
    if (f) {
      if (i + 4 > WHERE_LEN) {
        break;
      }
      out[i++] = ' ';
      out[i++] = '<';
      out[i++] = ' ';
      out[i] = '\0';
    }
    i = put_frame(out, i, frames[f]);
  }
}

// the call stack that led to a copy, outside this library, innermost
// first; at most cc_depth frames
static int stack_of(const void *ra, void **frames)
{
  frames[0] = (void *)ra;
  if (cc_depth <= 1) {
    return 1;
  }
  void *all[MAX_DEPTH + 8];
  int n = backtrace(all, cc_depth + 8);
  int skip = 0;
  while (skip < n && all[skip] != ra) {
    skip++;
  }
  if (skip == n) {
    return 1;
  }
  int k = 0;
  for (int f = skip; f < n && k < cc_depth; f++) {
    frames[k++] = all[f];
  }
  return k;
}

static void count(size_t n, const void *ra)
{
  struct counts *c = cc;
  if (!c || n < cc_min || in_hook) {
    return;
  }
  in_hook = 1;
  __atomic_add_fetch(&c->bytes, n, __ATOMIC_RELAXED);
  __atomic_add_fetch(&c->calls, 1, __ATOMIC_RELAXED);
  void *frames[MAX_DEPTH];
  int nframes = stack_of(ra, frames);
  uintptr_t key = 0;
  for (int f = 0; f < nframes; f++) {
    key = key * 1000003u ^ (uintptr_t)frames[f];
  }
  key |= 1;
  size_t h = (key >> 4) % NSITES;
  int found = 0;
  for (size_t probe = 0; probe < NSITES; probe++) {
    size_t i = (h + probe) % NSITES;
    uintptr_t cur = __atomic_load_n(&site_key[i], __ATOMIC_ACQUIRE);
    if (cur == 0) {
      uintptr_t zero = 0;
      if (__atomic_compare_exchange_n(&site_key[i], &zero, key, 0,
                                      __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        put_where(c->site[i].where, frames, nframes);
        cur = key;
      } else {
        cur = zero;
      }
    }
    if (cur == key) {
      __atomic_add_fetch(&c->site[i].bytes, n, __ATOMIC_RELAXED);
      __atomic_add_fetch(&c->site[i].calls, 1, __ATOMIC_RELAXED);
      found = 1;
      break;
    }
  }
  if (!found) {
    __atomic_add_fetch(&c->dropped, 1, __ATOMIC_RELAXED);
  }
  in_hook = 0;
}

// map COPYCOUNT_DIR/copycount.<pid>; leaves cc NULL if that fails
static void map_counts(void)
{
  const char *dir = getenv("COPYCOUNT_DIR");
  if (!dir || !*dir) {
    return;
  }
  char path[4096];
  size_t i = 0;
  while (dir[i] && i < sizeof(path) - 32) {
    path[i] = dir[i];
    i++;
  }
  const char *base = "/copycount.";
  while (*base) {
    path[i++] = *base++;
  }
  char digits[24];
  int d = 0;
  unsigned long pid = (unsigned long)getpid();
  do {
    digits[d++] = '0' + pid % 10;
    pid /= 10;
  } while (pid);
  while (d) {
    path[i++] = digits[--d];
  }
  path[i] = '\0';
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return;
  }
  if (ftruncate(fd, sizeof(struct counts)) < 0) {
    close(fd);
    return;
  }
  void *m = mmap(NULL, sizeof(struct counts), PROT_READ | PROT_WRITE,
                 MAP_SHARED, fd, 0);
  close(fd);
  if (m == MAP_FAILED) {
    return;
  }
  struct counts *c = m;
  copy_forward(c->magic, "COPYCNT2", 8);
  c->min = cc_min;
  __atomic_store_n(&cc, c, __ATOMIC_RELEASE);
}

static void after_fork_in_child(void)
{
  struct counts *old = cc;
  cc = NULL;
  for (size_t i = 0; i < NSITES; i++) {
    site_key[i] = 0;
  }
  if (old) {
    munmap(old, sizeof(struct counts));
  }
  map_counts();
}

__attribute__((constructor))
static void copycount_init(void)
{
  in_hook = 1;
  real_memcpy = dlsym(RTLD_NEXT, "memcpy");
  real_memmove = dlsym(RTLD_NEXT, "memmove");
  real_mempcpy = dlsym(RTLD_NEXT, "mempcpy");
  real_memcpy_chk = dlsym(RTLD_NEXT, "__memcpy_chk");
  real_memmove_chk = dlsym(RTLD_NEXT, "__memmove_chk");
  real_mempcpy_chk = dlsym(RTLD_NEXT, "__mempcpy_chk");
  const char *min = getenv("COPYCOUNT_MIN");
  if (min && *min) {
    cc_min = strtoull(min, NULL, 0);
  }
  const char *depth = getenv("COPYCOUNT_DEPTH");
  if (depth && *depth) {
    cc_depth = atoi(depth);
    if (cc_depth < 1) {
      cc_depth = 1;
    } else if (cc_depth > MAX_DEPTH) {
      cc_depth = MAX_DEPTH;
    }
    // backtrace() loads libgcc_s the first time: do that here
    void *warm[2];
    backtrace(warm, 2);
  }
  map_counts();
  pthread_atfork(NULL, NULL, after_fork_in_child);
  in_hook = 0;
}

EXPORT void *memcpy(void *d, const void *s, size_t n)
{
  count(n, __builtin_return_address(0));
  return real_memcpy ? real_memcpy(d, s, n) : copy_forward(d, s, n);
}

EXPORT void *memmove(void *d, const void *s, size_t n)
{
  count(n, __builtin_return_address(0));
  return real_memmove ? real_memmove(d, s, n) : copy_move(d, s, n);
}

EXPORT void *mempcpy(void *d, const void *s, size_t n)
{
  count(n, __builtin_return_address(0));
  if (real_mempcpy) {
    return real_mempcpy(d, s, n);
  }
  copy_forward(d, s, n);
  return (char *)d + n;
}

EXPORT void *__memcpy_chk(void *d, const void *s, size_t n, size_t dn)
{
  count(n, __builtin_return_address(0));
  if (real_memcpy_chk) {
    return real_memcpy_chk(d, s, n, dn);
  }
  if (n > dn) {
    abort();
  }
  return copy_forward(d, s, n);
}

EXPORT void *__memmove_chk(void *d, const void *s, size_t n, size_t dn)
{
  count(n, __builtin_return_address(0));
  if (real_memmove_chk) {
    return real_memmove_chk(d, s, n, dn);
  }
  if (n > dn) {
    abort();
  }
  return copy_move(d, s, n);
}

EXPORT void *__mempcpy_chk(void *d, const void *s, size_t n, size_t dn)
{
  count(n, __builtin_return_address(0));
  if (real_mempcpy_chk) {
    return real_mempcpy_chk(d, s, n, dn);
  }
  if (n > dn) {
    abort();
  }
  copy_forward(d, s, n);
  return (char *)d + n;
}
