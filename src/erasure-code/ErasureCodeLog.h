// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_ERASURE_CODE_LOG_H
#define CEPH_ERASURE_CODE_LOG_H

/*
 * Logging boundary for erasure-code plugins.
 *
 * The erasure-code plugins are classic, shared objects between the 
 * classic and crimson OSDs. To ensure logging is correctly wired 
 * for both types of OSDs, `ec_log()` is used, similar to `cls_log()`. 
 *
 * The implementation differs per OSD flavor: 
 *  - classic uses g_ceph_context-based `dout` 
 *  - crimson routes to the per-shard seastar logger
 *
 * The plugin only declares and calls `ec_log`; it never defines it and never
 * references `g_ceph_context`.  The symbol is resolved from the host at dlopen.
 */
extern int ec_log(int level, const char *format, ...)
  __attribute__((__format__(printf, 2, 3)));

#define EC_LOG(level, fmt, ...)                                         \
  ec_log(level, "<ec> %s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#define EC_ERR(fmt, ...) EC_LOG(0, fmt, ##__VA_ARGS__)

#endif // CEPH_ERASURE_CODE_LOG_H
