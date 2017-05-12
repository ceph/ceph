// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

typedef enum {
  OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL,
  OPT_ADDR, OPT_U32, OPT_U64, OPT_UUID
} ceph_option_type_t;

typedef enum {
  OPT_BASIC,      ///< basic option
  OPT_ADVANCED,   ///< advanced users only
  OPT_DEV,        ///< developer only  (make this 0, the default)
} ceph_option_level_t;

struct ceph_option {
  const char *name;
  ceph_option_type_t type;
  ceph_option_level_t level;
  const char *value;             ///< default value for everyone
  const char *daemon_value;      ///< default for daemons
  const char *nondaemon_value;   ///< default for non-daemons

  const char *description;
  const char *long_description;
  const char *see_also;

  const char *min_value;
  const char *max_value;
  const char *enum_values;

  const char *tags;
};

// array of ceph options.  the last one will have a blank name.
extern struct ceph_option *ceph_options;
