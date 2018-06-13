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

#include <optional>

#include "common/config.h"
#include "common/huge_page_pool.h"


static std::optional<ceph::huge_page_pool> hp_pool;
 
void ceph_init_huge_page_pools(class md_config_t* conf) {
  assert(conf);
  hp_pool.emplace(conf->get_val<std::size_t>("huge_page_pool_size"),
		  conf->get_val<std::size_t>("huge_page_size"));
}

ceph::huge_page_pool& ceph_get_huge_page_pool() {
  assert(hp_pool);
  return *hp_pool;
}
