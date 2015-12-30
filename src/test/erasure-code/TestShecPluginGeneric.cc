// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 * Copyright (C) 2015 FUJITSU LIMITED
 *
 * Author: Loic Dachary <loic@dachary.org>
 * Author: Shotaro Kawaguchi <kawaguchi.s@jp.fujitsu.com>
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "ceph_ver.h"

extern "C" const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

extern "C" int __erasure_code_init(char *plugin_name, char *directory)
{
  return -111;
}
