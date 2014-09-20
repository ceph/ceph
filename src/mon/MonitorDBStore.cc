// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/

#include "MonitorDBStore.h"
#include "os/LevelDBStore.h"

void MonitorDBStore::init_options()
{
  db->init();
  LevelDBStore *ldb = static_cast<LevelDBStore*>(&(*db));
  if (g_conf->mon_leveldb_write_buffer_size)
    ldb->options.write_buffer_size = g_conf->mon_leveldb_write_buffer_size;
  if (g_conf->mon_leveldb_cache_size)
    ldb->options.cache_size = g_conf->mon_leveldb_cache_size;
  if (g_conf->mon_leveldb_block_size)
    ldb->options.block_size = g_conf->mon_leveldb_block_size;
  if (g_conf->mon_leveldb_bloom_size)
    ldb->options.bloom_size = g_conf->mon_leveldb_bloom_size;
  if (g_conf->mon_leveldb_compression)
    ldb->options.compression_enabled = g_conf->mon_leveldb_compression;
  if (g_conf->mon_leveldb_max_open_files)
    ldb->options.max_open_files = g_conf->mon_leveldb_max_open_files;
  if (g_conf->mon_leveldb_paranoid)
    ldb->options.paranoid_checks = g_conf->mon_leveldb_paranoid;
  if (g_conf->mon_leveldb_log.length())
    ldb->options.log_file = g_conf->mon_leveldb_log;
}
