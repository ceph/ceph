/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 XSKY Inc.
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_COMPRESSION_PLUGIN_MyLZ4_H
#define CEPH_COMPRESSION_PLUGIN_MyLZ4_H

// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "compressor/CompressionPlugin.h"
#include "MyLZ4Compressor.h"
// -----------------------------------------------------------------------------

class CompressionPluginMyLZ4 : public ceph::CompressionPlugin {

public:

  explicit CompressionPluginMyLZ4(CephContext* cct) : CompressionPlugin(cct)
  {}

  // 【修正】去掉 alg_name 参数，只保留后面两个，必须与基类完全一致
  int factory(CompressorRef *cs, std::ostream *ss) override {
    // 【修正】去掉 alg_name 的赋值逻辑，因为参数里没有它了
    // 插件的名字已经在 .cc 文件的 __ceph_plugin_init 里注册了，这里不需要管
    
    if (compressor == 0) {
      MyLZ4Compressor *interface = new MyLZ4Compressor(cct);
      compressor = CompressorRef(interface);
    }
    *cs = compressor;
    return 0;
  }
};

#endif