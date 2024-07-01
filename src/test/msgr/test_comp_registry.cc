// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "include/stringify.h"
#include "compressor/Compressor.h"
#include "msg/compressor_registry.h"
#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "global/global_context.h"

#include <sstream>

TEST(CompressorRegistry, con_modes)
{
  auto cct = g_ceph_context;
  CompressorRegistry reg(cct);
  std::vector<uint32_t> methods;
  uint32_t method;
  uint32_t mode;

  const std::vector<uint32_t> snappy_method = { Compressor::COMP_ALG_SNAPPY };
  const std::vector<uint32_t> zlib_method = { Compressor::COMP_ALG_ZLIB };
  const std::vector<uint32_t> both_methods = { Compressor::COMP_ALG_ZLIB, Compressor::COMP_ALG_SNAPPY};
  const std::vector<uint32_t> no_method = { Compressor::COMP_ALG_NONE };

  cct->_conf.set_val(
    "enable_experimental_unrecoverable_data_corrupting_features", "*");

  // baseline: compression for communication with osd is enabled
  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  cct->_conf.set_val("ms_osd_compress_mode", "force");
  cct->_conf.set_val("ms_osd_compression_algorithm", "snappy");
  cct->_conf.set_val("ms_compress_secure", "false");
  cct->_conf.apply_changes(NULL);

  ASSERT_EQ(reg.get_is_compress_secure(), false);

  methods = reg.get_methods(CEPH_ENTITY_TYPE_MON);
  ASSERT_EQ(methods.size(), 0);
  method = reg.pick_method(CEPH_ENTITY_TYPE_MON, both_methods);
  ASSERT_EQ(method, Compressor::COMP_ALG_NONE);
  mode = reg.get_mode(CEPH_ENTITY_TYPE_MON, false);
  ASSERT_EQ(mode, Compressor::COMP_NONE);

  methods = reg.get_methods(CEPH_ENTITY_TYPE_OSD);
  ASSERT_EQ(methods, snappy_method);
  const std::vector<uint32_t> rev_both_methods (both_methods.rbegin(), both_methods.rend());
  method = reg.pick_method(CEPH_ENTITY_TYPE_OSD, rev_both_methods);
  ASSERT_EQ(method, Compressor::COMP_ALG_SNAPPY);
  mode = reg.get_mode(CEPH_ENTITY_TYPE_OSD, false);
  ASSERT_EQ(mode, Compressor::COMP_FORCE);
  mode = reg.get_mode(CEPH_ENTITY_TYPE_OSD, true);
  ASSERT_EQ(mode, Compressor::COMP_NONE);

  method = reg.pick_method(CEPH_ENTITY_TYPE_OSD, zlib_method);
  ASSERT_EQ(method, Compressor::COMP_ALG_NONE);

  // disable compression mode
  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  cct->_conf.set_val("ms_osd_compress_mode", "none");
  cct->_conf.apply_changes(NULL);

  mode = reg.get_mode(CEPH_ENTITY_TYPE_OSD, false);
  ASSERT_EQ(mode, Compressor::COMP_NONE);

  // no compression methods
  cct->_conf.set_val("ms_osd_compress_mode", "force");
  cct->_conf.set_val("ms_osd_compression_algorithm", "none");
  cct->_conf.apply_changes(NULL);

  method = reg.pick_method(CEPH_ENTITY_TYPE_OSD, both_methods);
  ASSERT_EQ(method, Compressor::COMP_ALG_NONE);

  // min compression size
  cct->_conf.set_val("ms_osd_compress_min_size", "1024");
  cct->_conf.apply_changes(NULL);

  uint32_t s = reg.get_min_compression_size(CEPH_ENTITY_TYPE_OSD);
  ASSERT_EQ(s, 1024);

  // allow secure with compression
  cct->_conf.set_val("ms_osd_compress_mode", "force");
  cct->_conf.set_val("ms_osd_compression_algorithm", "snappy");
  cct->_conf.set_val("ms_compress_secure", "true");
  cct->_conf.apply_changes(NULL);

  ASSERT_EQ(reg.get_is_compress_secure(), true);

  mode = reg.get_mode(CEPH_ENTITY_TYPE_OSD, true);
  ASSERT_EQ(mode, Compressor::COMP_FORCE);

  mode = reg.get_mode(CEPH_ENTITY_TYPE_OSD, false);
  ASSERT_EQ(mode, Compressor::COMP_FORCE);
  
  // back to normalish, for the benefit of the next test(s)
  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);  
}
