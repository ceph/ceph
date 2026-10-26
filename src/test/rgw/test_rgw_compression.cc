// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "rgw_compression.h"
#include "rgw_zone_types.h"
#include "common/ceph_json.h"

class ut_get_sink : public RGWGetObj_Filter {
  bufferlist sink;
public:
  ut_get_sink() {}
  virtual ~ut_get_sink() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    auto& bl_buffers = bl.buffers();
    auto i = bl_buffers.begin();
    while (bl_len > 0)
    {
      ceph_assert(i != bl_buffers.end());
      off_t len = std::min<off_t>(bl_len, i->length());
      sink.append(*i, 0, len);
      bl_len -= len;
      i++;
    }
    return 0;
  }
  bufferlist& get_sink()
  {
    return sink;
  }
};

class ut_get_sink_size : public RGWGetObj_Filter {
  size_t max_size = 0;
public:
  ut_get_sink_size() {}
  virtual ~ut_get_sink_size() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    if (bl_len > (off_t)max_size)
      max_size = bl_len;
    return 0;
  }
  size_t get_size()
  {
    return max_size;
  }
};

class ut_put_sink: public rgw::sal::DataProcessor
{
  bufferlist sink;
public:
  int process(bufferlist&& bl, uint64_t ofs) override
  {
    sink.claim_append(bl);
    return 0;
  }
  bufferlist&  get_sink()
  {
    return sink;
  }
};


struct MockGetDataCB : public RGWGetObj_Filter {
  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    return 0;
  }
} cb;

using range_t = std::pair<off_t, off_t>;

// call filter->fixup_range() and return the range as a pair. this makes it easy
// to fit on a single line for ASSERT_EQ()
range_t fixup_range(RGWGetObj_Decompress *filter, off_t ofs, off_t end)
{
  filter->fixup_range(ofs, end);
  return {ofs, end};
}


TEST(Decompress, FixupRangePartial)
{
  RGWCompressionInfo cs_info;

  // array of blocks with original len=8, compressed to len=6
  auto& blocks = cs_info.blocks;
  blocks.emplace_back(compression_block{0, 0, 6});
  blocks.emplace_back(compression_block{8, 6, 6});
  blocks.emplace_back(compression_block{16, 12, 6});
  blocks.emplace_back(compression_block{24, 18, 6});

  const bool partial = true;
  RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, partial, &cb);

  // test translation from logical ranges to compressed ranges
  ASSERT_EQ(range_t(0, 5), fixup_range(&decompress, 0, 1));
  ASSERT_EQ(range_t(0, 5), fixup_range(&decompress, 1, 7));
  ASSERT_EQ(range_t(0, 11), fixup_range(&decompress, 7, 8));
  ASSERT_EQ(range_t(0, 11), fixup_range(&decompress, 0, 9));
  ASSERT_EQ(range_t(0, 11), fixup_range(&decompress, 7, 9));
  ASSERT_EQ(range_t(6, 11), fixup_range(&decompress, 8, 9));
  ASSERT_EQ(range_t(6, 17), fixup_range(&decompress, 8, 16));
  ASSERT_EQ(range_t(6, 17), fixup_range(&decompress, 8, 17));
  ASSERT_EQ(range_t(12, 23), fixup_range(&decompress, 16, 24));
  ASSERT_EQ(range_t(12, 23), fixup_range(&decompress, 16, 999));
  ASSERT_EQ(range_t(18, 23), fixup_range(&decompress, 998, 999));
}

TEST(Compress, LimitedChunkSize)
{
  CompressorRef plugin;
  plugin = Compressor::create(g_ceph_context, Compressor::COMP_ALG_ZLIB);
  ASSERT_NE(plugin.get(), nullptr);

  for (size_t s = 100 ; s < 10000000 ; s = s*5/4)
  {
    bufferptr bp(s);
    bufferlist bl;
    bl.append(bp);

    ut_put_sink c_sink;
    RGWPutObj_Compress compressor(g_ceph_context, plugin, &c_sink);
    compressor.process(std::move(bl), 0);
    compressor.process({}, s); // flush

    RGWCompressionInfo cs_info;
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = s;
    cs_info.compressor_message = compressor.get_compressor_message();
    cs_info.blocks = std::move(compressor.get_compression_blocks());

    ut_get_sink_size d_sink;
    RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, false, &d_sink);

    off_t f_begin = 0;
    off_t f_end = s - 1;
    decompress.fixup_range(f_begin, f_end);

    decompress.handle_data(c_sink.get_sink(), 0, c_sink.get_sink().length());
    bufferlist empty;
    decompress.handle_data(empty, 0, 0);

    ASSERT_LE(d_sink.get_size(), (size_t)g_ceph_context->_conf->rgw_max_chunk_size);
  }
}


TEST(Compress, BillionZeros)
{
  CompressorRef plugin;
  ut_put_sink c_sink;
  plugin = Compressor::create(g_ceph_context, Compressor::COMP_ALG_ZLIB);
  ASSERT_NE(plugin.get(), nullptr);
  RGWPutObj_Compress compressor(g_ceph_context, plugin, &c_sink);

  constexpr size_t size = 1000000;
  bufferptr bp(size);
  bufferlist bl;
  bl.append(bp);

  for (int i=0; i<1000;i++)
    compressor.process(bufferlist{bl}, size*i);
  compressor.process({}, size*1000); // flush

  RGWCompressionInfo cs_info;
  cs_info.compression_type = plugin->get_type_name();
  cs_info.orig_size = size*1000;
  cs_info.compressor_message = compressor.get_compressor_message();
  cs_info.blocks = std::move(compressor.get_compression_blocks());

  ut_get_sink d_sink;
  RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, false, &d_sink);

  off_t f_begin = 0;
  off_t f_end = size*1000 - 1;
  decompress.fixup_range(f_begin, f_end);

  decompress.handle_data(c_sink.get_sink(), 0, c_sink.get_sink().length());
  bufferlist empty;
  decompress.handle_data(empty, 0, 0);

  ASSERT_EQ(d_sink.get_sink().length() , size*1000);
}

TEST(Compress, BelowMinSize)
{
  EXPECT_TRUE(rgw_compression_below_min_size(65536, 65535));
  EXPECT_TRUE(rgw_compression_below_min_size(65536, 0));
  // at or above the min size
  EXPECT_FALSE(rgw_compression_below_min_size(65536, 65536));
  EXPECT_FALSE(rgw_compression_below_min_size(65536, 65537));
  // an unknown size is never below it
  EXPECT_FALSE(rgw_compression_below_min_size(65536, std::nullopt));
  // no min size
  EXPECT_FALSE(rgw_compression_below_min_size(0, 0));
}

TEST(ZoneStorageClass, DecodeV1LeavesMinSizeUnset)
{
  // RGWZoneStorageClass as encoded before compression_min_size (v1)
  using ceph::encode;
  bufferlist bl;
  ENCODE_START(1, 1, bl);
  encode(boost::optional<rgw_pool>(rgw_pool("pool1")), bl);
  encode(boost::optional<std::string>("zstd"), bl);
  ENCODE_FINISH(bl);

  RGWZoneStorageClass sc;
  sc.compression_min_size = 4096;
  auto p = bl.cbegin();
  decode(sc, p);
  EXPECT_EQ("zstd", sc.compression_type.get_value_or(""));
  EXPECT_FALSE(sc.compression_min_size);
}

TEST(ZoneStorageClass, MinSizeJsonRoundTrip)
{
  RGWZoneStorageClass in;
  in.compression_type = "zstd";
  in.compression_min_size = 65536;

  JSONFormatter f;
  f.open_object_section("");
  in.dump(&f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);

  JSONParser parser;
  ASSERT_TRUE(parser.parse(ss.str().c_str(), ss.str().length()));
  RGWZoneStorageClass out;
  out.decode_json(&parser);
  EXPECT_EQ(65536u, out.compression_min_size.get_value_or(0));
  EXPECT_EQ("zstd", out.compression_type.get_value_or(""));
}

TEST(ZoneStorageClasses, ModifyKeepsMinSize)
{
  RGWZoneStorageClasses scs;
  const std::string zstd{"zstd"};
  const std::string lz4{"lz4"};
  scs.set_storage_class("STANDARD", nullptr, &zstd);
  scs.set_compression_min_size("STANDARD", 65536);
  // changing only the compression type keeps the min size
  scs.set_storage_class("STANDARD", nullptr, &lz4);

  const RGWZoneStorageClass* sc = nullptr;
  ASSERT_TRUE(scs.find("STANDARD", &sc));
  EXPECT_EQ("lz4", sc->compression_type.get_value_or(""));
  EXPECT_EQ(65536u, sc->compression_min_size.get_value_or(0));
}

TEST(ZonePlacementInfo, CompressionMinSizeFallbacks)
{
  RGWZonePlacementInfo info;
  info.storage_classes.set_compression_min_size("COLD", 4096);
  EXPECT_EQ(4096u, info.get_compression_min_size("COLD"));
  // unset on an existing storage class, and a missing storage class
  EXPECT_EQ(0u, info.get_compression_min_size("STANDARD"));
  EXPECT_EQ(0u, info.get_compression_min_size("MISSING"));
}

TEST(SyncCompressionTypes, StorageClasses)
{
  RGWZonePlacementInfo info;
  const std::string zstd = "zstd", zlib = "zlib", none = "none", random = "random";
  const std::string missing = "nosuchplugin";
  info.storage_classes.set_storage_class("STANDARD", nullptr, &zstd);
  info.storage_classes.set_storage_class("COLD", nullptr, &zlib);
  info.storage_classes.set_storage_class("ARCHIVE", nullptr, &none);
  info.storage_classes.set_storage_class("TEST", nullptr, &random);
  info.storage_classes.set_storage_class("NOPLUGIN", nullptr, &missing);
  info.storage_classes.set_storage_class("BAD,NAME", nullptr, &zstd);
  info.storage_classes.set_storage_class("BAD:NAME", nullptr, &zstd);
  info.storage_classes.set_storage_class("*", nullptr, &zstd);
  auto cct = g_ceph_context;
  // objects without a storage class go to the bucket's default class. classes
  // without a fixed type or a loadable plugin, and names with separators, are
  // left out
  EXPECT_EQ(":zstd,COLD:zlib,STANDARD:zstd",
            rgw_sync_compression_types(cct, info, "STANDARD", std::nullopt));
  EXPECT_EQ(":zlib,COLD:zlib,STANDARD:zstd",
            rgw_sync_compression_types(cct, info, "COLD", std::nullopt));
  // sync policy puts every object in one storage class, and an empty one
  // means the bucket's default
  EXPECT_EQ("*:zlib", rgw_sync_compression_types(cct, info, "STANDARD", "COLD"));
  EXPECT_EQ("*:zstd", rgw_sync_compression_types(cct, info, "STANDARD", ""));
  EXPECT_EQ("*:zlib", rgw_sync_compression_types(cct, info, "COLD", ""));
  EXPECT_EQ("", rgw_sync_compression_types(cct, info, "STANDARD", "ARCHIVE"));
  EXPECT_EQ("", rgw_sync_compression_types(cct, info, "STANDARD", "NOPLUGIN"));
  EXPECT_EQ("", rgw_sync_compression_types(cct, info, "STANDARD", "MISSING"));
  EXPECT_EQ("", rgw_sync_compression_types(cct, RGWZonePlacementInfo{}, "STANDARD",
                                           std::nullopt));
}

TEST(SyncCompressionTypes, Matches)
{
  const std::string_view types = ":zlib,COLD:zlib,STANDARD:zstd";
  EXPECT_TRUE(rgw_sync_compression_type_matches(types, "", "zlib"));
  EXPECT_TRUE(rgw_sync_compression_type_matches(types, "STANDARD", "zstd"));
  EXPECT_TRUE(rgw_sync_compression_type_matches(types, "COLD", "zlib"));
  // an object without a storage class uses the empty entry, an object with
  // an explicit one uses that one's
  EXPECT_FALSE(rgw_sync_compression_type_matches(types, "", "zstd"));
  EXPECT_FALSE(rgw_sync_compression_type_matches(types, "STANDARD", "zlib"));
  // the type of another storage class doesn't match
  EXPECT_FALSE(rgw_sync_compression_type_matches(types, "COLD", "zstd"));
  EXPECT_FALSE(rgw_sync_compression_type_matches(types, "MISSING", "zstd"));
  EXPECT_TRUE(rgw_sync_compression_type_matches("*:zlib", "STANDARD", "zlib"));
  EXPECT_FALSE(rgw_sync_compression_type_matches("*:zlib", "COLD", "zstd"));
  EXPECT_FALSE(rgw_sync_compression_type_matches("", "STANDARD", "zstd"));
  EXPECT_FALSE(rgw_sync_compression_type_matches("zstd,COLD", "COLD", "zstd"));
}
