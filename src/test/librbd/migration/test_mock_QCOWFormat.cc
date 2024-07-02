// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/migration/MockStreamInterface.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/migration/QCOWFormat.h"
#include "librbd/migration/SourceSpecBuilder.h"
#include "acconfig.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "json_spirit/json_spirit.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }

  static MockTestImageCtx *create(const std::string &image_name,
                                  const std::string &image_id,
                                  librados::snap_t snap_id, librados::IoCtx& p,
                                  bool read_only) {
    auto ictx = ImageCtx::create(
            image_name, image_id, snap_id, p, read_only);
    auto mock_image_ctx = new MockTestImageCtx(*ictx);
    mock_image_ctx->m_image_ctx = ictx;
    m_ictxs.insert(mock_image_ctx);
    return mock_image_ctx;
  }

  ImageCtx* m_image_ctx = nullptr;

  static std::set<MockTestImageCtx *> m_ictxs;
};

std::set<librbd::MockTestImageCtx *> MockTestImageCtx::m_ictxs;

} // anonymous namespace

namespace migration {

template<>
struct SourceSpecBuilder<librbd::MockTestImageCtx> {

  MOCK_CONST_METHOD3(build_stream, int(librbd::MockTestImageCtx*,
                                       const json_spirit::mObject&,
                                       std::shared_ptr<StreamInterface>*));

};

} // namespace migration

bool operator==(const SnapInfo& lhs, const SnapInfo& rhs) {
    return (lhs.name == rhs.name &&
            lhs.size == rhs.size);
}

} // namespace librbd

#include "librbd/migration/QCOWFormat.cc"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::ReturnRef;
using ::testing::WithArg;
using ::testing::WithArgs;

namespace librbd {
namespace migration {

using ::testing::Invoke;

class TestMockMigrationQCOWFormat : public TestMockFixture {
public:
  typedef QCOWFormat<MockTestImageCtx> MockQCOWFormat;
  typedef SourceSpecBuilder<MockTestImageCtx> MockSourceSpecBuilder;

  librbd::ImageCtx *m_image_ctx;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));

    json_spirit::mObject stream_obj;
    stream_obj["type"] = "file";
    json_object["stream"] = stream_obj;

    MockTestImageCtx::m_ictxs.clear();
  }

  void TearDown() {
    for (auto iter = MockTestImageCtx::m_ictxs.begin();
         iter != MockTestImageCtx::m_ictxs.end(); ++iter) {
      auto mock_image_ctx = *iter;
      auto ictx = mock_image_ctx->m_image_ctx;
      delete mock_image_ctx;
      ictx->state->close();
    }

    TestMockFixture::TearDown();
  }

  void expect_build_stream(MockSourceSpecBuilder& mock_source_spec_builder,
                           MockStreamInterface* mock_stream_interface, int r) {
    EXPECT_CALL(mock_source_spec_builder, build_stream(_, _, _))
      .WillOnce(WithArgs<2>(Invoke([mock_stream_interface, r]
        (std::shared_ptr<StreamInterface>* ptr) {
          ptr->reset(mock_stream_interface);
          return r;
        })));
  }

  void expect_stream_open(MockStreamInterface& mock_stream_interface, int r) {
    EXPECT_CALL(mock_stream_interface, open(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_stream_close(MockStreamInterface& mock_stream_interface, int r) {
    EXPECT_CALL(mock_stream_interface, close(_))
      .WillOnce(Invoke([r](Context* ctx) { ctx->complete(r); }));
  }

  void expect_stream_read(MockStreamInterface& mock_stream_interface,
                          const io::Extents& byte_extents,
                          const bufferlist& bl, int r) {
    EXPECT_CALL(mock_stream_interface, read(byte_extents, _, _))
      .WillOnce(WithArgs<1, 2>(Invoke([bl, r]
        (bufferlist* out_bl, Context* ctx) {
          *out_bl = bl;
          ctx->complete(r);
        })));
  }

  void expect_probe_header(MockStreamInterface& mock_stream_interface,
                           uint32_t magic, uint32_t version, int r) {
    magic = htobe32(magic);
    version = htobe32(version);

    bufferlist probe_bl;
    probe_bl.append(std::string_view(reinterpret_cast<char*>(&magic), 4));
    probe_bl.append(std::string_view(reinterpret_cast<char*>(&version), 4));
    expect_stream_read(mock_stream_interface, {{0, 8}}, probe_bl, r);
  }

  void expect_read_header(MockStreamInterface& mock_stream_interface,
                          uint32_t snapshot_count, int r) {
    QCowHeader qcow_header;
    memset(&qcow_header, 0, sizeof(qcow_header));
    qcow_header.magic = htobe32(QCOW_MAGIC);
    qcow_header.version = htobe32(2);
    qcow_header.size = htobe64(1<<30);
    qcow_header.cluster_bits = htobe32(16);
    qcow_header.l1_size = htobe32(2);
    qcow_header.l1_table_offset = htobe64(1<<20);
    if (snapshot_count > 0) {
      qcow_header.nb_snapshots = htobe32(snapshot_count);
      qcow_header.snapshots_offset = htobe64(1<<21);
    }

    bufferlist header_bl;
    header_bl.append(std::string_view(reinterpret_cast<char*>(&qcow_header),
                     sizeof(qcow_header)));
    expect_stream_read(mock_stream_interface, {{0, sizeof(qcow_header)}},
                       header_bl, r);
  }

  void expect_read_l1_table(MockStreamInterface& mock_stream_interface,
                            std::optional<std::vector<uint64_t>>&& l1_table,
                            int r) {
    bufferlist l1_table_bl;
    if (!l1_table) {
      l1_table.emplace(2);
    }

    l1_table->resize(2);
    for (size_t idx = 0; idx < l1_table->size(); ++idx) {
      (*l1_table)[idx] = htobe64((*l1_table)[idx]);
    }

    l1_table_bl.append(
      std::string_view(reinterpret_cast<char*>(l1_table->data()), 16));
    expect_stream_read(mock_stream_interface, {{1<<20, 16}}, l1_table_bl, r);
  }

  void expect_read_l2_table(MockStreamInterface& mock_stream_interface,
                            uint64_t l2_table_offset,
                            std::optional<std::vector<uint64_t>>&& l2_table,
                            int r) {
    size_t l2_table_size = 1<<(16 - 3); // cluster_bit - 3 bits for offset
    bufferlist l2_table_bl;
    if (!l2_table) {
      l2_table.emplace(l2_table_size);
    }

    l2_table->resize(l2_table_size);
    for (size_t idx = 0; idx < l2_table->size(); ++idx) {
      (*l2_table)[idx] = htobe64((*l2_table)[idx]);
    }

    l2_table_bl.append(
      std::string_view(reinterpret_cast<char*>(l2_table->data()),
                       l2_table->size() * sizeof(uint64_t)));
    expect_stream_read(mock_stream_interface,
                       {{l2_table_offset, l2_table->size() * sizeof(uint64_t)}},
                       l2_table_bl, r);
  }

  void expect_read_cluster(MockStreamInterface& mock_stream_interface,
                           uint64_t cluster_offset, uint32_t offset,
                           const bufferlist& bl, int r) {
    uint32_t cluster_size = 1<<16;

    bufferlist cluster_bl;
    if (offset > 0) {
      cluster_size -= offset;
      cluster_bl.append_zero(offset);
    }

    cluster_size -= bl.length();
    cluster_bl.append(bl);

    if (cluster_size > 0) {
      cluster_bl.append_zero(cluster_size);
    }

    expect_stream_read(mock_stream_interface,
                       {{cluster_offset, cluster_bl.length()}}, cluster_bl, r);
  }

  void expect_open(MockStreamInterface& mock_stream_interface,
                   std::optional<std::vector<uint64_t>>&& l1_table) {
    expect_stream_open(mock_stream_interface, 0);

    expect_probe_header(mock_stream_interface, QCOW_MAGIC, 2, 0);

    expect_read_header(mock_stream_interface, 0, 0);

    expect_read_l1_table(mock_stream_interface, std::move(l1_table), 0);
  }

  void expect_read_snapshot_header(MockStreamInterface& mock_stream_interface,
                                   const std::string& id,
                                   const std::string& name,
                                   uint64_t l1_table_offset,
                                   uint64_t* snapshot_offset, int r) {
    QCowSnapshotHeader snapshot_header;
    memset(&snapshot_header, 0, sizeof(snapshot_header));
    snapshot_header.id_str_size = htobe16(id.size());
    snapshot_header.name_size = htobe16(name.size());
    snapshot_header.extra_data_size = htobe32(16);
    snapshot_header.l1_size = htobe32(1);
    snapshot_header.l1_table_offset = htobe64(l1_table_offset);

    bufferlist snapshot_header_bl;
    snapshot_header_bl.append(
      std::string_view(reinterpret_cast<char*>(&snapshot_header),
                       sizeof(snapshot_header)));
    expect_stream_read(mock_stream_interface,
                       {{*snapshot_offset, sizeof(snapshot_header)}},
                       snapshot_header_bl, r);
    *snapshot_offset += sizeof(snapshot_header);
  }

  void expect_read_snapshot_header_extra(
      MockStreamInterface& mock_stream_interface,
      const std::string& id, const std::string& name, uint64_t size,
      uint64_t* snapshot_offset, int r) {
    QCowSnapshotExtraData snapshot_header_extra;
    memset(&snapshot_header_extra, 0, sizeof(snapshot_header_extra));
    snapshot_header_extra.disk_size = htobe64(size);

    bufferlist snapshot_header_extra_bl;
    snapshot_header_extra_bl.append(
      std::string_view(reinterpret_cast<char*>(&snapshot_header_extra), 16));
    snapshot_header_extra_bl.append(id);
    snapshot_header_extra_bl.append(name);
    expect_stream_read(mock_stream_interface,
                       {{*snapshot_offset, 16 + id.size() + name.size()}},
                       snapshot_header_extra_bl, r);

    *snapshot_offset += 16 + id.size() + name.size();
    *snapshot_offset = p2roundup(*snapshot_offset, static_cast<uint64_t>(8));
  }

  void expect_read_snapshot_l1_table(MockStreamInterface& mock_stream_interface,
                                     uint64_t l1_table_offset, int r) {
    uint64_t l2_table_cluster = htobe64(l1_table_offset);

    bufferlist snapshot_l1_table_bl;
    snapshot_l1_table_bl.append(
      std::string_view(reinterpret_cast<char*>(&l2_table_cluster), 8));
    expect_stream_read(mock_stream_interface, {{l1_table_offset, 8}},
                       snapshot_l1_table_bl, r);
  }


  void expect_read_snapshot(MockStreamInterface& mock_stream_interface,
                            const std::string& id, const std::string& name,
                            uint64_t size, uint64_t l1_table_offset,
                            uint64_t* snapshot_offset) {
    expect_read_snapshot_header(mock_stream_interface, id, name,
                                l1_table_offset, snapshot_offset, 0);
    expect_read_snapshot_header_extra(mock_stream_interface, id, name,
                                      size, snapshot_offset, 0);
    expect_read_snapshot_l1_table(mock_stream_interface, l1_table_offset, 0);
  }

  json_spirit::mObject json_object;
};

TEST_F(TestMockMigrationQCOWFormat, OpenCloseV1) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  int expected_open_ret_val = 0;
  QCowHeaderV1 qcow_header;
  memset(&qcow_header, 0, sizeof(qcow_header));
  qcow_header.magic = htobe32(QCOW_MAGIC);
  qcow_header.version = htobe32(1);
  qcow_header.size = htobe64(1<<30);
  qcow_header.l1_table_offset = htobe64(1<<20);
  qcow_header.cluster_bits = 16;
  qcow_header.l2_bits = 13;

  bufferlist probe_bl;
  probe_bl.append(std::string_view(reinterpret_cast<char*>(&qcow_header), 8));
  expect_stream_read(*mock_stream_interface, {{0, 8}}, probe_bl, 0);

#ifdef WITH_RBD_MIGRATION_FORMAT_QCOW_V1

  bufferlist header_bl;
  header_bl.append(std::string_view(reinterpret_cast<char*>(&qcow_header),
                                    sizeof(qcow_header)));
  expect_stream_read(*mock_stream_interface, {{0, sizeof(qcow_header)}},
                     header_bl, 0);

  bufferlist l1_table_bl;
  l1_table_bl.append_zero(16);
  expect_stream_read(*mock_stream_interface, {{1<<20, 16}}, l1_table_bl, 0);

#else // WITH_RBD_MIGRATION_FORMAT_QCOW_V1

  expected_open_ret_val = -ENOTSUP;

#endif // WITH_RBD_MIGRATION_FORMAT_QCOW_V1

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(expected_open_ret_val, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, OpenCloseV2) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {});

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ProbeInvalidMagic) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, 0, 2, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ProbeInvalidVersion) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 0, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ProbeError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, -EIO);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EIO, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

#ifdef WITH_RBD_MIGRATION_FORMAT_QCOW_V1

TEST_F(TestMockMigrationQCOWFormat, ReadHeaderV1Error) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 1, 0);

  QCowHeaderV1 qcow_header;
  memset(&qcow_header, 0, sizeof(qcow_header));
  qcow_header.magic = htobe32(QCOW_MAGIC);
  qcow_header.version = htobe32(1);
  qcow_header.size = htobe64(1<<30);
  qcow_header.l1_table_offset = htobe64(1<<20);
  qcow_header.cluster_bits = 16;
  qcow_header.l2_bits = 13;

  bufferlist header_bl;
  header_bl.append(std::string_view(reinterpret_cast<char*>(&qcow_header),
                                    sizeof(qcow_header)));
  expect_stream_read(*mock_stream_interface, {{0, sizeof(qcow_header)}},
                     header_bl, -EPERM);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EPERM, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

#endif // WITH_RBD_MIGRATION_FORMAT_QCOW_V1

TEST_F(TestMockMigrationQCOWFormat, ReadHeaderV2Error) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 0, -EIO);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EIO, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadL1TableError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 0, 0);

  expect_read_l1_table(*mock_stream_interface, {}, -EPERM);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EPERM, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, Snapshots) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 2, 0);

  uint64_t snapshot_offset = 1<<21;
  expect_read_snapshot(*mock_stream_interface, "1", "snap1", 1<<29, 1<<22,
                       &snapshot_offset);
  expect_read_snapshot(*mock_stream_interface, "2", "snap2", 1<<29, 1<<22,
                       &snapshot_offset);

  expect_read_l1_table(*mock_stream_interface, {}, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  SnapInfos snap_infos;
  C_SaferCond ctx2;
  mock_qcow_format.get_snapshots(&snap_infos, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  SnapInfos expected_snap_infos{
    {1, {"snap1", cls::rbd::UserSnapshotNamespace{}, 1<<29, {}, 0, 0, {}}},
    {2, {"snap2", cls::rbd::UserSnapshotNamespace{}, 1<<29, {}, 0, 0, {}}}};
  ASSERT_EQ(expected_snap_infos, snap_infos);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, SnapshotHeaderError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 2, 0);

  uint64_t snapshot_offset = 1<<21;
  expect_read_snapshot_header(*mock_stream_interface, "1", "snap1",
                              1<<22, &snapshot_offset, -EINVAL);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, SnapshotExtraError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 2, 0);

  uint64_t snapshot_offset = 1<<21;
  expect_read_snapshot_header(*mock_stream_interface, "1", "snap1",
                              1<<22, &snapshot_offset, 0);
  expect_read_snapshot_header_extra(*mock_stream_interface, "1", "snap1",
                                    1<<29, &snapshot_offset, -EINVAL);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, SnapshotL1TableError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 2, 0);

  uint64_t snapshot_offset = 1<<21;
  expect_read_snapshot_header(*mock_stream_interface, "1", "snap1",
                              1<<22, &snapshot_offset, 0);
  expect_read_snapshot_header_extra(*mock_stream_interface, "1", "snap1",
                                    1<<29, &snapshot_offset, 0);
  expect_read_snapshot_l1_table(*mock_stream_interface, 1<<22, -EINVAL);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_qcow_format.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationQCOWFormat, GetImageSize) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {});

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  uint64_t size = 0;
  C_SaferCond ctx2;
  mock_qcow_format.get_image_size(CEPH_NOSNAP, &size, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(1<<30, size);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, GetImageSizeSnap) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 1, 0);

  uint64_t snapshot_offset = 1<<21;
  expect_read_snapshot(*mock_stream_interface, "1", "snap1", 1<<29, 1<<22,
                       &snapshot_offset);

  expect_read_l1_table(*mock_stream_interface, {}, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  uint64_t size = 0;
  C_SaferCond ctx2;
  mock_qcow_format.get_image_size(1U, &size, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(1<<29, size);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, GetImageSizeSnapDNE) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {});

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  uint64_t size = 0;
  C_SaferCond ctx2;
  mock_qcow_format.get_image_size(1U, &size, &ctx2);
  ASSERT_EQ(-ENOENT, ctx2.wait());

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, Read) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {{1ULL<<40}});

  expect_read_l2_table(*mock_stream_interface, 1ULL<<40,
                       {{0, 1ULL<<32}}, 0);

  bufferlist expect_bl;
  expect_bl.append(std::string(123, '1'));
  expect_read_cluster(*mock_stream_interface, 1ULL<<32, 123, expect_bl, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, CEPH_NOSNAP, {{65659, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(123, ctx2.wait());
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadL1DNE) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {});

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, CEPH_NOSNAP, {{234, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(123, ctx2.wait());
  bufferlist expect_bl;
  expect_bl.append_zero(123);
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadL2DNE) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {{1ULL<<40}});

  expect_read_l2_table(*mock_stream_interface, 1ULL<<40,
                       {{0, 1ULL<<32}}, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, CEPH_NOSNAP, {{234, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(123, ctx2.wait());
  bufferlist expect_bl;
  expect_bl.append_zero(123);
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadZero) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {{1ULL<<40}});

  expect_read_l2_table(*mock_stream_interface, 1ULL<<40,
                       {{0, QCOW_OFLAG_ZERO}}, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, CEPH_NOSNAP, {{65659, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(123, ctx2.wait());
  bufferlist expect_bl;
  expect_bl.append_zero(123);
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadSnap) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 1, 0);

  uint64_t snapshot_offset = 1<<21;
  expect_read_snapshot(*mock_stream_interface, "1", "snap1", 1<<29, 1<<22,
                       &snapshot_offset);

  expect_read_l1_table(*mock_stream_interface, {}, 0);

  expect_read_l2_table(*mock_stream_interface, 1<<22,
                       {{0, 1ULL<<32}}, 0);

  bufferlist expect_bl;
  expect_bl.append(std::string(123, '1'));
  expect_read_cluster(*mock_stream_interface, 1ULL<<32, 123, expect_bl, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, 1, {{65659, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(123, ctx2.wait());
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadSnapDNE) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {{1ULL<<40}});

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, 1, {{65659, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(-ENOENT, ctx2.wait());

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadClusterCacheHit) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {{1ULL<<40}});

  expect_read_l2_table(*mock_stream_interface, 1ULL<<40,
                       {{0, 1ULL<<32}}, 0);

  bufferlist expect_bl1;
  expect_bl1.append(std::string(123, '1'));

  bufferlist expect_bl2;
  expect_bl2.append(std::string(234, '2'));

  bufferlist cluster_bl;
  cluster_bl.append_zero(123);
  cluster_bl.append(expect_bl1);
  cluster_bl.append_zero(234);
  cluster_bl.append(expect_bl2);

  expect_read_cluster(*mock_stream_interface, 1ULL<<32, 0, cluster_bl, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp1 = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl1;
  io::ReadResult read_result1{&bl1};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp1, CEPH_NOSNAP, {{65659, 123}},
                                    std::move(read_result1), 0, 0, {}));
  ASSERT_EQ(123, ctx2.wait());
  ASSERT_EQ(expect_bl1, bl1);

  C_SaferCond ctx3;
  auto aio_comp2 = io::AioCompletion::create_and_start(
    &ctx3, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl2;
  io::ReadResult read_result2{&bl2};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp2, CEPH_NOSNAP, {{66016, 234}},
                                    std::move(read_result2), 0, 0, {}));
  ASSERT_EQ(234, ctx3.wait());
  ASSERT_EQ(expect_bl2, bl2);

  C_SaferCond ctx4;
  mock_qcow_format.close(&ctx4);
  ASSERT_EQ(0, ctx4.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadClusterError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {{1ULL<<40}});

  expect_read_l2_table(*mock_stream_interface, 1ULL<<40,
                       {{0, 1ULL<<32}}, 0);

  bufferlist expect_bl;
  expect_bl.append(std::string(123, '1'));
  expect_read_cluster(*mock_stream_interface, 1ULL<<32, 123, expect_bl, -EIO);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, CEPH_NOSNAP, {{65659, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(-EIO, ctx2.wait());

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ReadL2TableError) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_open(*mock_stream_interface, {{1ULL<<40}});

  expect_read_l2_table(*mock_stream_interface, 1ULL<<40,
                       {{0, 1ULL<<32}}, -EIO);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto aio_comp = io::AioCompletion::create_and_start(
    &ctx2, m_image_ctx, io::AIO_TYPE_READ);
  bufferlist bl;
  io::ReadResult read_result{&bl};
  ASSERT_TRUE(mock_qcow_format.read(aio_comp, CEPH_NOSNAP, {{65659, 123}},
                                    std::move(read_result), 0, 0, {}));
  ASSERT_EQ(-EIO, ctx2.wait());

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationQCOWFormat, ListSnaps) {
  MockTestImageCtx mock_dst_image_ctx(*m_image_ctx);
  MockTestImageCtx* mock_src_image_ctx;

  InSequence seq;
  MockSourceSpecBuilder mock_source_spec_builder;

  auto mock_stream_interface = new MockStreamInterface();
  expect_build_stream(mock_source_spec_builder, mock_stream_interface, 0);

  expect_stream_open(*mock_stream_interface, 0);

  expect_probe_header(*mock_stream_interface, QCOW_MAGIC, 2, 0);

  expect_read_header(*mock_stream_interface, 2, 0);

  uint64_t snapshot_offset = 1<<21;
  expect_read_snapshot(*mock_stream_interface, "1", "snap1", 1<<29, 1<<22,
                       &snapshot_offset);
  expect_read_snapshot(*mock_stream_interface, "2", "snap2", 1<<29, 1<<23,
                       &snapshot_offset);

  expect_read_l1_table(*mock_stream_interface, {{1ULL<<28}}, 0);

  io::SparseExtents sparse_extents_1;
  sparse_extents_1.insert(0, 196608, {io::SPARSE_EXTENT_STATE_DATA, 196608});
  expect_read_l2_table(*mock_stream_interface, 1ULL<<22,
                       {{1ULL<<23, 1ULL<<24, 1ULL<<25}}, 0);

  io::SparseExtents sparse_extents_2;
  sparse_extents_2.insert(0, 65536, {io::SPARSE_EXTENT_STATE_DATA, 65536});
  sparse_extents_2.insert(65536, 65536,
                         {io::SPARSE_EXTENT_STATE_ZEROED, 65536});
  expect_read_l2_table(*mock_stream_interface, 1ULL<<23,
                       {{1ULL<<26, QCOW_OFLAG_ZERO, 1ULL<<25}}, 0);

  io::SparseExtents sparse_extents_head;
  sparse_extents_head.insert(131072, 65536,
                             {io::SPARSE_EXTENT_STATE_DATA, 65536});
  expect_read_l2_table(*mock_stream_interface, 1ULL<<28,
                       {{1ULL<<26, QCOW_OFLAG_ZERO, 1ULL<<27}}, 0);

  expect_stream_close(*mock_stream_interface, 0);

  MockQCOWFormat mock_qcow_format(json_object, &mock_source_spec_builder);

  C_SaferCond ctx1;
  mock_qcow_format.open(mock_dst_image_ctx.md_ctx, &mock_dst_image_ctx,
                        &mock_src_image_ctx, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SnapshotDelta snapshot_delta;
  mock_qcow_format.list_snaps({{0, 196608}}, {1, CEPH_NOSNAP}, 0,
                              &snapshot_delta, {}, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{1, 1}] = sparse_extents_1;
  expected_snapshot_delta[{CEPH_NOSNAP, 2}] = sparse_extents_2;
  expected_snapshot_delta[{CEPH_NOSNAP, CEPH_NOSNAP}] = sparse_extents_head;
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);

  C_SaferCond ctx3;
  mock_qcow_format.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

} // namespace migration
} // namespace librbd
