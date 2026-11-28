#ifndef CEPH_TEST_CLS_RGW_MAINTENANCE_WIRE_H
 #define CEPH_TEST_CLS_RGW_MAINTENANCE_WIRE_H

#include "cls/rgw/cls_rgw_ops.h"

namespace rgw_cls_test::maintenance {

// Independent native encoding oracle for the version-1 maintenance frames.
inline void encode_cursor(const rgw_cls_index_cursor& cursor, ceph::bufferlist& output)
{
  ceph::encode(cursor.version, output);
  ceph::encode(cursor.identity, output);
  ceph::encode(static_cast<uint8_t>(cursor.phase), output);
  ceph::encode(cursor.marker, output);
}

inline void decode_cursor(rgw_cls_index_cursor& cursor, ceph::bufferlist::const_iterator& input)
{
  ceph::decode(cursor.version, input);
  ceph::decode(cursor.identity, input);
  uint8_t phase;
  ceph::decode(phase, input);
  cursor.phase = static_cast<rgw_cls_index_phase>(phase);
  ceph::decode(cursor.marker, input);
}

inline ceph::bufferlist check_request(const rgw_cls_index_cursor& cursor,
                                      const uint32_t rows = RGW_CLS_INDEX_PAGE_ROWS,
                                      const uint64_t bytes = RGW_CLS_INDEX_PAGE_BYTES)
{
  ceph::bufferlist output;
  ENCODE_START(1, 1, output);
  encode_cursor(cursor, output);
  ceph::encode(rows, output);
  ceph::encode(bytes, output);
  ENCODE_FINISH(output);

  return output;
}

inline ceph::bufferlist rebuild_request(const rgw_cls_index_cursor& cursor,
                                        const rgw_bucket_dir_stats& stats)
{
  ceph::bufferlist output;
  ENCODE_START(1, 1, output);
  encode_cursor(cursor, output);
  ceph::encode(stats, output);
  ENCODE_FINISH(output);

  return output;
}

struct page final
{
  rgw_cls_index_cursor cursor;
  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_stats stats;

public:
  ceph::bufferlist encoded() const
  {
    ceph::bufferlist output;
    ENCODE_START(1, 1, output);
    encode_cursor(cursor, output);
    existing_header.encode(output);
    ceph::encode(stats, output);
    ENCODE_FINISH(output);

    return output;
  }
};

inline page decode_page(const ceph::bufferlist& input)
{
  page result;
  auto position = input.cbegin();
  DECODE_START(1, position);
  decode_cursor(result.cursor, position);
  result.existing_header.decode(position);
  ceph::decode(result.stats, position);
  DECODE_FINISH(position);
  if (!position.end()) {
    throw ceph::buffer::malformed_input("trailing maintenance reply bytes");
  }

  return result;
}

} // namespace rgw_cls_test::maintenance

#endif
