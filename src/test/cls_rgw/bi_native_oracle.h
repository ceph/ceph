// RGW CLS: Plain declarations for an independently compiled native wire oracle.
#ifndef CEPH_TEST_CLS_RGW_BI_NATIVE_ORACLE_H
 #define CEPH_TEST_CLS_RGW_BI_NATIVE_ORACLE_H

#include "include/buffer.h"

#include <cstdint>

#include <map>
#include <string>
#include <vector>

namespace rgw_bi_oracle {

struct row final
{
    std::uint8_t kind;
    std::string key;
    ::ceph::buffer::list payload;
};

struct request final
{
    std::uint32_t maximum;
    std::string filter;
    std::string marker;
    bool reshard;
};

struct get_request final
{
    std::string name;
    std::string instance;
    std::uint8_t kind;
};

::ceph::buffer::list encode_get_request(const get_request& value, std::uint8_t version = 1);
get_request decode_get_request(const ::ceph::buffer::list& bytes);
::ceph::buffer::list encode_get_reply(const row& value);
row decode_get_reply(const ::ceph::buffer::list& bytes);

::ceph::buffer::list encode_request(const request& value, std::uint8_t version = 2);
request decode_request(const ::ceph::buffer::list& bytes);
::ceph::buffer::list encode_reply(const std::vector<row>& rows, bool truncated);
std::vector<row> decode_reply(const ::ceph::buffer::list& bytes, bool& truncated);
::ceph::buffer::list encode_put_request(const std::vector<row>& rows, bool check_existing);
std::vector<row> decode_put_request(const ::ceph::buffer::list& bytes, bool& check_existing);
::ceph::buffer::list encode_put_entry(const row& value);
row decode_put_entry(const ::ceph::buffer::list& bytes);
::ceph::buffer::list extended_put_entry(const row& value);
::ceph::buffer::list extended_reply(const std::vector<row>& rows);

using epoch_groups = std::map<std::uint64_t, std::vector<std::uint32_t>>;

::ceph::buffer::list encode_groups(const epoch_groups& groups);
epoch_groups decode_groups(const ::ceph::buffer::list& bytes);

} // rgw_bi_oracle

#endif // CEPH_TEST_CLS_RGW_BI_NATIVE_ORACLE_H
