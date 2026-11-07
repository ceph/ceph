// RGW admin: compare independent OLH pages with the established diagnostic output.
#include "radosgw-admin/olh.h"
#include "rgw/datum/adapters/cls/olh.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "common/JSONFormatter.h"
#include "common/XMLFormatter.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <sstream>
#include <string>

namespace {

template <typename FormatterT, typename DrawT>
std::string render(const bool pretty, DrawT&& draw)
{
    FormatterT formatter {pretty};
    draw(formatter);
    std::ostringstream output;
    formatter.flush(output);

    return output.str();
}

void check_output(const ::rgw_cls_read_olh_log_ret& native,
                  const ::cls::rgw::olh::reply& page)
{
    const auto original = [&native](::ceph::Formatter& formatter) {
        formatter.open_object_section("result");
        ::encode_json("is_truncated", native.is_truncated, &formatter);
        ::encode_json("log", native.log, &formatter);
        formatter.close_section();
    };
    const auto independent = [&page](::ceph::Formatter& formatter) {
        ::rgw::admin::dump_olh_log(page, formatter);
    };
    for (const bool pretty : {false, true}) {
        CHECK(render<::ceph::JSONFormatter>(pretty, independent) ==
              render<::ceph::JSONFormatter>(pretty, original));
        CHECK(render<::ceph::XMLFormatter>(pretty, independent) ==
              render<::ceph::XMLFormatter>(pretty, original));
    }
}

} // anonymous namespace

TEST_CASE("Independent OLH pages retain native JSON and XML diagnostic output", "[rgw][cls][olh][admin]")
{
    for (const bool truncated : {false, true}) {
        ::rgw_cls_read_olh_log_ret native;
        native.is_truncated = truncated;
        for (const unsigned groups : {0, 1, 3}) {
            native.log.clear();
            for (unsigned index = 0; index < groups; ++index) {
                const std::uint64_t epoch =
                    2 == index ? std::numeric_limits<std::uint64_t>::max() : index;
                auto& entries = native.log[epoch];
                if (1 == index) {
                    continue;
                }
                // Values through seven are representable by the unfixed native enum.
                for (unsigned operation = 0; operation < 8; ++operation) {
                    auto& entry = entries.emplace_back();
                    entry.epoch = std::numeric_limits<std::uint64_t>::max() - operation;
                    entry.op = static_cast<::OLHLogOp>(operation);
                    entry.op_tag = std::string {"tag\0\"<&", 7};
                    entry.key.name = "object<&\"";
                    entry.key.instance = operation % 2 ? "" : std::string(128, 'v');
                    entry.delete_marker = 0 != operation % 2;
                }
            }
            ::ceph::buffer::list encoded;
            ::encode(native, encoded);
            ::cls::rgw::olh::reply page;
            ::cls::rgw::olh_datum::decode_reply(encoded, page);
            encoded.clear();
            check_output(native, page);
        }
    }
}

TEST_CASE("Independent OLH diagnostics render unknown byte actions without native enum conversion",
          "[rgw][cls][olh][admin]")
{
    ::rgw_cls_read_olh_log_ret native;
    auto& entry = native.log[7].emplace_back();
    entry.epoch = 9;
    entry.op = ::CLS_RGW_OLH_OP_UNKNOWN;
    ::cls::rgw::olh::reply page {{{7, {{{}, {}, 9,
                                      static_cast<::cls::rgw::olh::action>(255), false}}}}, false};
    check_output(native, page);
}
