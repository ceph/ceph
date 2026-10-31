// Encode an RGW bucket-list request and decode one reply, including its next marker.

#include <print>

#include <cls/rgw/cls_rgw_types.h>

#include <rgw/datum/adapters/cls/bucket.h>

#include "fixtures/16_bucket_pages.h"

namespace bucket = ::cls::rgw::bucket_datum;

int main()
{
    const ::cls_rgw_obj_key marker {"photos/bee.jpg", ""};

    // Request up to two objects under photos/, without delimiter grouping or versions:
    const auto request = bucket::encode_request(marker, "photos/", "", 2, false);
    const auto response = tutorial_fixture::bucket_page(request);

    // Decode native directory entries and the server's continuation information:
    const auto page = bucket::decode_reply<bucket::flat_entries>(response);
    for (const auto& [index_key, entry] : page.dir.m) {
        std::println("object: {}", entry.key.name);
    }

    std::println("more: {}", page.is_truncated);
    if (page.is_truncated) {
        std::println("next marker: {}", page.marker.name);
    }
}
