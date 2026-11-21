// Read RGW bucket-index pages through native consumer bindings and keep the server's next marker.

#include <print>
#include <ranges>
#include <optional>
#include <algorithm>

#include <cls/rgw/cls_rgw_types.h>

#include <datum/traversal.h>
#include <rgw/datum/adapters/cls/bucket.h>

#include "fixtures/16_bucket_pages.h"

namespace traversal = ceph::salad::datum::traversal;
namespace bucket = cls::rgw::bucket_datum;

using bucket_marker = cls_rgw_obj_key;

int main()
{
    // The runnable example supplies native replies instead of contacting a cluster:
    const auto read_page = [](const bucket_marker& marker) -> bucket::reply {
        const ceph::buffer::list request =
            bucket::encode_request(marker, "photos/", "", 2, false);
        const ceph::buffer::list response = tutorial_fixture::bucket_page(request);

        return bucket::decode_reply<bucket::flat_entries>(response);
    };

    const bucket_marker start {};

    // Visit CLS listing pages; return progress and an optional resume marker:
    const auto result = traversal::for_each_page(
        start,

        read_page,

        // Resume with the server's marker whenever it reports more work:
        [](const bucket_marker&, const bucket::reply& page) -> std::optional<bucket_marker> {
            return page.is_truncated ? std::optional {page.marker} : std::nullopt;
        },

        // Read object names from the decoded RGW directory entries:
        [](const bucket::reply& page) {
            std::ranges::for_each(page.dir.m | std::views::values, [](const auto& entry) {
                std::println("object: {}", entry.key.name);
            });

            return traversal::page_action::stop;
        });

    std::println("pages: {}", result.pages);
    std::println("complete: {}", result.complete());
    if (result.resume) {
        std::println("resume after: {}", result.resume->name);
    }
}
