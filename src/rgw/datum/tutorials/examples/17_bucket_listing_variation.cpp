// Read RGW bucket-index pages through native consumer bindings and keep the server's next marker.

#include <print>
#include <optional>

#include <cls/rgw/cls_rgw_types.h>

#include <datum/traversal.h>
#include <rgw/datum/adapters/cls/bucket.h>

#include "fixtures/16_bucket_pages.h"

namespace traversal = ::ceph::salad::datum::traversal;
namespace bucket = ::cls::rgw::bucket_datum;

using bucket_marker = ::cls_rgw_obj_key;

int main()
{
    const bucket_marker start {};

    // Visit CLS listing pages; return progress and an optional resume marker:
    const auto result = traversal::for_each_page(
        start,

        // Use the production wire bindings; the fixture supplies the CLS reply:
        [](const bucket_marker& marker) {
            const auto request = bucket::encode_request(marker, "photos/", "", 2, false);
            const auto response = tutorial_fixture::bucket_page(request);

            return bucket::decode_reply<bucket::flat_entries>(response);
        },

        // Resume with the server's marker whenever it reports more work:
        [](const auto&, const auto& page) -> std::optional<bucket_marker> {
            return page.is_truncated ? std::optional {page.marker} : std::nullopt;
        },

        // Read object names from the decoded RGW directory entries:
        [](const auto& page) {
            for (const auto& [index_key, entry] : page.dir.m) {
                std::println("object: {}", entry.key.name);
            }

            return traversal::page_action::stop;
        });

    std::println("pages: {}", result.pages);
    std::println("complete: {}", result.complete());
    if (result.resume) {
        std::println("resume after: {}", result.resume->name);
    }
}
