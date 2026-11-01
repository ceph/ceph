// Supply deterministic native RGW replies in place of a live CLS call.

#ifndef DATUM_TUTORIAL_BUCKET_PAGES_H
 #define DATUM_TUTORIAL_BUCKET_PAGES_H

#include <array>
#include <utility>
#include <stdexcept>

#include <cls/rgw/cls_rgw_ops.h>

namespace tutorial_fixture {

inline auto bucket_page(const ::ceph::buffer::list& encoded_request)
{

    // Check the request with RGW's native decoder:
    ::rgw_cls_list_op request;
    auto input = encoded_request.cbegin();
    ::decode(request, input);
    if (request.filter_prefix != "photos/" || !request.delimiter.empty() ||
        request.num_entries != 2 || request.list_versions ||
        !request.start_obj.instance.empty() ||
        (!request.start_obj.name.empty() && request.start_obj.name != "photos/bee.jpg")) {
        throw std::invalid_argument {"unexpected bucket-list fixture request"};
    }

    // Supply two native reply pages with an explicit continuation marker:
    const std::array names {"photos/ant.jpg", "photos/bee.jpg", "photos/cat.jpg"};
    const auto first = request.start_obj.name.empty() ? 0u : 2u;
    const auto finish = first == 0 ? 2u : 3u;
    ::rgw_cls_list_ret reply;
    for (auto index = first; index != finish; ++index) {
        ::rgw_bucket_dir_entry entry;
        entry.key = {names[index], ""};
        entry.exists = true;
        reply.dir.m.emplace(names[index], std::move(entry));
    }
    reply.is_truncated = first == 0;
    reply.marker = {names[finish - 1], ""};

    ::ceph::buffer::list output;
    ::encode(reply, output);

    return output;
}

} // tutorial_fixture

#endif // DATUM_TUTORIAL_BUCKET_PAGES_H
