// RGW CLS: An independent owning client representation for OLH log pages.
// Epoch groups retain native numeric order and first-wins duplicate semantics.
#ifndef CEPH_CLS_RGW_OLH_TYPES_H
 #define CEPH_CLS_RGW_OLH_TYPES_H

#include <cstdint>
#include <utility>

#include <string>
#include <vector>

namespace cls::rgw::olh {

// These values are the OLHLogOp wire tags, independent of that native enum's storage width.
// Unrecognized byte values remain representable for the replay error path.
enum struct action : std::uint8_t
{
    unknown = 0,
    link = 1,
    unlink = 2,
    remove_instance = 3,
    stale = 4
};

struct object_key final
{
    std::string name;
    std::string instance;
};

struct event final
{
    object_key object;
    std::string tag;

    std::uint64_t epoch = 0;
    action operation = action::unknown;
    bool delete_marker = false;
};

// The group key drives pagination/trim; event.epoch drives link arbitration. They need not match.
using epoch_group = std::pair<std::uint64_t, std::vector<event>>;

struct reply final
{
    std::vector<epoch_group> epochs;

    bool truncated = false;
};

} // cls::rgw::olh

#endif // CEPH_CLS_RGW_OLH_TYPES_H
