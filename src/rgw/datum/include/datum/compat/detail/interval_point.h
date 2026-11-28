// Datum: Temporary GCC 12 string ownership for constant evaluation.
#ifndef CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_POINT_H
 #define CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_POINT_H

#include <optional>
#include <string>
#include <type_traits>
#include <utility>

namespace ceph::salad::datum::compat::detail {

// Only boundary's private storage uses this subset of optional<string>.
// Remove with the GCC 12 workaround; other toolchains already use std::optional.
class string_point_storage final
{
    using storage_type = std::optional<std::string>;

    storage_type storage;

    constexpr void copy_and_clear(std::string& source)
    {
        storage.emplace(source);
        source.clear();
    }

public:
    constexpr string_point_storage() = default;
    constexpr string_point_storage(std::nullopt_t) noexcept
        : storage(std::nullopt)
    {}

    constexpr string_point_storage(const string_point_storage&) = default;

    constexpr string_point_storage(std::in_place_t, const std::string& source)
        : storage(std::in_place, source)
    {}

    // GCC 12 loses string identity through engaged temporaries in constant evaluation.
    // Runtime initializes directly; constant evaluation emplaces after this owner is known.
    constexpr string_point_storage(std::in_place_t, std::string&& source) noexcept
        : storage(std::is_constant_evaluated() ? storage_type {}
                                               : storage_type {std::in_place, std::move(source)})
    {
        if consteval {
            copy_and_clear(source);
        }
    }

    constexpr string_point_storage(string_point_storage&& source) noexcept
        : storage(std::is_constant_evaluated() ? storage_type {}
                                               : storage_type {std::move(source.storage)})
    {
        if consteval {
            if (source.storage) {
                copy_and_clear(*source.storage);
            }
        }
    }

public:
    constexpr string_point_storage& operator=(const string_point_storage&) = default;

    constexpr string_point_storage& operator=(string_point_storage&& source) noexcept
    {
        if consteval {
            storage = source.storage;
            if (source.storage) {
                source.storage->clear();
            }

            return *this;
        }

        storage = std::move(source.storage);

        return *this;
    }

public:
    constexpr const std::string& operator*() const noexcept { return *storage; }

    constexpr std::string& emplace(const std::string& source)
    {
        return storage.emplace(source);
    }

};

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_POINT_H
