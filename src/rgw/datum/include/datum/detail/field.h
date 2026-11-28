// Datum: Member projection policy and ordered, scoped field observation.
#ifndef CEPH_RGW_DATUM_DETAIL_FIELD_H
 #define CEPH_RGW_DATUM_DETAIL_FIELD_H

#include <concepts>
#include <cstddef>
#include <functional>
#include <utility>

namespace ceph::salad::datum {

// Consumers may select a value view (for example string_view) instead of the
// default const reference. Projection must not outlive the source's storage.
template <typename T>
struct member_view
{
    using type = const T&;

public:
    static constexpr type get(const T& value) noexcept { return value; }

};

// Enumerate lenses before reading values so policy can omit an observation.
// Effects and exceptions belong to the consumer; a void result is not progress.
template <auto ...Lens>
struct field_order
{
    static constexpr std::size_t size = sizeof...(Lens);

public:
    template <typename SinkT>
    requires (requires(SinkT& sink) {
        { std::invoke(sink, Lens) } -> std::same_as<void>;
    } and ...)
    static constexpr void visit(SinkT&& sink)
    {
        (std::invoke(sink, Lens), ...);
    }

public:
    template <typename ValueT, typename SinkT>
    requires (requires(const ValueT& value, SinkT& sink) {
        { std::invoke(sink, std::invoke(Lens, value)) } -> std::same_as<void>;
    } and ...)
    static constexpr void observe(const ValueT& value, SinkT&& sink)
    {
        visit([&value, &sink](const auto& lens) { std::invoke(sink, std::invoke(lens, value)); });
    }

};

} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_DETAIL_FIELD_H
