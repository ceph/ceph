// Datum: Fixed and fragmented byte transfers, cursors and scoped slices.
// A data boundary above the mathematical core, independent of any codec.
//
// Please keep native buffers and transport ownership in consumer adapters.
// These spans borrow storage; transferring bytes does not establish admission.

#ifndef CEPH_RGW_DATUM_BYTES_H
 #define CEPH_RGW_DATUM_BYTES_H

#include <concepts>
#include <cstddef>
#include <limits>
#include <type_traits>

#include <stdexcept>
#include <utility>

#include <span>

#include <cstring>

#include <algorithm>
#include <functional>
#include <iterator>
#include <ranges>

namespace ceph::salad::datum {

template <typename T>
concept byte_value =
    std::same_as<std::remove_cv_t<T>, std::byte> || std::same_as<std::remove_cv_t<T>, char> ||
    std::same_as<std::remove_cv_t<T>, unsigned char> ||
    std::same_as<std::remove_cv_t<T>, signed char>;

template <typename T>
concept byte_block = std::ranges::contiguous_range<T> && std::ranges::sized_range<T> &&
                     byte_value<std::ranges::range_value_t<T>>;

template <typename T>
concept byte_fragments =
    std::ranges::forward_range<T> && std::ranges::forward_range<const std::remove_reference_t<T>> &&
    byte_block<std::ranges::range_reference_t<T>> &&
    byte_block<std::ranges::range_reference_t<const std::remove_reference_t<T>>>;

namespace detail {

template <byte_value T, byte_value U>
constexpr T byte_cast(U value)
{
    if constexpr (std::same_as<std::remove_cv_t<U>, std::byte>) {
        return static_cast<T>(std::to_integer<unsigned char>(value));
    }

    return static_cast<T>(static_cast<unsigned char>(value));
}

// Capacity has been admitted by the caller. Representation copying handles
// every byte spelling; constant evaluation and volatile access use value reads.
template <byte_value SourceT, byte_value DestinationT>
constexpr void copy_byte_block(const SourceT *source, DestinationT *destination,
                               const std::size_t count)
{
    auto copy_values = [source, destination, count] {
        for (std::size_t index = 0; index < count; ++index) {
            destination[index] = byte_cast<std::remove_cv_t<DestinationT>>(source[index]);
        }
    };
    if consteval {
        copy_values();

        return;
    }

    if constexpr (!std::is_volatile_v<SourceT> && !std::is_volatile_v<DestinationT>) {
        if (count) {
            std::memcpy(destination, source, count);
        }

        return;
    }
    copy_values();
}
} // detail

// Preflight capacity before the first write. Source and destination must not
// overlap. Fragment descriptors and their backing storage survive this call.
template <byte_fragments FragmentsT>
constexpr std::size_t byte_capacity(const FragmentsT& fragments)
{
    std::size_t total = 0;
    for (const auto& fragment : fragments) {
        const auto size = std::ranges::size(fragment);
        if (size > std::numeric_limits<std::size_t>::max() - total) {
            throw std::length_error {"byte fragment size overflow"};
        }
        total += size;
    }

    return total;
}

template <byte_value ByteT, std::size_t Extent, byte_fragments FragmentsT>
requires (!std::is_const_v<std::remove_reference_t<
             std::ranges::range_reference_t<std::ranges::range_reference_t<FragmentsT>>>>)
constexpr std::size_t scatter_bytes(std::span<ByteT, Extent> source, FragmentsT&& destination)
{
    if (std::size(source) > byte_capacity(destination)) {
        throw std::length_error {"byte fragments have insufficient capacity"};
    }
    std::size_t copied = 0;
    for (auto&& fragment : destination) {
        const auto count =
            std::min<std::size_t>(std::size(source) - copied, std::ranges::size(fragment));
        detail::copy_byte_block(std::data(source) + copied, std::ranges::data(fragment), count);
        copied += count;
        if (copied == std::size(source)) {
            break;
        }
    }

    return copied;
}

template <byte_fragments FragmentsT, byte_value ByteT, std::size_t Extent>
requires (!std::is_const_v<ByteT>)
constexpr std::span<ByteT> gather_bytes(const FragmentsT& source,
                                        std::span<ByteT, Extent> destination)
{
    const auto size = byte_capacity(source);
    if (size > std::size(destination)) {
        throw std::length_error {"byte staging area has insufficient capacity"};
    }
    std::size_t copied = 0;
    for (const auto& fragment : source) {
        const auto count = std::min<std::size_t>(size - copied, std::ranges::size(fragment));
        detail::copy_byte_block(std::ranges::data(fragment), std::data(destination) + copied,
                                count);
        copied += count;
        if (copied == size) {
            break;
        }
    }

    return destination.first(size);
}

// Datum: Bounded, borrowing byte slices and cursors over standard fragments.
// Descriptors retain positions; byte storage stays with the caller's owner.
// Please keep native buffers, I/O and transport lifetime rules in bindings.

template <std::forward_iterator IteratorT>
requires byte_block<std::iter_reference_t<IteratorT>>
class byte_slice_view final : public std::ranges::view_interface<byte_slice_view<IteratorT>>
{
    IteratorT first {};

    std::size_t offset = 0;
    std::size_t length = 0;

public:
    using byte_type =
        std::remove_reference_t<std::ranges::range_reference_t<std::iter_reference_t<IteratorT>>>;

    class iterator final
    {
        IteratorT fragment {};
        std::size_t offset = 0;
        std::size_t remaining = 0;

    private:
        constexpr void skip_empty()
        {
            while (remaining && offset == std::ranges::size(*fragment)) {
                ++fragment;
                offset = 0;
            }
        }

    public:
        using value_type = std::span<const byte_type>;
        using difference_type = std::ptrdiff_t;
        using iterator_concept = std::forward_iterator_tag;
        using iterator_category = std::input_iterator_tag;

    public:
        constexpr iterator() = default;
        constexpr iterator(IteratorT source, const std::size_t start, const std::size_t count)
            : fragment(source), offset(start), remaining(count)
        {
            skip_empty();
        }

    public:
        constexpr value_type operator*() const
        {
            const auto& block = *fragment;

            return std::span<const byte_type> {std::ranges::data(block), std::ranges::size(block)}
                .subspan(offset,
                         std::min<std::size_t>(remaining, std::ranges::size(block) - offset));
        }

        constexpr iterator& operator++()
        {
            remaining -= std::size(**this);
            if (remaining) {
                ++fragment;
                offset = 0;
                skip_empty();
            }

            return *this;
        }

        constexpr iterator operator++(int)
        {
            auto saved = *this;
            ++*this;

            return saved;
        }

        constexpr bool operator==(const iterator&) const = default;
        constexpr bool operator==(std::default_sentinel_t) const { return 0 == remaining; }

    };

public:
    constexpr byte_slice_view() = default;
    constexpr byte_slice_view(IteratorT source, const std::size_t start, const std::size_t count)
        : first(source), offset(start), length(count)
    {}

public:
    constexpr iterator begin() const { return {first, offset, length}; }

    constexpr std::default_sentinel_t end() const { return {}; }

public:
    // Range elements are fragments; byte count is deliberately not range size().
    constexpr std::size_t byte_size() const noexcept { return length; }

};

template <byte_fragments FragmentsT>
class byte_cursor final
{
    std::ranges::iterator_t<const FragmentsT> current;

    std::ranges::sentinel_t<const FragmentsT> finish;
    std::size_t offset = 0;
    std::size_t available;

    std::size_t capacity;

private:
    constexpr void skip_empty()
    {
        while (current != finish && offset == std::ranges::size(*current)) {
            ++current;
            offset = 0;
        }
    }

    template <typename ConsumerT>
    constexpr void advance(const std::size_t count, ConsumerT&& consume_block)
    {
        if (count > available) {
            throw std::length_error {"byte cursor has insufficient input"};
        }
        auto left = count;
        while (left) {
            const auto& block = *current;
            const auto step = std::min<std::size_t>(left, std::ranges::size(block) - offset);
            std::invoke(consume_block, block, offset, count - left, step);
            offset += step;
            left -= step;
            available -= step;
            skip_empty();
        }
    }

public:
    using slice_type = byte_slice_view<std::ranges::iterator_t<const FragmentsT>>;

    class bookmark final
    {
        friend class byte_cursor;

        const byte_cursor *owner;

        std::ranges::iterator_t<const FragmentsT> fragment;

        std::size_t offset;
        std::size_t available;

    private:
        constexpr bookmark(const byte_cursor& cursor)
            : owner(&cursor), fragment(cursor.current), offset(cursor.offset),
              available(cursor.available)
        {}

    };

public:
    constexpr explicit byte_cursor(const FragmentsT& fragments)

        : current(std::ranges::begin(fragments)), finish(std::ranges::end(fragments)),
          available(byte_capacity(fragments)), capacity(available)
    {
        skip_empty();
    }

    byte_cursor(FragmentsT&&) = delete;
    byte_cursor(const FragmentsT&&) = delete;
    byte_cursor(const byte_cursor&) = delete;
    byte_cursor(byte_cursor&&) = delete;

public:
    constexpr std::size_t remaining() const noexcept { return available; }

public:
    constexpr std::size_t position() const noexcept { return capacity - available; }

public:
    constexpr bookmark mark() const { return bookmark {*this}; }

public:
    constexpr void restore(const bookmark& saved)
    {
        if (saved.owner != this) {
            throw std::invalid_argument {"byte bookmark belongs to another cursor"};
        }
        current = saved.fragment;
        offset = saved.offset;
        available = saved.available;
    }

public:
    constexpr void consume(const std::size_t count)
    {
        advance(count, [](const auto&, std::size_t, std::size_t, std::size_t) {});
    }

public:
    constexpr slice_type take(const std::size_t count)
    {
        const auto slice = slice_type {current, offset, count};
        consume(count);

        return slice;
    }

public:
    template <byte_value ByteT, std::size_t Extent>
    requires (!std::is_const_v<ByteT>)
    constexpr void read_into(std::span<ByteT, Extent> destination)
    {
        advance(std::size(destination),
                [&destination](const auto& block, const std::size_t source_offset,
                               const std::size_t output_offset, const std::size_t count) {
                    detail::copy_byte_block(std::ranges::data(block) + source_offset,
                                            std::data(destination) + output_offset, count);
                });
    }

};

template <typename FragmentsT>
byte_cursor(const FragmentsT&) -> byte_cursor<FragmentsT>;

// Synchronous consumption: retention of the borrowed blocks is a binding policy.
template <byte_fragments FragmentsT, typename SinkT>
requires requires(SinkT& sink, std::ranges::range_reference_t<const FragmentsT> block) {
    { std::invoke(sink, block) } -> std::same_as<void>;
}
constexpr void write_fragments(const FragmentsT& fragments, SinkT&& sink)
{
    for (const auto& block : fragments) {
        std::invoke(sink, block);
    }
}

struct byte_slice final
{
    std::size_t offset;
    std::size_t length;

public:
    template <byte_block BlockT>

    constexpr auto operator()(const BlockT& block) const
    {
        if (offset > std::ranges::size(block) || length > std::ranges::size(block) - offset) {
            throw std::length_error {"byte slice exceeds its owner"};
        }
        using byte_type = std::remove_reference_t<std::ranges::range_reference_t<const BlockT>>;

        return std::span<const byte_type> {std::ranges::data(block), std::ranges::size(block)}
            .subspan(offset, length);
    }

    template <typename BlockT>
    void operator()(const BlockT&&) const = delete;

};

} // ceph::salad::datum

namespace std::ranges {

template <typename IteratorT>
inline constexpr bool enable_borrowed_range<::ceph::salad::datum::byte_slice_view<IteratorT>> =
    true;
} // std::ranges

#endif // CEPH_RGW_DATUM_BYTES_H
