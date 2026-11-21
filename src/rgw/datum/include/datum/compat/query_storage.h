// Datum: Internal bounded storage for the temporary C++23 query implementation.

#ifndef CEPH_RGW_DATUM_COMPAT_QUERY_STORAGE_H
 #define CEPH_RGW_DATUM_COMPAT_QUERY_STORAGE_H

#include <cstddef>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace ceph::salad::datum::compat::detail {

// C++26: restore std::inplace_vector once constexpr, layout, and exception checks pass.
// Keep one storage representation across dialects to preserve enclosing type definitions.
// QueryT is Datum query storage only: its default owns no points or other resources.
template <typename QueryT, std::size_t Capacity>
class query_storage final
{
    using T = QueryT;

    static_assert(std::is_nothrow_default_constructible_v<T>);

    union raw_storage final
    {
        T values[Capacity];
        std::byte bytes[sizeof(T) * Capacity];

    public:
        constexpr explicit raw_storage(std::true_type) noexcept : values {}
        {}

        explicit raw_storage(std::false_type) noexcept
        {
            // Beginning byte-array lifetime implicitly creates the containing T array.
            ::new (static_cast<void *>(std::addressof(bytes))) std::byte[sizeof(T) * Capacity];
        }

        raw_storage(const raw_storage&) = default;
        raw_storage(raw_storage&&) = default;

        raw_storage& operator=(const raw_storage&) = default;
        raw_storage& operator=(raw_storage&&) = default;

        ~raw_storage() requires std::is_trivially_destructible_v<T> = default;

        constexpr ~raw_storage()
        requires (!std::is_trivially_destructible_v<T>)
        {}

    } raw = std::is_constant_evaluated() ? raw_storage(std::true_type {})
                                         : raw_storage(std::false_type {});

    using count_type = std::conditional_t<
        Capacity <= std::numeric_limits<unsigned char>::max() and alignof(T) <= sizeof(unsigned char),
        unsigned char,
        std::conditional_t<
            Capacity <= std::numeric_limits<unsigned short>::max() and
                alignof(T) <= sizeof(unsigned short),
            unsigned short,
            std::conditional_t<
                Capacity <= std::numeric_limits<unsigned>::max() and alignof(T) <= sizeof(unsigned),
                unsigned, std::size_t>>>;
    count_type count = 0;

    constexpr T *data() noexcept
    {
        if (std::is_constant_evaluated()) {
            return raw.values;
        }

        return *std::launder(reinterpret_cast<T (*)[Capacity]>(std::addressof(raw)));
    }

    constexpr const T *data() const noexcept
    {
        if (std::is_constant_evaluated()) {
            return raw.values;
        }

        return *std::launder(reinterpret_cast<const T (*)[Capacity]>(std::addressof(raw)));
    }

    constexpr void clear() noexcept
    {
        while (count) {
            std::destroy_at(data() + --count);
        }
    }

    template <bool Move, typename Source>
    static constexpr decltype(auto) element_from(Source& source, const std::size_t index)
    {
        if constexpr (Move) {
            return std::move(source.data()[index]);
        }

        if constexpr (!Move) {
            return (source.data()[index]);
        }
    }

    template <bool Move, typename Source>
    constexpr void construct_from(Source& source)
    {
        try
         {
            for (std::size_t index = 0; index != source.size(); ++index) {
                unchecked_emplace_back(element_from<Move>(source, index));
            }
         }
        catch (...)
         {
            clear();

            throw;
         }
    }

    template <bool Move, typename Source>
    constexpr void assign_from(Source& source)
    {
        if (this == std::addressof(source)) {
            return;
        }

        std::size_t index = 0;
        for (; index < count and index < source.size(); ++index) {
            data()[index] = element_from<Move>(source, index);
        }

        const auto original_count = count;

        try
         {
            for (; index < source.size(); ++index) {
                unchecked_emplace_back(element_from<Move>(source, index));
            }
         }
        catch (...)
         {
            while (count > original_count) {
                std::destroy_at(data() + --count);
            }

            throw;
         }

        while (count > source.size()) {
            std::destroy_at(data() + --count);
        }
    }

public:
    using const_iterator = const T *;

public:
    constexpr query_storage() noexcept = default;
    query_storage(const query_storage&)
    requires std::is_trivially_copy_constructible_v<T> = default;
    constexpr query_storage(const query_storage& source)
        noexcept(std::is_nothrow_copy_constructible_v<T>)
    requires (!std::is_trivially_copy_constructible_v<T>) and std::is_copy_constructible_v<T>
    {
        construct_from<false>(source);
    }

    query_storage(query_storage&&)
    requires std::is_trivially_move_constructible_v<T> = default;
    constexpr query_storage(query_storage&& source)
        noexcept(std::is_nothrow_move_constructible_v<T>)
    requires (!std::is_trivially_move_constructible_v<T>) and std::is_move_constructible_v<T>
    {
        construct_from<true>(source);
    }

public:
    ~query_storage() requires std::is_trivially_destructible_v<T> = default;

    constexpr ~query_storage()
    requires (!std::is_trivially_destructible_v<T>)
    {
        clear();
    }

public:
    query_storage& operator=(const query_storage&)
    requires std::is_trivially_copy_constructible_v<T> and std::is_trivially_copy_assignable_v<T>
    = default;
    constexpr query_storage& operator=(const query_storage& source)
    requires (!(std::is_trivially_copy_constructible_v<T> and
                std::is_trivially_copy_assignable_v<T>)) and
             std::is_copy_constructible_v<T> and std::is_copy_assignable_v<T>
    {
        assign_from<false>(source);

        return *this;
    }

    query_storage& operator=(query_storage&&)
    requires std::is_trivially_move_constructible_v<T> and std::is_trivially_move_assignable_v<T>
    = default;
    constexpr query_storage& operator=(query_storage&& source)
        noexcept(std::is_nothrow_move_constructible_v<T> and std::is_nothrow_move_assignable_v<T>)
    requires (!(std::is_trivially_move_constructible_v<T> and
                std::is_trivially_move_assignable_v<T>)) and
             std::is_move_constructible_v<T> and std::is_move_assignable_v<T>
    {
        assign_from<true>(source);

        return *this;
    }

public:
    template <typename ...Args>
    constexpr T& unchecked_emplace_back(Args&& ...arguments)
    {
        T *value = std::construct_at(data() + count, std::forward<Args>(arguments)...);
        ++count;

        return *value;
    }

    constexpr bool empty() const noexcept { return 0 == count; }
    constexpr std::size_t size() const noexcept { return count; }
    constexpr const T& operator[](const std::size_t index) const noexcept { return data()[index]; }
    constexpr const_iterator begin() const noexcept { return data(); }
    constexpr const_iterator end() const noexcept { return data() + count; }

};

template <typename QueryT>
class query_storage<QueryT, 0> final
{
    using T = QueryT;

public:
    using const_iterator = const T *;

    template <typename ...Args>
    [[noreturn]] constexpr T& unchecked_emplace_back(Args&& ...) { std::terminate(); }

    constexpr bool empty() const noexcept { return true; }
    constexpr std::size_t size() const noexcept { return 0; }
    [[noreturn]] constexpr const T& operator[](std::size_t) const noexcept { std::terminate(); }
    constexpr const_iterator begin() const noexcept { return nullptr; }
    constexpr const_iterator end() const noexcept { return nullptr; }

};

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_QUERY_STORAGE_H
