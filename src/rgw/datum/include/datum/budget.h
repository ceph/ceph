// Datum: Explicit cumulative resource charges and scoped nesting depth.
// Data operations charge before work; accounting needs cooperating codecs/allocators.
// Please keep admission policy and native allocation/error handling in bindings.
#ifndef CEPH_RGW_DATUM_BUDGET_H
 #define CEPH_RGW_DATUM_BUDGET_H

#include <cstddef>
#include <limits>

#include <optional>
#include <stdexcept>
#include <utility>

#include <array>

namespace ceph::salad::datum {

enum struct resource { input_bytes, payload_bytes, elements, allocation_bytes, work, depth };

struct resource_limits final
{
    std::optional<std::size_t> input_bytes = {};
    std::optional<std::size_t> payload_bytes = {};
    std::optional<std::size_t> elements = {};
    std::optional<std::size_t> allocation_bytes = {};
    std::optional<std::size_t> work = {};

    std::optional<std::size_t> depth = {};

public:
    constexpr std::optional<std::size_t> maximum(const resource kind) const
    {
        switch (kind) {
        case resource::input_bytes:
            return input_bytes;
        case resource::payload_bytes:
            return payload_bytes;
        case resource::elements:
            return elements;
        case resource::allocation_bytes:
            return allocation_bytes;
        case resource::work:
            return work;
        case resource::depth:
            return depth;
        }
        throw std::invalid_argument {"unknown data resource"};
    }

};

class resource_budget final
{
    resource_limits limits;

    std::array<std::size_t, 6> usage {};

public:
    class depth_guard final
    {
        resource_budget *budget;

        friend class resource_budget;

    private:
        constexpr explicit depth_guard(resource_budget& owner) : budget(&owner)
        {
            budget->charge(resource::depth, 1);
        }

    public:
        depth_guard(const depth_guard&) = delete;
        constexpr depth_guard(depth_guard&& other) noexcept
            : budget(std::exchange(other.budget, nullptr))
        {}

    public:
        constexpr ~depth_guard()
        {
            if (budget) {
                --budget->usage[static_cast<std::size_t>(resource::depth)];
            }
        }

    };

public:
    constexpr explicit resource_budget(resource_limits bounds = {}) : limits(bounds)
    {}
    resource_budget(const resource_budget&) = delete;
    resource_budget(resource_budget&&) = delete;

public:
    constexpr std::size_t used(const resource kind) const
    {
        // maximum() also checks an invalid enumerator before indexing.
        limits.maximum(kind);

        return usage[static_cast<std::size_t>(kind)];
    }

public:
    constexpr void charge(const resource kind, const std::size_t amount)
    {
        const auto maximum = limits.maximum(kind);
        auto& current = usage[static_cast<std::size_t>(kind)];
        if (amount > std::numeric_limits<std::size_t>::max() - current ||
            (maximum && (current > *maximum || amount > *maximum - current))) {
            throw std::length_error {"data resource budget exceeded"};
        }
        current += amount;
    }

public:
    constexpr depth_guard enter_depth() { return depth_guard {*this}; }

};

struct unlimited_budget final
{
    // Retain ordinary RAII spelling without unused-variable warnings; the
    // empty destructor and all accounting disappear after inlining.
    struct depth_guard final
    {
        constexpr ~depth_guard() {}

    };

public:
    constexpr void charge(resource, std::size_t) const noexcept {}

public:
    constexpr depth_guard enter_depth() const noexcept { return {}; }

};

} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_BUDGET_H
