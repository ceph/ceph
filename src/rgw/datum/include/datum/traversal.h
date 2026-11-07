// Datum: Range acceptance and cursor-driven page traversal.
// A generic data traversal facility, separate from interval evaluation.
//
// Please keep this layer independent of native records, resume protocols and durable commit
// semantics.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_TRAVERSAL_H
 #define CEPH_RGW_DATUM_TRAVERSAL_H

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <expected>
#include <optional>
#include <stdexcept>
#include <utility>

#include <functional>

#include <datum/compat/stop_token.h>
#include <datum/range.h>

// Datum: Explicit fallible page traversal, cancellation and resume state.
// A generic traversal facility whose readers and consumers own I/O effects.
//
// Please keep this layer independent of native cursor types, transaction rules and
// application page policy.
// Put domain-specific behavior in consumer profiles or adapters.

namespace ceph::salad::datum::traversal {

enum struct page_action { next, stop };

enum struct page_traversal_outcome { complete, stopped, cancelled, limit_reached };

struct page_traversal_options final
{
    compat::stop_token cancellation;
    std::optional<std::size_t> page_limit;
};

template <typename StateT>
struct page_traversal_result final
{
    std::optional<StateT> resume;
    std::size_t pages = 0;
    page_traversal_outcome outcome = page_traversal_outcome::complete;

public:
    constexpr bool complete() const noexcept { return page_traversal_outcome::complete == outcome; }

};

template <typename StateT, typename ErrorT>
struct page_traversal_failure final
{
    StateT resume;
    ErrorT error;
    std::size_t pages = 0;
};

namespace page_traversal_detail {

template <typename T>
struct page_result_traits final
{
    using value_type = T;

public:
    static constexpr auto& get(auto& result) noexcept { return result; }

    template <typename StateT>

    using traversal_type = page_traversal_result<StateT>;

};

template <typename ValueT, typename ErrorT>
struct page_result_traits<std::expected<ValueT, ErrorT>> final
{
    using value_type = ValueT;
    using error_type = ErrorT;

public:
    static constexpr decltype(auto) get(auto& result) noexcept { return *result; }

    template <typename StateT>

    using traversal_type =
        std::expected<page_traversal_result<StateT>, page_traversal_failure<StateT, ErrorT>>;

};

template <typename T>
concept expected = requires {
    typename page_result_traits<T>::value_type;
    typename page_result_traits<T>::error_type;
};

template <typename ReaderT, typename StateT>
using read_result = std::invoke_result_t<ReaderT&, const StateT&>;

template <typename ReaderT, typename StateT>
concept page_reader =
    std::invocable<ReaderT&, const StateT&> and
    std::same_as<read_result<ReaderT, StateT>, std::remove_cvref_t<read_result<ReaderT, StateT>>> and
    (!std::is_void_v<typename page_result_traits<read_result<ReaderT, StateT>>::value_type>);

template <typename ReaderT, typename StateT>
using page_type = typename page_result_traits<read_result<ReaderT, StateT>>::value_type;

template <typename NextT, typename StateT, typename PageT>
concept next_page =
    std::invocable<NextT&, const StateT&, PageT&> and
    std::same_as<std::invoke_result_t<NextT&, const StateT&, PageT&>, std::optional<StateT>>;

template <typename VisitorT, typename PageT>
concept page_visitor = std::invocable<VisitorT&, PageT&&> and
                       (std::same_as<std::invoke_result_t<VisitorT&, PageT&&>, void> ||
                        std::same_as<std::invoke_result_t<VisitorT&, PageT&&>, page_action>);

template <typename VisitorT, typename PageT>
requires page_visitor<VisitorT, PageT>
constexpr page_action visit(VisitorT& visitor, PageT&& page)
{
    if constexpr (std::same_as<std::invoke_result_t<VisitorT&, PageT&&>, void>) {
        std::invoke(visitor, std::forward<PageT>(page));

        return page_action::next;
    }

    if constexpr (std::same_as<std::invoke_result_t<VisitorT&, PageT&&>, page_action>) {
        return std::invoke(visitor, std::forward<PageT>(page));
    }
}

} // page_traversal_detail

// Visit fallible, cursor-driven pages without hiding I/O in iterator syntax.
// A nonterminal page must produce a different next state. "next" runs before
// the visitor, so it may move continuation data from the page into the new
// state. A stopped traversal returns the state; a read failure reports it with
// prior progress. Readers that throw return the traversal result directly and
// propagate exceptions; expected readers retain their error and resume state.
template <typename StateT, typename ReaderT, typename NextT, typename VisitorT>
requires std::move_constructible<StateT> and std::assignable_from<StateT&, StateT> and
         std::equality_comparable<StateT> and page_traversal_detail::page_reader<ReaderT, StateT> and
         page_traversal_detail::next_page<NextT, StateT,
                                          page_traversal_detail::page_type<ReaderT, StateT>> and
         page_traversal_detail::page_visitor<VisitorT,
                                             page_traversal_detail::page_type<ReaderT, StateT>>
[[nodiscard]] auto for_each_page(StateT state, ReaderT&& fetch_page, NextT&& next,
                                 VisitorT&& visitor, const page_traversal_options options = {})
{
    using read_type = page_traversal_detail::read_result<ReaderT, StateT>;
    using traits = page_traversal_detail::page_result_traits<read_type>;
    using result_type = page_traversal_result<StateT>;
    using traversal_result = traits::template traversal_type<StateT>;

    auto pages = std::size_t {0};

    for (;;) {
        if (options.cancellation.stop_requested()) {
            return traversal_result {result_type {.resume = std::move(state),
                                                  .pages = pages,
                                                  .outcome = page_traversal_outcome::cancelled}};
        }

        if (options.page_limit and pages == *options.page_limit) {
            return traversal_result {
                        result_type {.resume = std::move(state),
                                     .pages = pages,
                                     .outcome = page_traversal_outcome::limit_reached}
                    };
        }

        auto read = std::invoke(fetch_page, std::as_const(state));

        if constexpr (page_traversal_detail::expected<read_type>) {
            if (!read) {
                return traversal_result {
                            std::unexpected(page_traversal_failure<StateT, typename traits::error_type> {
                                .resume = std::move(state),
                                .error = std::move(read.error()),
                                .pages = pages})
                        };
            }
        }

        auto& page = traits::get(read);
        auto following = std::invoke(next, std::as_const(state), page);

        if (following and state == *following) {
            throw std::logic_error {"Datum paged traversal did not advance its state"};
        }

        const auto action = page_traversal_detail::visit(visitor, std::move(page));
        ++pages;

        if (!following) {
            return traversal_result {result_type {.resume = std::nullopt,
                                                  .pages = pages,
                                                  .outcome = page_traversal_outcome::complete}};
        }

        if (page_action::stop == action) {
            return traversal_result {result_type {.resume = std::move(*following),
                                                  .pages = pages,
                                                  .outcome = page_traversal_outcome::stopped}};
        }

        state = std::move(*following);
    }
}

} // ceph::salad::datum::traversal

#endif // CEPH_RGW_DATUM_TRAVERSAL_H
