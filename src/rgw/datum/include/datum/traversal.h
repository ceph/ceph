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

template <typename ReaderT, typename StateT>
using read_result = std::invoke_result_t<ReaderT&, const StateT&>;

template <typename ReaderT, typename StateT>
concept page_reader =
    std::invocable<ReaderT&, const StateT&> and
    std::same_as<read_result<ReaderT, StateT>, std::remove_cvref_t<read_result<ReaderT, StateT>>> and
    (!std::is_void_v<typename detail::result_traits<read_result<ReaderT, StateT>>::value_type>);

template <typename ReaderT, typename StateT>
using page_type = typename detail::result_traits<read_result<ReaderT, StateT>>::value_type;

template <typename NextT, typename StateT, typename PageT>
concept next_page =
    std::invocable<NextT&, const StateT&, PageT&> and
    std::same_as<std::invoke_result_t<NextT&, const StateT&, PageT&>, std::optional<StateT>>;

template <typename VisitorT, typename PageT>
using visit_result = std::invoke_result_t<VisitorT&, PageT&&>;

template <typename VisitorT, typename PageT>
concept page_visitor = std::invocable<VisitorT&, PageT&&> and
    std::same_as<visit_result<VisitorT, PageT>, std::remove_cvref_t<visit_result<VisitorT, PageT>>> and
    (std::is_void_v<typename detail::result_traits<visit_result<VisitorT, PageT>>::value_type> ||
     std::same_as<typename detail::result_traits<visit_result<VisitorT, PageT>>::value_type, page_action>);

template <typename VisitorT, typename PageT>
requires page_visitor<VisitorT, PageT>
constexpr auto visit(VisitorT& visitor, PageT&& page)
{
    using response_type = visit_result<VisitorT, PageT>;
    using traits = detail::result_traits<response_type>;
    using error_type = typename traits::error_type;
    using result_type = std::conditional_t<std::is_void_v<error_type>, page_action,
                                           std::expected<page_action, error_type>>;

    if constexpr (std::is_void_v<response_type>) {
        std::invoke(visitor, std::forward<PageT>(page));

        return page_action::next;
    }

    if constexpr (!std::is_void_v<response_type>) {
        auto response = std::invoke(visitor, std::forward<PageT>(page));
        if constexpr (!std::is_void_v<error_type>) {
            if (!response) {
                return result_type {std::unexpected(std::move(response.error()))};
            }
        }
        if constexpr (std::is_void_v<typename traits::value_type>) {
            return result_type {page_action::next};
        }

        if constexpr (!std::is_void_v<typename traits::value_type>) {
            return result_type {detail::result_value(response)};
        }
    }
}

} // page_traversal_detail

// Visit fallible, cursor-driven pages without hiding I/O in iterator syntax.
// A nonterminal page must produce a different next state. "next" runs before
// the visitor, so it may move continuation data from the page into the new
// state. A stopped traversal returns the state; a read failure reports it with
// prior progress. Readers that throw return the traversal result directly and
// propagate exceptions. Expected reader/visitor errors retain the current page's
// input state and completed page count; a failing visitor does not complete that
// page. Callback effects are not rolled back. Fallible readers and visitors use
// the same error type when both are present.
template <typename StateT, typename ReaderT, typename NextT, typename VisitorT>
requires std::move_constructible<StateT> and std::assignable_from<StateT&, StateT> and
         std::equality_comparable<StateT> and page_traversal_detail::page_reader<ReaderT, StateT> and
         page_traversal_detail::next_page<NextT, StateT,
                                          page_traversal_detail::page_type<ReaderT, StateT>> and
         page_traversal_detail::page_visitor<VisitorT,
                                             page_traversal_detail::page_type<ReaderT, StateT>> and
         detail::compatible_result_errors<page_traversal_detail::read_result<ReaderT, StateT>,
             page_traversal_detail::visit_result<
                 VisitorT, page_traversal_detail::page_type<ReaderT, StateT>>>
[[nodiscard]] auto for_each_page(StateT state, ReaderT&& fetch_page, NextT&& next,
                                 VisitorT&& visitor, const page_traversal_options options = {})
{
    using read_type = page_traversal_detail::read_result<ReaderT, StateT>;
    using traits = detail::result_traits<read_type>;
    using visitor_type = page_traversal_detail::visit_result<
        VisitorT, page_traversal_detail::page_type<ReaderT, StateT>>;
    using visitor_traits = detail::result_traits<visitor_type>;
    using error_type = std::conditional_t<std::is_void_v<typename traits::error_type>,
                                         typename visitor_traits::error_type, typename traits::error_type>;
    using result_type = page_traversal_result<StateT>;
    using traversal_result = std::conditional_t<std::is_void_v<error_type>, result_type,
        std::expected<result_type, page_traversal_failure<StateT, error_type>>>;

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

        if constexpr (!std::is_void_v<typename traits::error_type>) {
            if (!read) {
                return traversal_result {
                            std::unexpected(page_traversal_failure<StateT, typename traits::error_type> {
                                .resume = std::move(state),
                                .error = std::move(read.error()),
                                .pages = pages})
                        };
            }
        }

        auto& page = detail::result_value(read);
        auto following = std::invoke(next, std::as_const(state), page);

        if (following and state == *following) {
            throw std::logic_error {"Datum paged traversal did not advance its state"};
        }

        auto response = page_traversal_detail::visit(visitor, std::move(page));
        if constexpr (!std::is_void_v<typename visitor_traits::error_type>) {
            if (!response) {
                return traversal_result {std::unexpected(page_traversal_failure<StateT, error_type> {
                    .resume = std::move(state), .error = std::move(response.error()), .pages = pages})};
            }
        }
        const auto action = detail::result_value(response);
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
