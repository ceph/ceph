// Datum: Temporary type facts and borrowed standard-tuple traversal for C++23.
#ifndef CEPH_RGW_DATUM_COMPAT_DETAIL_RENDER_H
 #define CEPH_RGW_DATUM_COMPAT_DETAIL_RENDER_H

#include <functional>
#include <string_view>
#include <tuple>
#include <type_traits>

namespace ceph::salad::datum::compat::detail {

// Automatic record labels need reflection; C++23 schemas provide their name explicitly.
template <typename T>
consteval std::string_view implicit_record_name() = delete;

template <typename T, template <typename ...> typename TemplateT>
struct template_instance : std::false_type
{};

template <template <typename ...> typename TemplateT, typename ...ArgumentT>
struct template_instance<TemplateT<ArgumentT...>, TemplateT> : std::true_type
{};

template <typename T, template <typename ...> typename TemplateT>
inline constexpr bool specialization_of =
    template_instance<std::remove_cvref_t<T>, TemplateT>::value;

// std::get has no observation side effects for the exact tuple/pair vocabulary above.
// The comma fold sequences callbacks and borrows both the tuple and the operation.
template <typename TupleT, typename FunctionT>
constexpr void visit_display_tuple(const TupleT& values, FunctionT& function)
{
    std::apply([&function](const auto& ...value) {
        (std::invoke(function, value), ...);
    }, values);
}

template <typename T>
inline constexpr bool unsupported_display = false;

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_DETAIL_RENDER_H
