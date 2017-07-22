// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:t -*-
// vim: ts=8 sw=4 smarttab
/*
 * Copied from:
 * https://github.com/exclipy/inline_variant_visitor/blob/master/inline_variant.hpp
 */

#ifndef INLINE_VARIANT_H
#define INLINE_VARIANT_H

#include <boost/function_types/function_arity.hpp>
#include <boost/fusion/algorithm/transformation/transform.hpp>
#include <boost/mpl/contains.hpp>
#include <boost/mpl/map.hpp>
#include <boost/mpl/vector.hpp>
#include <boost/mpl/range_c.hpp>

#include "function_signature.h"

namespace detail {

// A metafunction class for getting the argument type from a unary function or functor type
struct function_arg_extractor
{
    // Function is either a function type like void(int const&), or a functor - eg. a class with void operator(int)
    // Sets type to the argument type with the constness and referenceness stripped (eg. int)
    template <typename Function>
    struct apply
    {
    private:
        typedef typename boost::remove_const< typename boost::remove_reference<Function>::type >::type bare_type;
        typedef typename signature_of<bare_type>::type normalized_function_type;
        typedef typename boost::function_types::function_arity<normalized_function_type>::type arity;
        typedef typename boost::function_types::parameter_types<normalized_function_type>::type parameter_types;
        typedef typename boost::function_types::result_type<normalized_function_type>::type result_type;

        BOOST_STATIC_ASSERT_MSG((arity::value == 1), "make_visitor called with a non-unary function");

        typedef typename boost::mpl::front<parameter_types>::type parameter_type;
    public:
        typedef typename boost::remove_const< typename boost::remove_reference<parameter_type>::type >::type type;
    };
};

struct make_pair
{
    template <typename AType, typename Ind>
    struct apply {
	typedef boost::mpl::pair<AType, Ind> type;
    };
};

// A metafunction class that asserts the second argument is in Allowed, and returns void
template<typename Allowed>
struct check_in
{
    template <typename Type1, typename Type2>
    struct apply
    {
    private:
        BOOST_STATIC_ASSERT_MSG((boost::mpl::contains<Allowed, typename boost::mpl::first<Type2>::type>::value),
                "make_visitor called with spurious handler functions");
    public:
        typedef void type;
    };
};

template <typename Seq>
struct as_map
{
private:
    struct insert_helper {
	template <typename M, typename P>
	struct apply
	{
	    typedef typename boost::mpl::insert<
		M,
		P>::type type;
	};
    };
public:
    typedef typename boost::mpl::fold<Seq, boost::mpl::map0<>, insert_helper>::type type;
};

// A functor template suitable for passing into apply_visitor.  The constructor accepts the list of handler functions,
// which are then exposed through a set of operator()s
template <typename Result, typename Variant, typename... Functions>
struct generic_visitor : boost::static_visitor<Result>, boost::noncopyable
{
private:
    typedef generic_visitor<Result, Variant, Functions...> type;

    // Compute the function_map type
    typedef boost::mpl::vector<Functions...> function_types;
    typedef typename boost::mpl::transform<function_types, function_arg_extractor>::type arg_types;
    typedef typename boost::mpl::transform<
        arg_types,
	boost::mpl::range_c<int, 0, boost::mpl::size<arg_types>::value>,
	make_pair
	>::type pair_list;
    typedef typename as_map<pair_list>::type fmap;

    // Check that the argument types are unique
    BOOST_STATIC_ASSERT_MSG((boost::mpl::size<fmap>::value == boost::mpl::size<arg_types>::value),
            "make_visitor called with non-unique argument types for handler functions");

    // Check that there aren't any argument types not in the variant types
    typedef typename boost::mpl::fold<fmap, void, check_in<typename Variant::types> >::type dummy;

    boost::fusion::vector<Functions...> fvec;


    template <typename T>
    Result apply_helper(const T& object, boost::mpl::true_) const {
	typedef typename boost::mpl::at<fmap, T>::type Ind;
        return boost::fusion::at<Ind>(fvec)(object);
    }

    template <typename T>
    Result apply_helper(const T& object, boost::mpl::false_) const {
        return Result();
    }

    BOOST_MOVABLE_BUT_NOT_COPYABLE(generic_visitor)

public:
    generic_visitor(BOOST_RV_REF(type) other)
    :
        fvec(boost::move(other.fvec))
    {
    }
    generic_visitor(Functions&&... functions)
    :
        fvec(std::forward<Functions>(functions)...)
    {
    }

    template <typename T>
    Result operator()(const T& object) const {
        typedef typename boost::mpl::has_key<fmap, T>::type correct_key;
        BOOST_STATIC_ASSERT_MSG(correct_key::value,
            "make_visitor called without specifying handlers for all required types");
        return apply_helper(object, correct_key());
    }
};

// A metafunction class for getting the return type of a function
struct function_return_extractor
{
    template <typename Function>
    struct apply : boost::function_types::result_type<typename signature_of<Function>::type>
    {
    };
};

// A metafunction class that asserts the two arguments are the same and returns the first one
struct check_same
{
    template <typename Type1, typename Type2>
    struct apply
    {
    private:
        BOOST_STATIC_ASSERT_MSG((boost::is_same<Type1, Type2>::value),
                "make_visitor called with functions of differing return types");
    public:
        typedef Type1 type;
    };
};

// A metafunction for getting the required generic_visitor type for the set of Functions
template <typename Variant, typename... Functions>
struct get_generic_visitor
{
private:
    typedef boost::mpl::vector<Functions...> function_types;
    typedef typename boost::mpl::transform<
        function_types,
        boost::remove_const< boost::remove_reference<boost::mpl::_1> >
    >::type bare_function_types;
    typedef typename boost::mpl::transform<bare_function_types, function_return_extractor>::type return_types;

public:
    // Set result_type to the return type of the first function
    typedef typename boost::mpl::front<return_types>::type result_type;
    typedef generic_visitor<result_type, Variant, Functions...> type;

private:
    // Assert that every return type is the same as the first one
    typedef typename boost::mpl::fold<return_types, result_type, check_same>::type dummy;
};

// Accepts a set of functions and returns an object suitable for apply_visitor
template <typename Variant, typename... Functions>
auto make_visitor(BOOST_RV_REF(Functions)... functions)
    -> typename detail::get_generic_visitor<Variant, Functions...>::type
{
    return typename detail::get_generic_visitor<Variant, Functions...>::type(boost::forward<Functions>(functions)...);
}

}

template <typename Variant, typename... Functions>
auto match(Variant const& variant, BOOST_RV_REF(Functions)... functions)
    -> typename detail::get_generic_visitor<Variant, Functions...>::result_type
{
    return boost::apply_visitor(detail::make_visitor<Variant>(
        boost::forward<Functions>(functions)...), variant);
}

#endif
