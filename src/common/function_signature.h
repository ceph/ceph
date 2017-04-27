// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copied from:
 * https://github.com/exclipy/inline_variant_visitor/blob/master/function_signature.hpp
 * which apparently copied it from
 * http://stackoverflow.com/questions/4771417/how-to-get-the-signature-of-a-c-bind-expression
 */

#ifndef FUNCTION_SIGNATURE_H
#define FUNCTION_SIGNATURE_H

#include <boost/mpl/pop_front.hpp>
#include <boost/mpl/push_front.hpp>
#include <boost/function_types/is_member_function_pointer.hpp>
#include <boost/function_types/function_type.hpp>
#include <boost/function_types/result_type.hpp>
#include <boost/function_types/parameter_types.hpp>

#include <boost/type_traits.hpp>

template <typename F>
struct signature_of_member
{
    typedef typename boost::function_types::result_type<F>::type result_type;
    typedef typename boost::function_types::parameter_types<F>::type parameter_types;
    typedef typename boost::mpl::pop_front<parameter_types>::type base;
    typedef typename boost::mpl::push_front<base, result_type>::type L;
    typedef typename boost::function_types::function_type<L>::type type;
};

template <typename F, bool is_class>
struct signature_of_impl
{
    typedef typename boost::function_types::function_type<F>::type type;
};

template <typename F>
struct signature_of_impl<F, true>
{
    typedef typename signature_of_member<decltype(&F::operator())>::type type;
};

template <typename F>
struct signature_of
{
    typedef typename signature_of_impl<F, boost::is_class<F>::value>::type type;
};

#endif
