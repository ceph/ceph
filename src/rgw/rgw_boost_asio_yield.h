//
// copy of needed macors from yield.hpp
//
// Copyright (c) 2003-2013 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef RGW_BOOST_ASIO_YIELD_H
#define RGW_BOOST_ASIO_YIELD_H

#ifdef HAVE_BOOST_ASIO_COROUTINE
#include <boost/asio/yield.hpp>
#else
#define BOOST_ASIO_CORO_REENTER(c) \
  switch (::boost::asio::detail::coroutine_ref _coro_value = c) \
    case -1: if (_coro_value) \
    { \
      goto terminate_coroutine; \
      terminate_coroutine: \
      _coro_value = -1; \
      goto bail_out_of_coroutine; \
      bail_out_of_coroutine: \
      break; \
    } \
    else case 0:

#define BOOST_ASIO_CORO_YIELD_IMPL(n) \
  for (_coro_value = (n);;) \
    if (_coro_value == 0) \
    { \
      case (n): ; \
      break; \
    } \
    else \
      switch (_coro_value ? 0 : 1) \
        for (;;) \
          case -1: if (_coro_value) \
            goto terminate_coroutine; \
          else for (;;) \
            case 1: if (_coro_value) \
              goto bail_out_of_coroutine; \
            else case 0:

#define BOOST_ASIO_CORO_FORK_IMPL(n) \
  for (_coro_value = -(n);; _coro_value = (n)) \
    if (_coro_value == (n)) \
    { \
      case -(n): ; \
      break; \
    } \
    else

#if defined(_MSC_VER)
# define BOOST_ASIO_CORO_YIELD BOOST_ASIO_CORO_YIELD_IMPL(__COUNTER__ + 1)
# define BOOST_ASIO_CORO_FORK BOOST_ASIO_CORO_FORK_IMPL(__COUNTER__ + 1)
#else // defined(_MSC_VER)
# define BOOST_ASIO_CORO_YIELD BOOST_ASIO_CORO_YIELD_IMPL(__LINE__)
# define BOOST_ASIO_CORO_FORK BOOST_ASIO_CORO_FORK_IMPL(__LINE__)
#endif // defined(_MSC_VER)

#ifndef reenter
# define reenter(c) BOOST_ASIO_CORO_REENTER(c)
#endif

#ifndef yield
# define yield BOOST_ASIO_CORO_YIELD
#endif

#ifndef fork
# define fork BOOST_ASIO_CORO_FORK
#endif

#endif // HAVE_BOOST_ASIO_COROUTINE

#endif // RGW_BOOST_ASIO_YIELD_H

