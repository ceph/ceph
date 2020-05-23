// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_B64_H
#define RGW_B64_H

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/insert_linebreaks.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/archive/iterators/remove_whitespace.hpp>
#include <limits>
#include <string_view>

namespace rgw {

  /*
   * A header-only Base64 encoder built on boost::archive.  The
   * formula is based on a class poposed for inclusion in boost in
   * 2011 by Denis Shevchenko (abandoned), updated slightly
   * (e.g., uses std::string_view).
   *
   * Also, wrap_width added as template argument, based on
   * feedback from Marcus.
   */

  template<int wrap_width = std::numeric_limits<int>::max()>
  inline std::string to_base64(std::string_view sview)
  {
    using namespace boost::archive::iterators;

    // output must be =padded modulo 3
    auto psize = sview.size();
    while ((psize % 3) != 0) {
      ++psize;
    }

    /* RFC 2045 requires linebreaks to be present in the output
     * sequence every at-most 76 characters (MIME-compliance),
     * but we could likely omit it. */
    typedef
      insert_linebreaks<
        base64_from_binary<
          transform_width<
	    std::string_view::const_iterator
            ,6,8>
          >
          ,wrap_width
        > b64_iter;

    std::string outstr(b64_iter(sview.data()),
		       b64_iter(sview.data() + sview.size()));

    // pad outstr with '=' to a length that is a multiple of 3
    for (size_t ix = 0; ix < (psize-sview.size()); ++ix)
      outstr.push_back('=');

    return outstr;
  }

  inline std::string from_base64(std::string_view sview)
  {
    using namespace boost::archive::iterators;
    if (sview.empty())
      return std::string();
    /* MIME-compliant input will have line-breaks, so we have to
     * filter WS */
    typedef
      transform_width<
      binary_from_base64<
	remove_whitespace<
	  std::string_view::const_iterator>>
      ,8,6
      > b64_iter;

    while (sview.back() == '=')
      sview.remove_suffix(1);

    std::string outstr(b64_iter(sview.data()),
		      b64_iter(sview.data() + sview.size()));

    return outstr;
  }
} /* namespace */

#endif /* RGW_B64_H */
