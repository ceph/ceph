// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iomanip>
#include <sstream>

#include "iso_8601.h"
#include "include/timegm.h"

namespace ceph {
using std::chrono::duration_cast;
using std::chrono::nanoseconds;
using std::chrono::seconds;
using std::setw;
using std::size_t;
using std::stringstream;
using std::string;
using std::uint16_t;

using boost::none;
using boost::optional;
using std::string_view;

using ceph::real_clock;
using ceph::real_time;

using sriter = string_view::const_iterator;

namespace {
// This assumes a contiguous block of numbers in the correct order.
uint16_t digit(char c) {
  if (!(c >= '0' && c <= '9')) {
    throw std::invalid_argument("Not a digit.");
  }
  return static_cast<uint16_t>(c - '0');
}

optional<real_time> calculate(const tm& t, uint32_t n = 0) {
  ceph_assert(n < 1000000000);
  time_t tt = internal_timegm(&t);
  if (tt == static_cast<time_t>(-1)) {
    return none;
  }

  return boost::make_optional<real_time>(real_clock::from_time_t(tt)
                                         + nanoseconds(n));
}
}

optional<real_time> from_iso_8601(const string_view s,
				  const bool ws_terminates) noexcept {
  auto end = s.cend();
  auto read_digit = [end](sriter& c) mutable {
    if (c == end) {
      throw std::invalid_argument("End of input.");
    }
    auto f = digit(*c);
    ++c;
    return f;
  };

  auto read_digits = [&read_digit](sriter& c, std::size_t n) {
    auto v = 0ULL;
    for (auto i = 0U; i < n; ++i) {
      auto d = read_digit(c);
      v = (10ULL * v) + d;
    }
    return v;
  };
  auto partial_date = [end, ws_terminates](sriter& c) {
    return (c == end || (ws_terminates && std::isspace(*c)));
  };
  auto time_end = [end, ws_terminates](sriter& c) {
    return (c != end && *c == 'Z' &&
	    ((c + 1) == end ||
	     (ws_terminates && std::isspace(*(c + 1)))));
  };
  auto consume_delimiter = [end](sriter& c, char q) {
    if (c == end || *c != q) {
      throw std::invalid_argument("Expected delimiter not found.");
    } else {
      ++c;
    }
  };

  tm t = { 0, // tm_sec
	   0, // tm_min
	   0, // tm_hour
	   1, // tm_mday
	   0, // tm_mon
	   70, // tm_year
	   0, // tm_wday
	   0, // tm_yday
	   0, // tm_isdst
  };
  try {
    auto c = s.cbegin();
    {
      auto y = read_digits(c, 4);
      if (y < 1970) {
	return none;
      }
      t.tm_year = y - 1900;
    }
    if (partial_date(c)) {
      return calculate(t, 0);
    }

    consume_delimiter(c, '-');
    t.tm_mon = (read_digits(c, 2) - 1);
    if (partial_date(c)) {
      return calculate(t);
    }
    consume_delimiter(c, '-');
    t.tm_mday = read_digits(c, 2);
    if (partial_date(c)) {
      return calculate(t);
    }
    consume_delimiter(c, 'T');
    t.tm_hour = read_digits(c, 2);
    if (time_end(c)) {
      return calculate(t);
    }
    consume_delimiter(c, ':');
    t.tm_min = read_digits(c, 2);
    if (time_end(c)) {
      return calculate(t);
    }
    consume_delimiter(c, ':');
    t.tm_sec = read_digits(c, 2);
    if (time_end(c)) {
      return calculate(t);
    }
    consume_delimiter(c, '.');

    auto n = 0UL;
    auto multiplier = 100000000UL;
    for (auto i = 0U; i < 9U; ++i) {
      auto d = read_digit(c);
      n += d * multiplier;
      multiplier /= 10;
      if (time_end(c)) {
	return calculate(t, n);
      }
    }
  } catch (std::invalid_argument& e) {
    // fallthrough
  }
  return none;
}

string to_iso_8601(const real_time t,
		   const iso_8601_format f) noexcept {
  ceph_assert(f >= iso_8601_format::Y &&
	      f <= iso_8601_format::YMDhmsn);
  stringstream out(std::ios_base::out);

  auto sec = real_clock::to_time_t(t);
  auto nsec = duration_cast<nanoseconds>(t.time_since_epoch() %
					 seconds(1)).count();

  struct tm bt;
  gmtime_r(&sec, &bt);
  out.fill('0');

  out << 1900 + bt.tm_year;
  if (f == iso_8601_format::Y) {
    return out.str();
  }

  out << '-' << setw(2) << bt.tm_mon + 1;
  if (f == iso_8601_format::YM) {
    return out.str();
  }

  out << '-' << setw(2) << bt.tm_mday;
  if (f == iso_8601_format::YMD) {
    return out.str();
  }

  out << 'T' << setw(2) << bt.tm_hour;
  if (f == iso_8601_format::YMDh) {
    out << 'Z';
    return out.str();
  }

  out << ':' << setw(2) << bt.tm_min;
  if (f == iso_8601_format::YMDhm) {
    out << 'Z';
    return out.str();
  }

  out << ':' << setw(2) << bt.tm_sec;
  if (f == iso_8601_format::YMDhms) {
    out << 'Z';
    return out.str();
  }
  out << '.' << setw(9) << nsec << 'Z';
  return out.str();
}
}
