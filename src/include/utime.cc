// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "utime.h"
#include "common/Formatter.h"
#include "common/strtol.h"
#include "include/timegm.h"

#include <iomanip> // for std::setw()

#include <errno.h>

void utime_t::dump(ceph::Formatter *f) const
{
  f->dump_int("seconds", tv.tv_sec);
  f->dump_int("nanoseconds", tv.tv_nsec);
}

void utime_t::generate_test_instances(std::list<utime_t*>& o)
{
  o.push_back(new utime_t());
  o.push_back(new utime_t());
  o.back()->tv.tv_sec = static_cast<__u32>((1L << 32) - 1);
  o.push_back(new utime_t());
  o.back()->tv.tv_nsec = static_cast<__u32>((1L << 32) - 1);
}

std::ostream& utime_t::gmtime(std::ostream& out, bool legacy_form) const {
  out.setf(std::ios::right);
  char oldfill = out.fill();
  out.fill('0');
  if (sec() < ((time_t)(60*60*24*365*10))) {
    // raw seconds.  this looks like a relative time.
    out << (long)sec() << "." << std::setw(6) << usec();
  } else {
    // this looks like an absolute time.
    //  conform to http://en.wikipedia.org/wiki/ISO_8601
    struct tm bdt;
    time_t tt = sec();
    gmtime_r(&tt, &bdt);
    out << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
	<< '-' << std::setw(2) << (bdt.tm_mon+1)
	<< '-' << std::setw(2) << bdt.tm_mday;
    if (legacy_form) {
      out << ' ';
    } else {
      out << 'T';
    }
    out << std::setw(2) << bdt.tm_hour
	<< ':' << std::setw(2) << bdt.tm_min
	<< ':' << std::setw(2) << bdt.tm_sec;
    out << "." << std::setw(6) << usec();
    out << "Z";
  }
  out.fill(oldfill);
  out.unsetf(std::ios::right);
  return out;
}

std::ostream& utime_t::gmtime_nsec(std::ostream& out) const {
  out.setf(std::ios::right);
  char oldfill = out.fill();
  out.fill('0');
  if (sec() < ((time_t)(60*60*24*365*10))) {
    // raw seconds.  this looks like a relative time.
    out << (long)sec() << "." << std::setw(6) << usec();
  } else {
    // this looks like an absolute time.
    //  conform to http://en.wikipedia.org/wiki/ISO_8601
    struct tm bdt;
    time_t tt = sec();
    gmtime_r(&tt, &bdt);
    out << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
	<< '-' << std::setw(2) << (bdt.tm_mon+1)
	<< '-' << std::setw(2) << bdt.tm_mday
	<< 'T'
	<< std::setw(2) << bdt.tm_hour
	<< ':' << std::setw(2) << bdt.tm_min
	<< ':' << std::setw(2) << bdt.tm_sec;
    out << "." << std::setw(9) << nsec();
    out << "Z";
  }
  out.fill(oldfill);
  out.unsetf(std::ios::right);
  return out;
}

std::ostream& utime_t::asctime(std::ostream& out) const {
  out.setf(std::ios::right);
  char oldfill = out.fill();
  out.fill('0');
  if (sec() < ((time_t)(60*60*24*365*10))) {
    // raw seconds.  this looks like a relative time.
    out << (long)sec() << "." << std::setw(6) << usec();
  } else {
    // this looks like an absolute time.
    struct tm bdt;
    time_t tt = sec();
    gmtime_r(&tt, &bdt);

    char buf[128];
    asctime_r(&bdt, buf);
    int len = strlen(buf);
    if (buf[len - 1] == '\n')
      buf[len - 1] = '\0';
    out << buf;
  }
  out.fill(oldfill);
  out.unsetf(std::ios::right);
  return out;
}

std::ostream& utime_t::localtime(std::ostream& out, bool legacy_form) const {
  out.setf(std::ios::right);
  char oldfill = out.fill();
  out.fill('0');
  if (sec() < ((time_t)(60*60*24*365*10))) {
    // raw seconds.  this looks like a relative time.
    out << (long)sec() << "." << std::setw(6) << usec();
  } else {
    // this looks like an absolute time.
    //  conform to http://en.wikipedia.org/wiki/ISO_8601
    struct tm bdt;
    time_t tt = sec();
    localtime_r(&tt, &bdt);
    out << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
    << '-' << std::setw(2) << (bdt.tm_mon+1)
    << '-' << std::setw(2) << bdt.tm_mday;
    if (legacy_form) {
      out << ' ';
    } else {
      out << 'T';
    }
    out << std::setw(2) << bdt.tm_hour
    << ':' << std::setw(2) << bdt.tm_min
    << ':' << std::setw(2) << bdt.tm_sec;
    out << "." << std::setw(6) << usec();
    if (!legacy_form) {
      char buf[32] = { 0 };
      strftime(buf, sizeof(buf), "%z", &bdt);
      out << buf;
    }
  }
  out.fill(oldfill);
  out.unsetf(std::ios::right);
  return out;
}

int utime_t::parse_date(const std::string& date, uint64_t *epoch, uint64_t *nsec,
                        std::string *out_date,
			std::string *out_time) {
  struct tm tm;
  memset(&tm, 0, sizeof(tm));

  if (nsec)
    *nsec = 0;

  const char *p = strptime(date.c_str(), "%Y-%m-%d", &tm);
  if (p) {
    if (*p == ' ' || *p == 'T') {
      p++;
      // strptime doesn't understand fractional/decimal seconds, and
      // it also only takes format chars or literals, so we have to
      // get creative.
      char fmt[32] = {0};
      strncpy(fmt, p, sizeof(fmt) - 1);
      fmt[0] = '%';
      fmt[1] = 'H';
      fmt[2] = ':';
      fmt[3] = '%';
      fmt[4] = 'M';
      fmt[6] = '%';
      fmt[7] = 'S';
      const char *subsec = 0;
      char *q = fmt + 8;
      if (*q == '.') {
	++q;
	subsec = p + 9;
	q = fmt + 9;
	while (*q && isdigit(*q)) {
	  ++q;
	}
      }
      // look for tz...
      if (*q == '-' || *q == '+') {
	*q = '%';
	*(q+1) = 'z';
	*(q+2) = 0;
      }
      p = strptime(p, fmt, &tm);
      if (!p) {
	return -EINVAL;
      }
      if (nsec && subsec) {
	unsigned i;
	char buf[10]; /* 9 digit + null termination */
	for (i = 0; (i < sizeof(buf) - 1) && isdigit(*subsec); ++i, ++subsec) {
	  buf[i] = *subsec;
	}
	for (; i < sizeof(buf) - 1; ++i) {
	  buf[i] = '0';
	}
	buf[i] = '\0';
	std::string err;
	*nsec = (uint64_t)strict_strtol(buf, 10, &err);
	if (!err.empty()) {
	  return -EINVAL;
	}
      }
    }
  } else {
    int sec, usec;
    int r = sscanf(date.c_str(), "%d.%d", &sec, &usec);
    if (r != 2) {
      return -EINVAL;
    }

    time_t tt = sec;
    gmtime_r(&tt, &tm);

    if (nsec) {
      *nsec = (uint64_t)usec * 1000;
    }
  }

  #ifndef _WIN32
  // apply the tm_gmtoff manually below, since none of mktime,
  // gmtime, and localtime seem to do it.  zero it out here just in
  // case some other libc *does* apply it.  :(
  auto gmtoff = tm.tm_gmtoff;
  tm.tm_gmtoff = 0;
  #else
  auto gmtoff = _timezone;
  #endif /* _WIN32 */

  time_t t = internal_timegm(&tm);
  if (epoch)
    *epoch = (uint64_t)t;

  *epoch -= gmtoff;

  if (out_date) {
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d", &tm);
    *out_date = buf;
  }
  if (out_time) {
    char buf[32];
    strftime(buf, sizeof(buf), "%H:%M:%S", &tm);
    *out_time = buf;
  }

  return 0;
}
