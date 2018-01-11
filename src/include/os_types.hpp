// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
// no header!
#ifndef OS_TYPES_HPP
#define OS_TYPES_HPP
enum class os_types {linux, freebsd};

#ifdef __linux__
constexpr os_types os = os_types::linux;
#elif __APPLE__ || __FreeBSD__
constexpr os_types os = os_types::freebsd;
#endif
#endif // OS_TYPES_HPP
