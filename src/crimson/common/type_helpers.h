// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "boost/intrusive_ptr.hpp"

template<typename T> using Ref = boost::intrusive_ptr<T>;
