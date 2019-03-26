// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>

class object_not_found : public std::exception {
};

class object_corrupted : public std::exception {
};

