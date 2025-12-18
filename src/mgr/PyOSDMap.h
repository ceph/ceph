// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <Python.h>

#include <string>

extern PyTypeObject BasePyOSDMapType;
extern PyTypeObject BasePyOSDMapIncrementalType;
extern PyTypeObject BasePyCRUSHType;

PyObject *construct_with_capsule(
    const std::string &module,
    const std::string &clsname,
    void *wrapped);

