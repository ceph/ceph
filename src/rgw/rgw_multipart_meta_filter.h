// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include <string>

extern const std::string MP_META_SUFFIX;

/**
 * A filter to a) test whether an object name is a multipart meta
 * object, and b) filter out just the key used to determine the bucket
 * index shard.
 *
 * Objects for multipart meta have names adorned with an upload id and
 * other elements -- specifically a ".", MULTIPART_UPLOAD_ID_PREFIX,
 * unique id, and MP_META_SUFFIX. This filter will return true when
 * the name provided is such. It will also extract the key used for
 * bucket index shard calculation from the adorned name.
 */
/**
 * @param name [in] The object name as it appears in the bucket index.
 * @param key [out] An output parameter that will contain the bucket
 *        index key if this entry is in the form of a multipart meta object.
 * @return true if the name provided is in the form of a multipart meta
 *         object, false otherwise
 */
bool MultipartMetaFilter(const std::string& name, std::string& key);
