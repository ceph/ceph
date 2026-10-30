// SPDX-License-Identifier: LGPL-2.1-or-later
#pragma once
#include <cstddef>

extern "C" {
void ceph_tcmalloc_get_stats(char *buf, int length);
void ceph_tcmalloc_release_free_memory();
bool ceph_tcmalloc_get_numeric_property(const char *property, size_t *value);
}
