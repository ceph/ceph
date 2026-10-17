// SPDX-License-Identifier: LGPL-2.1-or-later
#include "google_tcmalloc.h"
#include "tcmalloc/malloc_extension.h"
#include <cstdio>
#include <limits>

void ceph_tcmalloc_get_stats(char *buf, int length)
{
  if (length > 0) {
    auto stats = tcmalloc::MallocExtension::GetStats();
    std::snprintf(buf, length, "%s", stats.c_str());
  }
}

void ceph_tcmalloc_release_free_memory()
{
  tcmalloc::MallocExtension::ReleaseMemoryToSystem(
    std::numeric_limits<size_t>::max());
}

bool ceph_tcmalloc_get_numeric_property(const char *property, size_t *value)
{
  auto result = tcmalloc::MallocExtension::GetNumericProperty(property);
  if (!result) {
    return false;
  }
  *value = *result;
  return true;
}
