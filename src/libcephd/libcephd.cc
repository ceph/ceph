#include "common/version.h"
#include "include/cephd/libcephd.h"

extern "C" void cephd_version(int *pmajor, int *pminor, int *ppatch)
{
  if (pmajor)
    *pmajor = LIBCEPHD_VER_MAJOR;
  if (pminor)
    *pminor = LIBCEPHD_VER_MINOR;
  if (ppatch)
    *ppatch = LIBCEPHD_VER_PATCH;
}

extern "C" const char *ceph_version(int *pmajor, int *pminor, int *ppatch)
{
  int major, minor, patch;
  const char *v = ceph_version_to_str();

  int n = sscanf(v, "%d.%d.%d", &major, &minor, &patch);
  if (pmajor)
    *pmajor = (n >= 1) ? major : 0;
  if (pminor)
    *pminor = (n >= 2) ? minor : 0;
  if (ppatch)
    *ppatch = (n >= 3) ? patch : 0;
  return v;
}
