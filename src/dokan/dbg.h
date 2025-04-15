// Various helpers used for debugging purposes, such as functions
// logging certain flags. Since those can be rather verbose, it's
// better if we keep them separate.

#ifndef CEPH_DOKAN_DBG_H
#define CEPH_DOKAN_DBG_H

#include "include/compat.h"

#include <sstream>

#include "ceph_dokan.h"

void print_credentials(
  std::ostringstream& Stream,
  PDOKAN_FILE_INFO DokanFileInfo);
void print_open_params(
  LPCSTR FilePath,
  ACCESS_MASK AccessMode,
  DWORD FlagsAndAttributes,
  ULONG ShareMode,
  DWORD CreationDisposition,
  ULONG CreateOptions,
  PDOKAN_FILE_INFO DokanFileInfo);

#endif // CEPH_DOKAN_DBG_H
