/*
 * Copyright (C) 2021 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "ceph_dokan.h"
#include "utils.h"
#include "dbg.h"

#include "common/debug.h"
#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-wnbd: "

#define check_flag(stream, val, flag) if (val & flag) { stream << "[" #flag "]"; }
#define check_flag_eq(stream, val, flag) if (val == flag) { stream << "[" #flag "]"; }

using namespace std;

void print_credentials(ostringstream& Stream, PDOKAN_FILE_INFO DokanFileInfo)
{
  UCHAR buffer[1024];
  DWORD returnLength;
  CHAR accountName[256];
  CHAR domainName[256];
  DWORD accountLength = sizeof(accountName) / sizeof(WCHAR);
  DWORD domainLength = sizeof(domainName) / sizeof(WCHAR);
  SID_NAME_USE snu;

  int err = 0;
  HANDLE handle = DokanOpenRequestorToken(DokanFileInfo);
  if (handle == INVALID_HANDLE_VALUE) {
    err = GetLastError();
    derr << "DokanOpenRequestorToken failed. Error: " << err << dendl;
    return;
  }

  if (!GetTokenInformation(handle, TokenUser, buffer,
                           sizeof(buffer), &returnLength)) {
    err = GetLastError();
    derr << "GetTokenInformation failed. Error: " << err << dendl;
    CloseHandle(handle);
    return;
  }

  CloseHandle(handle);

  PTOKEN_USER tokenUser = (PTOKEN_USER)buffer;
  if (!LookupAccountSidA(NULL, tokenUser->User.Sid, accountName,
      &accountLength, domainName, &domainLength, &snu)) {
    err = GetLastError();
    derr << "LookupAccountSid failed. Error: " << err << dendl;
    return;
  }

  Stream << "\n\tAccountName: " << accountName << ", DomainName: " << domainName;
}

void print_open_params(
  LPCSTR FilePath,
  ACCESS_MASK AccessMode,
  DWORD FlagsAndAttributes,
  ULONG ShareMode,
  DWORD CreationDisposition,
  ULONG CreateOptions,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  ostringstream o;
  o << "CreateFile: " << FilePath << ". ";
  print_credentials(o, DokanFileInfo);

  o << "\n\tCreateDisposition: " << hex << CreationDisposition << " ";
  check_flag_eq(o, CreationDisposition, CREATE_NEW);
  check_flag_eq(o, CreationDisposition, OPEN_ALWAYS);
  check_flag_eq(o, CreationDisposition, CREATE_ALWAYS);
  check_flag_eq(o, CreationDisposition, OPEN_EXISTING);
  check_flag_eq(o, CreationDisposition, TRUNCATE_EXISTING);

  o << "\n\tShareMode: " << hex << ShareMode << " ";
  check_flag(o, ShareMode, FILE_SHARE_READ);
  check_flag(o, ShareMode, FILE_SHARE_WRITE);
  check_flag(o, ShareMode, FILE_SHARE_DELETE);

  o << "\n\tAccessMode: " << hex << AccessMode << " ";
  check_flag(o, AccessMode, GENERIC_READ);
  check_flag(o, AccessMode, GENERIC_WRITE);
  check_flag(o, AccessMode, GENERIC_EXECUTE);

  check_flag(o, AccessMode, WIN32_DELETE);
  check_flag(o, AccessMode, FILE_READ_DATA);
  check_flag(o, AccessMode, FILE_READ_ATTRIBUTES);
  check_flag(o, AccessMode, FILE_READ_EA);
  check_flag(o, AccessMode, READ_CONTROL);
  check_flag(o, AccessMode, FILE_WRITE_DATA);
  check_flag(o, AccessMode, FILE_WRITE_ATTRIBUTES);
  check_flag(o, AccessMode, FILE_WRITE_EA);
  check_flag(o, AccessMode, FILE_APPEND_DATA);
  check_flag(o, AccessMode, WRITE_DAC);
  check_flag(o, AccessMode, WRITE_OWNER);
  check_flag(o, AccessMode, SYNCHRONIZE);
  check_flag(o, AccessMode, FILE_EXECUTE);
  check_flag(o, AccessMode, STANDARD_RIGHTS_READ);
  check_flag(o, AccessMode, STANDARD_RIGHTS_WRITE);
  check_flag(o, AccessMode, STANDARD_RIGHTS_EXECUTE);

  o << "\n\tFlagsAndAttributes: " << hex << FlagsAndAttributes << " ";
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_ARCHIVE);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_ENCRYPTED);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_HIDDEN);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_NORMAL);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_NOT_CONTENT_INDEXED);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_OFFLINE);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_READONLY);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_SYSTEM);
  check_flag(o, FlagsAndAttributes, FILE_ATTRIBUTE_TEMPORARY);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_WRITE_THROUGH);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_OVERLAPPED);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_NO_BUFFERING);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_RANDOM_ACCESS);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_SEQUENTIAL_SCAN);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_DELETE_ON_CLOSE);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_BACKUP_SEMANTICS);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_POSIX_SEMANTICS);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_OPEN_REPARSE_POINT);
  check_flag(o, FlagsAndAttributes, FILE_FLAG_OPEN_NO_RECALL);
  check_flag(o, FlagsAndAttributes, SECURITY_ANONYMOUS);
  check_flag(o, FlagsAndAttributes, SECURITY_IDENTIFICATION);
  check_flag(o, FlagsAndAttributes, SECURITY_IMPERSONATION);
  check_flag(o, FlagsAndAttributes, SECURITY_DELEGATION);
  check_flag(o, FlagsAndAttributes, SECURITY_CONTEXT_TRACKING);
  check_flag(o, FlagsAndAttributes, SECURITY_EFFECTIVE_ONLY);
  check_flag(o, FlagsAndAttributes, SECURITY_SQOS_PRESENT);

  o << "\n\tIsDirectory: " << static_cast<bool>(DokanFileInfo->IsDirectory);

  o << "\n\tCreateOptions: " << hex << CreateOptions << " ";
  check_flag(o, CreateOptions, FILE_DIRECTORY_FILE);
  check_flag(o, CreateOptions, FILE_WRITE_THROUGH);
  check_flag(o, CreateOptions, FILE_SEQUENTIAL_ONLY);
  check_flag(o, CreateOptions, FILE_NO_INTERMEDIATE_BUFFERING);
  check_flag(o, CreateOptions, FILE_SYNCHRONOUS_IO_ALERT);
  check_flag(o, CreateOptions, FILE_SYNCHRONOUS_IO_NONALERT);
  check_flag(o, CreateOptions, FILE_NON_DIRECTORY_FILE);
  check_flag(o, CreateOptions, FILE_CREATE_TREE_CONNECTION);
  check_flag(o, CreateOptions, FILE_COMPLETE_IF_OPLOCKED);
  check_flag(o, CreateOptions, FILE_NO_EA_KNOWLEDGE);
  check_flag(o, CreateOptions, FILE_OPEN_REMOTE_INSTANCE);
  check_flag(o, CreateOptions, FILE_RANDOM_ACCESS);
  check_flag(o, CreateOptions, FILE_DELETE_ON_CLOSE);
  check_flag(o, CreateOptions, FILE_OPEN_BY_FILE_ID);
  check_flag(o, CreateOptions, FILE_OPEN_FOR_BACKUP_INTENT);
  check_flag(o, CreateOptions, FILE_NO_COMPRESSION);
  check_flag(o, CreateOptions, FILE_OPEN_REQUIRING_OPLOCK);
  check_flag(o, CreateOptions, FILE_DISALLOW_EXCLUSIVE);
  check_flag(o, CreateOptions, FILE_RESERVE_OPFILTER);
  check_flag(o, CreateOptions, FILE_OPEN_REPARSE_POINT);
  check_flag(o, CreateOptions, FILE_OPEN_NO_RECALL);
  check_flag(o, CreateOptions, FILE_OPEN_FOR_FREE_SPACE_QUERY);

  // We're using a high log level since this will only be enabled with the
  // explicit debug flag.
  dout(0) << o.str() << dendl;
}
