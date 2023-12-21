/*
 * ceph-dokan - Win32 CephFS client based on Dokan
 *
 * Copyright (C) 2021 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#define UNICODE
#define _UNICODE

#include "include/compat.h"
#include "include/cephfs/libcephfs.h"

#include "ceph_dokan.h"

#include <algorithm>
#include <stdlib.h>
#include <fileinfo.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sddl.h>
#include <accctrl.h>
#include <aclapi.h>
#include <ntstatus.h>

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/version.h"
#include "common/win32/wstring.h"

#include "global/global_init.h"

#include "include/uuid.h"

#include "dbg.h"
#include "utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_client
#undef dout_prefix
#define dout_prefix *_dout << "ceph-dokan: "

using namespace std;

#define READ_ACCESS_REQUESTED(access_mode) \
    (access_mode & GENERIC_READ || \
     access_mode & FILE_SHARE_READ || \
     access_mode & STANDARD_RIGHTS_READ || \
     access_mode & FILE_SHARE_READ)
#define WRITE_ACCESS_REQUESTED(access_mode) \
    (access_mode & GENERIC_WRITE || \
     access_mode & FILE_SHARE_WRITE || \
     access_mode & STANDARD_RIGHTS_WRITE || \
     access_mode & FILE_SHARE_WRITE)

// TODO: check if those dokan limits still stand.
#define CEPH_DOKAN_MAX_FILE_SZ (1LL << 40) // 1TB
#define CEPH_DOKAN_MAX_IO_SZ (128 * 1024 * 1024) // 128MB

struct ceph_mount_info *cmount;
Config *g_cfg;

// Used as part of DOKAN_FILE_INFO.Context, must fit within 8B.
typedef struct {
  int   fd;
  short read_only;
} fd_context, *pfd_context;
static_assert(sizeof(fd_context) <= 8,
              "fd_context exceeds DOKAN_FILE_INFO.Context size.");

string get_path(LPCWSTR path_w, bool normalize_case=true) {
  string path = to_string(path_w);
  replace(path.begin(), path.end(), '\\', '/');

  if (normalize_case && !g_cfg->case_sensitive) {
    if (g_cfg->convert_to_uppercase) {
      std::transform(
        path.begin(), path.end(), path.begin(),
        [](unsigned char c){
          return std::toupper(c);
        });
    } else {
      std::transform(
        path.begin(), path.end(), path.begin(),
        [](unsigned char c){
          return std::tolower(c);
        });
    }
  }

  return path;
}

static NTSTATUS do_open_file(
  string path,
  int flags,
  mode_t mode,
  fd_context* fdc)
{
  dout(20) << __func__ << " " << path << dendl;
  int fd = ceph_open(cmount, path.c_str(), flags, mode);
  if (fd < 0) {
    dout(2) << __func__ << " " << path
            << ": ceph_open failed. Error: " << fd << dendl;
    return cephfs_errno_to_ntstatus_map(fd);
  }

  fdc->fd = fd;
  dout(20) << __func__ << " " << path << " - fd: " << fd << dendl;
  return 0;
}

static NTSTATUS WinCephCreateDirectory(
  LPCWSTR FileName,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  dout(20) << __func__ << " " << path << dendl;
  if (path == "/") {
    return 0;
  }

  int ret = ceph_mkdir(cmount, path.c_str(), g_cfg->dir_mode);
  if (ret < 0) {
    dout(2) << __func__ << " " << path
            << ": ceph_mkdir failed. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }
  return 0;
}

static NTSTATUS WinCephCreateFile(
  LPCWSTR FileName,
  PDOKAN_IO_SECURITY_CONTEXT SecurityContext,
  ACCESS_MASK DesiredAccess,
  ULONG FileAttributes,
  ULONG ShareMode,
  ULONG CreateDisposition,
  ULONG CreateOptions,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  // TODO: use ZwCreateFile args by default and avoid conversions.
  ACCESS_MASK AccessMode;
  DWORD FlagsAndAttributes, CreationDisposition;
  DokanMapKernelToUserCreateFileFlags(
    DesiredAccess, FileAttributes, CreateOptions, CreateDisposition,
    &AccessMode, &FlagsAndAttributes, &CreationDisposition);

  string path = get_path(FileName);
  dout(20) << __func__ << " " << path
           << ". CreationDisposition: " << CreationDisposition << dendl;

  if (g_cfg->debug) {
    print_open_params(
      path.c_str(), AccessMode, FlagsAndAttributes, ShareMode,
      CreationDisposition, CreateOptions, DokanFileInfo);
  }

  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  *fdc = { 0 };
  NTSTATUS st = 0;

  struct ceph_statx stbuf;
  unsigned int requested_attrs = CEPH_STATX_BASIC_STATS;
  int ret = ceph_statx(cmount, path.c_str(), &stbuf, requested_attrs, 0);
  if (!ret) { /* File Exists */
    if (S_ISREG(stbuf.stx_mode)) {
      dout(20) << __func__ << " " << path << ". File exists." << dendl;
      if (CreateOptions & FILE_DIRECTORY_FILE) {
        dout(2) << __func__ << " " << path << ". Not a directory." << dendl;
        return STATUS_NOT_A_DIRECTORY;
      }
      switch (CreationDisposition) {
      case CREATE_NEW:
        return STATUS_OBJECT_NAME_COLLISION;
      case TRUNCATE_EXISTING:
        // open O_TRUNC & return 0
        return do_open_file(path, O_CREAT | O_TRUNC | O_RDWR,
                            g_cfg->file_mode, fdc);
      case OPEN_ALWAYS:
        // open & return STATUS_OBJECT_NAME_COLLISION
        if (!WRITE_ACCESS_REQUESTED(AccessMode))
          fdc->read_only = 1;
        if ((st = do_open_file(path, fdc->read_only ? O_RDONLY : O_RDWR,
                               g_cfg->file_mode, fdc)))
          return st;
        return STATUS_OBJECT_NAME_COLLISION;
      case OPEN_EXISTING:
        // open & return 0
        if (!WRITE_ACCESS_REQUESTED(AccessMode))
          fdc->read_only = 1;
        if ((st = do_open_file(path, fdc->read_only ? O_RDONLY : O_RDWR,
                               g_cfg->file_mode, fdc)))
          return st;
        return 0;
      case CREATE_ALWAYS:
        // open O_TRUNC & return STATUS_OBJECT_NAME_COLLISION
        if ((st = do_open_file(path, O_CREAT | O_TRUNC | O_RDWR,
                               g_cfg->file_mode, fdc)))
          return st;
        return STATUS_OBJECT_NAME_COLLISION;
      }
    } else if (S_ISDIR(stbuf.stx_mode)) {
      dout(20) << __func__ << " " << path << ". Directory exists." << dendl;
      DokanFileInfo->IsDirectory = TRUE;
      if (CreateOptions & FILE_NON_DIRECTORY_FILE) {
        dout(2) << __func__ << " " << path << ". File is a directory." << dendl;
        return STATUS_FILE_IS_A_DIRECTORY;
      }

      switch (CreationDisposition) {
      case CREATE_NEW:
        return STATUS_OBJECT_NAME_COLLISION;
      case TRUNCATE_EXISTING:
        return 0;
      case OPEN_ALWAYS:
      case OPEN_EXISTING:
        return do_open_file(path, O_RDONLY, g_cfg->file_mode, fdc);
      case CREATE_ALWAYS:
        return STATUS_OBJECT_NAME_COLLISION;
      }
    } else {
      derr << __func__ << " " << path
             << ": Unsupported st_mode: " << stbuf.stx_mode << dendl;
      return STATUS_BAD_FILE_TYPE;
    }
  } else { // The file doens't exist.
    if (DokanFileInfo->IsDirectory) {
      // TODO: check create disposition.
      dout(20) << __func__ << " " << path << ". New directory." << dendl;
      if ((st = WinCephCreateDirectory(FileName, DokanFileInfo)))
        return st;
      // Dokan expects a file handle even when creating new directories.
      return do_open_file(path, O_RDONLY, g_cfg->file_mode, fdc);
    }
    dout(20) << __func__ << " " << path << ". New file." << dendl;
    switch (CreationDisposition) {
      case CREATE_NEW:
        // create & return 0
        return do_open_file(path, O_CREAT | O_RDWR | O_EXCL,
                            g_cfg->file_mode, fdc);
      case CREATE_ALWAYS:
        // create & return 0
        return do_open_file(path, O_CREAT | O_TRUNC | O_RDWR,
                            g_cfg->file_mode, fdc);
      case OPEN_ALWAYS:
        return do_open_file(path, O_CREAT | O_RDWR,
                            g_cfg->file_mode, fdc);
      case OPEN_EXISTING:
      case TRUNCATE_EXISTING:
        dout(2) << __func__ << " " << path << ": Not found." << dendl;
        return STATUS_OBJECT_NAME_NOT_FOUND;
      default:
        derr << __func__ << " " << path
             << ": Unsupported create disposition: "
             << CreationDisposition << dendl;
        return STATUS_INVALID_PARAMETER;
    }
  }

  // We shouldn't get here.
  derr << __func__ << ": unknown error while opening: " << path << dendl;
  return STATUS_INTERNAL_ERROR;
}

static void WinCephCloseFile(
  LPCWSTR FileName,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);

  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  if (!fdc) {
    derr << __func__ << ": missing context: " << path << dendl;
    return;
  }

  dout(20) << __func__ << " " << path << " fd: " << fdc->fd << dendl;
  int ret = ceph_close(cmount, fdc->fd);
  if (ret) {
    dout(2) << __func__ << " " << path
            << " failed. fd: " << fdc->fd
            << ". Error: " << ret << dendl;
  }

  DokanFileInfo->Context = 0;
}

static void WinCephCleanup(
  LPCWSTR FileName,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);

  if (!DokanFileInfo->Context) {
    dout(10) << __func__ << ": missing context: " << path << dendl;
    return;
  }

  if (DokanFileInfo->DeleteOnClose) {
    dout(20) << __func__ << " DeleteOnClose: " << path << dendl;
    if (DokanFileInfo->IsDirectory) {
      int ret = ceph_rmdir(cmount, path.c_str());
      if (ret)
        derr << __func__ << " " << path
             << ": ceph_rmdir failed. Error: " << ret << dendl;
    } else {
      int ret = ceph_unlink(cmount, path.c_str());
      if (ret != 0) {
        derr << __func__ << " " << path
             << ": ceph_unlink failed. Error: " << ret << dendl;
      }
    }
  }
}

static NTSTATUS WinCephReadFile(
  LPCWSTR FileName,
  LPVOID Buffer,
  DWORD BufferLength,
  LPDWORD ReadLength,
  LONGLONG Offset,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  if (!BufferLength) {
    *ReadLength = 0;
    return 0;
  }
  if (Offset < 0) {
    dout(2) << __func__ << " " << get_path(FileName)
            << ": Invalid offset: " << Offset << dendl;
    return STATUS_INVALID_PARAMETER;
  }
  if (Offset > CEPH_DOKAN_MAX_FILE_SZ ||
      BufferLength > CEPH_DOKAN_MAX_IO_SZ) {
    dout(2) << "File read too large: " << get_path(FileName)
            << ". Offset: " << Offset
            << ". Buffer length: " << BufferLength << dendl;
    return STATUS_FILE_TOO_LARGE;
  }

  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  if (!fdc->fd) {
    dout(15) << __func__ << " " << get_path(FileName)
             << ". Missing context, using temporary handle." << dendl;

    string path = get_path(FileName);
    int fd_new = ceph_open(cmount, path.c_str(), O_RDONLY, 0);
    if (fd_new < 0) {
      dout(2) << __func__ << " " << path
              << ": ceph_open failed. Error: " << fd_new << dendl;
      return cephfs_errno_to_ntstatus_map(fd_new);
    }

    int ret = ceph_read(cmount, fd_new, (char*) Buffer, BufferLength, Offset);
    if (ret < 0) {
      dout(2) << __func__ << " " << path
              << ": ceph_read failed. Error: " << ret
              << ". Offset: " << Offset
              << "Buffer length: " << BufferLength << dendl;
      ceph_close(cmount, fd_new);
      return cephfs_errno_to_ntstatus_map(ret);
    }
    *ReadLength = ret;
    ceph_close(cmount, fd_new);
    return 0;
  } else {
    int ret = ceph_read(cmount, fdc->fd, (char*) Buffer, BufferLength, Offset);
    if (ret < 0) {
      dout(2) << __func__ << " " << get_path(FileName)
              << ": ceph_read failed. Error: " << ret
              << ". Offset: " << Offset
              << "Buffer length: " << BufferLength << dendl;
      return cephfs_errno_to_ntstatus_map(ret);
    }
    *ReadLength = ret;
    return 0;
  }
}

static NTSTATUS WinCephWriteFile(
  LPCWSTR FileName,
  LPCVOID Buffer,
  DWORD NumberOfBytesToWrite,
  LPDWORD NumberOfBytesWritten,
  LONGLONG Offset,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  if (!NumberOfBytesToWrite) {
    *NumberOfBytesWritten = 0;
    return 0;
  }
  if (Offset < 0) {
    if (DokanFileInfo->WriteToEndOfFile) {
      string path = get_path(FileName);
      struct ceph_statx stbuf;
      unsigned int requested_attrs = CEPH_STATX_BASIC_STATS;

      int ret = ceph_statx(cmount, path.c_str(), &stbuf, requested_attrs, 0);
      if (ret) {
        dout(2) << __func__ << " " << path
                << ": ceph_statx failed. Error: " << ret << dendl;
        return cephfs_errno_to_ntstatus_map(ret);
      }

      Offset = stbuf.stx_size;
    } else {
      dout(2) << __func__ << " " << get_path(FileName)
            << ": Invalid offset: " << Offset << dendl;
      return STATUS_INVALID_PARAMETER;
    }
  }

  if (Offset > CEPH_DOKAN_MAX_FILE_SZ ||
      NumberOfBytesToWrite > CEPH_DOKAN_MAX_IO_SZ) {
    dout(2) << "File write too large: " << get_path(FileName)
            << ". Offset: " << Offset
            << ". Buffer length: " << NumberOfBytesToWrite
            << ". WriteToEndOfFile: " << (bool) DokanFileInfo->WriteToEndOfFile
            << dendl;
    return STATUS_FILE_TOO_LARGE;
  }
  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  if (fdc->read_only)
    return STATUS_ACCESS_DENIED;

  // TODO: check if we still have to support missing handles.
  // According to Dokan docs, it might be related to memory mapped files, in
  // which case reads/writes can be performed between the Close/Cleanup calls.
  if (!fdc->fd) {
    string path = get_path(FileName);
    dout(15) << __func__ << " " << path
             << ". Missing context, using temporary handle." << dendl;

    int fd_new = ceph_open(cmount, path.c_str(), O_RDWR, 0);
    if (fd_new < 0) {
      dout(2) << __func__ << " " << path
              << ": ceph_open failed. Error: " << fd_new << dendl;
      return cephfs_errno_to_ntstatus_map(fd_new);
    }

    int ret = ceph_write(cmount, fd_new, (char*) Buffer,
                         NumberOfBytesToWrite, Offset);
    if (ret < 0) {
      dout(2) << __func__ << " " << path
              << ": ceph_write failed. Error: " << ret
              << ". Offset: " << Offset
              << "Buffer length: " << NumberOfBytesToWrite << dendl;
      ceph_close(cmount, fd_new);
      return cephfs_errno_to_ntstatus_map(ret);
    }
    *NumberOfBytesWritten = ret;
    ceph_close(cmount, fd_new);
    return 0;
  } else {
    int ret = ceph_write(cmount, fdc->fd, (char*) Buffer,
                         NumberOfBytesToWrite, Offset);
    if (ret < 0) {
      dout(2) << __func__ << " " << get_path(FileName)
              << ": ceph_write failed. Error: " << ret
              << ". Offset: " << Offset
              << "Buffer length: " << NumberOfBytesToWrite << dendl;
      return cephfs_errno_to_ntstatus_map(ret);
    }
    *NumberOfBytesWritten = ret;
    return 0;
  }
}

static NTSTATUS WinCephFlushFileBuffers(
  LPCWSTR FileName,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  if (!fdc->fd) {
    derr << __func__ << ": missing context: " << get_path(FileName) << dendl;
    return STATUS_INVALID_HANDLE;
  }

  int ret = ceph_fsync(cmount, fdc->fd, 0);
  if (ret) {
    dout(2) << __func__ << " " << get_path(FileName)
            << ": ceph_sync failed. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }
  return 0;
}

static NTSTATUS WinCephGetFileInformation(
  LPCWSTR FileName,
  LPBY_HANDLE_FILE_INFORMATION HandleFileInformation,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  dout(20) << __func__ << " " << path << dendl;

  memset(HandleFileInformation, 0, sizeof(BY_HANDLE_FILE_INFORMATION));

  struct ceph_statx stbuf;
  unsigned int requested_attrs = CEPH_STATX_BASIC_STATS;
  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  if (!fdc->fd) {
    int ret = ceph_statx(cmount, path.c_str(), &stbuf, requested_attrs, 0);
    if (ret) {
      dout(2) << __func__ << " " << path
              << ": ceph_statx failed. Error: " << ret << dendl;
      return cephfs_errno_to_ntstatus_map(ret);
    }
  } else {
    int ret = ceph_fstatx(cmount, fdc->fd, &stbuf, requested_attrs, 0);
    if (ret) {
      dout(2) << __func__ << " " << path
              << ": ceph_fstatx failed. Error: " << ret << dendl;
      return cephfs_errno_to_ntstatus_map(ret);
    }
  }

  HandleFileInformation->nFileSizeLow = (stbuf.stx_size << 32) >> 32;
  HandleFileInformation->nFileSizeHigh = stbuf.stx_size >> 32;

  to_filetime(stbuf.stx_ctime.tv_sec, &HandleFileInformation->ftCreationTime);
  to_filetime(stbuf.stx_atime.tv_sec, &HandleFileInformation->ftLastAccessTime);
  to_filetime(stbuf.stx_mtime.tv_sec, &HandleFileInformation->ftLastWriteTime);

  if (S_ISDIR(stbuf.stx_mode)) {
    HandleFileInformation->dwFileAttributes |= FILE_ATTRIBUTE_DIRECTORY;
  } else if (S_ISREG(stbuf.stx_mode)) {
    HandleFileInformation->dwFileAttributes |= FILE_ATTRIBUTE_NORMAL;
  }

  HandleFileInformation->nFileIndexLow = (stbuf.stx_ino << 32) >> 32;
  HandleFileInformation->nFileIndexHigh = stbuf.stx_ino >> 32;

  HandleFileInformation->nNumberOfLinks = stbuf.stx_nlink;
  return 0;
}

static NTSTATUS WinCephFindFiles(
  LPCWSTR FileName,
  PFillFindData FillFindData, // function pointer
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  dout(20) << __func__ << " " << path << dendl;

  struct ceph_dir_result *dirp;
  int ret = ceph_opendir(cmount, path.c_str(), &dirp);
  if (ret != 0) {
    dout(2) << __func__ << " " << path
            << ": ceph_mkdir failed. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }

  // TODO: retrieve the original case (e.g. using xattr) if configured
  // to do so.
  // TODO: provide aliases when case insensitive mounts cause collisions.
  // For example, when having test.txt and Test.txt, the latter becomes
  // TEST~1.txt
  WIN32_FIND_DATAW findData;
  int count = 0;
  while (1) {
    memset(&findData, 0, sizeof(findData));
    struct dirent result;
    struct ceph_statx stbuf;

    unsigned int requested_attrs = CEPH_STATX_BASIC_STATS;
    ret = ceph_readdirplus_r(cmount, dirp, &result, &stbuf,
                             requested_attrs,
                             0,     // no special flags used when filling attrs
                             NULL); // we're not using inodes.
    if (!ret)
      break;
    if (ret < 0) {
      dout(2) << __func__ << " " << path
              << ": ceph_readdirplus_r failed. Error: " << ret << dendl;
      return cephfs_errno_to_ntstatus_map(ret);
    }

    to_wstring(result.d_name).copy(findData.cFileName, MAX_PATH);

    findData.nFileSizeLow = (stbuf.stx_size << 32) >> 32;
    findData.nFileSizeHigh = stbuf.stx_size >> 32;

    to_filetime(stbuf.stx_ctime.tv_sec, &findData.ftCreationTime);
    to_filetime(stbuf.stx_atime.tv_sec, &findData.ftLastAccessTime);
    to_filetime(stbuf.stx_mtime.tv_sec, &findData.ftLastWriteTime);

    if (S_ISDIR(stbuf.stx_mode)) {
      findData.dwFileAttributes |= FILE_ATTRIBUTE_DIRECTORY;
    } else if (S_ISREG(stbuf.stx_mode)) {
      findData.dwFileAttributes |= FILE_ATTRIBUTE_NORMAL;
    }

    FillFindData(&findData, DokanFileInfo);
    count++;
  }

  ceph_closedir(cmount, dirp);

  dout(20) << __func__ << " " << path
           << " found " << count << " entries." << dendl;
  return 0;
}

/**
 * This callback is only supposed to check if deleting a file is
 * allowed. The actual file deletion will be performed by WinCephCleanup
 */
static NTSTATUS WinCephDeleteFile(
  LPCWSTR FileName,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  dout(20) << __func__ << " " << path << dendl;

  if (ceph_may_delete(cmount, path.c_str()) < 0) {
    return STATUS_ACCESS_DENIED;
  }

  return 0;
}

static NTSTATUS WinCephDeleteDirectory(
  LPCWSTR FileName,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  dout(20) << __func__ << " " << path << dendl;

  if (ceph_may_delete(cmount, path.c_str()) < 0) {
    return STATUS_ACCESS_DENIED;
  }

  struct ceph_dir_result *dirp;
  int ret = ceph_opendir(cmount, path.c_str(), &dirp);
  if (ret != 0) {
    dout(2) << __func__ << " " << path
            << ": ceph_opendir failed. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }

  WIN32_FIND_DATAW findData;
  while (1) {
    memset(&findData, 0, sizeof(findData));
    struct dirent *result = ceph_readdir(cmount, dirp);
    if (result) {
      if (strcmp(result->d_name, ".") && strcmp(result->d_name, "..")) {
        ceph_closedir(cmount, dirp);
        dout(2) << __func__ << " " << path
                << ": directory is not empty. " << dendl;
        return STATUS_DIRECTORY_NOT_EMPTY;
      }
    } else break;
  }

  ceph_closedir(cmount, dirp);
  return 0;
}

static NTSTATUS WinCephMoveFile(
  LPCWSTR FileName, // existing file name
  LPCWSTR NewFileName,
  BOOL ReplaceIfExisting,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  string new_path = get_path(NewFileName);
  dout(20) << __func__ << " " << path << " -> " << new_path << dendl;

  int ret = ceph_rename(cmount, path.c_str(), new_path.c_str());
  if (ret) {
    dout(2) << __func__ << " " << path << " -> " << new_path
            << ": ceph_rename failed. Error: " << ret << dendl;
  }

  return cephfs_errno_to_ntstatus_map(ret);
}

static NTSTATUS WinCephSetEndOfFile(
  LPCWSTR FileName,
  LONGLONG ByteOffset,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  if (!fdc->fd) {
    derr << __func__ << ": missing context: " << get_path(FileName) << dendl;
    return STATUS_INVALID_HANDLE;
  }

  int ret = ceph_ftruncate(cmount, fdc->fd, ByteOffset);
  if (ret) {
    dout(2) << __func__ << " " << get_path(FileName)
            << ": ceph_ftruncate failed. Error: " << ret
            << " Offset: " << ByteOffset << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }

  return 0;
}

static NTSTATUS WinCephSetAllocationSize(
  LPCWSTR FileName,
  LONGLONG AllocSize,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  pfd_context fdc = (pfd_context) &(DokanFileInfo->Context);
  if (!fdc->fd) {
    derr << __func__ << ": missing context: " << get_path(FileName) << dendl;
    return STATUS_INVALID_HANDLE;
  }

  struct ceph_statx stbuf;
  unsigned int requested_attrs = CEPH_STATX_BASIC_STATS;
  int ret = ceph_fstatx(cmount, fdc->fd, &stbuf, requested_attrs, 0);
  if (ret) {
    dout(2) << __func__ << " " << get_path(FileName)
            << ": ceph_fstatx failed. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }

  if ((unsigned long long) AllocSize < stbuf.stx_size) {
    int ret = ceph_ftruncate(cmount, fdc->fd, AllocSize);
    if (ret) {
      dout(2) << __func__ << " " << get_path(FileName)
              << ": ceph_ftruncate failed. Error: " << ret << dendl;
      return cephfs_errno_to_ntstatus_map(ret);
    }
    return 0;
  }
  return 0;
}

static NTSTATUS WinCephSetFileAttributes(
  LPCWSTR FileName,
  DWORD FileAttributes,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  dout(20) << __func__ << " (stubbed) " << path << dendl;
  return 0;
}

static NTSTATUS WinCephSetFileTime(
  LPCWSTR FileName,
  CONST FILETIME* CreationTime,
  CONST FILETIME* LastAccessTime,
  CONST FILETIME* LastWriteTime,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  // TODO: as per a previous inline comment, this might cause problems
  // with some apps such as MS Office (different error code than expected
  // or ctime issues probably). We might allow disabling it.
  string path = get_path(FileName);
  dout(20) << __func__ << " " << path << dendl;

  struct ceph_statx stbuf = { 0 };
  int mask = 0;
  if (CreationTime) {
    mask |= CEPH_SETATTR_CTIME;
    // On Windows, st_ctime is the creation time while on Linux it's the time
    // of the last metadata change. We'll try to stick with the Windows
    // semantics, although this might be overridden by Linux hosts.
    to_unix_time(*CreationTime, &stbuf.stx_ctime.tv_sec);
  }
  if (LastAccessTime) {
    mask |= CEPH_SETATTR_ATIME;
    to_unix_time(*LastAccessTime, &stbuf.stx_atime.tv_sec);
  }
  if (LastWriteTime) {
    mask |= CEPH_SETATTR_MTIME;
    to_unix_time(*LastWriteTime, &stbuf.stx_mtime.tv_sec);
  }

  int ret = ceph_setattrx(cmount, path.c_str(), &stbuf, mask, 0);
  if (ret) {
    dout(2) << __func__ << " " << path
            << ": ceph_setattrx failed. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }
  return 0;
}

static NTSTATUS WinCephSetFileSecurity(
  LPCWSTR FileName,
  PSECURITY_INFORMATION SecurityInformation,
  PSECURITY_DESCRIPTOR SecurityDescriptor,
  ULONG SecurityDescriptorLength,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  string path = get_path(FileName);
  dout(20) << __func__ << " (stubbed) " << path << dendl;
  // TODO: Windows ACLs are ignored. At the moment, we're reporting this
  // operation as successful to avoid breaking applications. We might consider
  // making this behavior configurable.
  return 0;
}

static NTSTATUS WinCephGetVolumeInformation(
  LPWSTR VolumeNameBuffer,
  DWORD VolumeNameSize,
  LPDWORD VolumeSerialNumber,
  LPDWORD MaximumComponentLength,
  LPDWORD FileSystemFlags,
  LPWSTR FileSystemNameBuffer,
  DWORD FileSystemNameSize,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  g_cfg->win_vol_name.copy(VolumeNameBuffer, VolumeNameSize);
  *VolumeSerialNumber = g_cfg->win_vol_serial;
  *MaximumComponentLength = g_cfg->max_path_len;

  *FileSystemFlags =
    FILE_SUPPORTS_REMOTE_STORAGE |
    FILE_UNICODE_ON_DISK |
    FILE_PERSISTENT_ACLS;

  if (g_cfg->case_sensitive) {
    *FileSystemFlags |=
      FILE_CASE_SENSITIVE_SEARCH |
      FILE_CASE_PRESERVED_NAMES;
  }

  wcscpy(FileSystemNameBuffer, L"Ceph");
  return 0;
}

static NTSTATUS WinCephGetDiskFreeSpace(
  PULONGLONG FreeBytesAvailable,
  PULONGLONG TotalNumberOfBytes,
  PULONGLONG TotalNumberOfFreeBytes,
  PDOKAN_FILE_INFO DokanFileInfo)
{
  struct statvfs vfsbuf;
  int ret = ceph_statfs(cmount, "/", &vfsbuf);
  if (ret) {
    derr << "ceph_statfs failed. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);;
  }

  *FreeBytesAvailable   = vfsbuf.f_bsize * vfsbuf.f_bfree;
  *TotalNumberOfBytes   = vfsbuf.f_bsize * vfsbuf.f_blocks;
  *TotalNumberOfFreeBytes = vfsbuf.f_bsize * vfsbuf.f_bfree;

  return 0;
}

int do_unmap(wstring& mountpoint) {
  if (!DokanRemoveMountPoint(mountpoint.c_str())) {
    wcerr << "Couldn't remove the specified CephFS mount: "
          << mountpoint << std::endl;
    return -EINVAL;
  }
  return 0;
}

int cleanup_mount() {
  int ret = ceph_unmount(cmount);
  if (ret)
    derr << "Couldn't perform clean unmount. Error: " << ret << dendl;
  else
    dout(0) << "Unmounted." << dendl;
  return ret;
}

static NTSTATUS WinCephUnmount(
  PDOKAN_FILE_INFO  DokanFileInfo)
{
  cleanup_mount();
  // TODO: consider propagating unmount errors to Dokan.
  return 0;
}

BOOL WINAPI ConsoleHandler(DWORD dwType)
{
  switch(dwType) {
  case CTRL_C_EVENT:
    dout(0) << "Received ctrl-c." << dendl;
    exit(0);
  case CTRL_BREAK_EVENT:
    dout(0) << "Received break event." << dendl;
    break;
  default:
    dout(0) << "Received console event: " << dwType << dendl;
  }
  return TRUE;
}

static void unmount_atexit(void)
{
  cleanup_mount();
}

NTSTATUS get_volume_serial(PDWORD serial) {
  int64_t fs_cid = ceph_get_fs_cid(cmount);

  char fsid_str[64] = { 0 };
  int ret = ceph_getxattr(cmount, "/", "ceph.cluster_fsid",
                          fsid_str, sizeof(fsid_str));
  if (ret < 0) {
    dout(2) << "Coudln't retrieve the cluster fsid. Error: " << ret << dendl;
    return cephfs_errno_to_ntstatus_map(ret);
  }

  uuid_d fsid;
  if (!fsid.parse(fsid_str)) {
    dout(2) << "Couldn't parse cluster fsid" << dendl;
    return STATUS_INTERNAL_ERROR;
  }

  // We're generating a volume serial number by concatenating the last 16 bits
  // of the filesystem id and the cluster fsid.
  *serial = ((*(uint16_t*) fsid.bytes() & 0xffff) << 16) | (fs_cid & 0xffff);

  return 0;
}

int do_map() {
  PDOKAN_OPERATIONS dokan_operations =
      (PDOKAN_OPERATIONS) malloc(sizeof(DOKAN_OPERATIONS));
  PDOKAN_OPTIONS dokan_options =
      (PDOKAN_OPTIONS) malloc(sizeof(DOKAN_OPTIONS));
  if (!dokan_operations || !dokan_options) {
    derr << "Not enough memory" << dendl;
    return -ENOMEM;
  }

  int r = set_dokan_options(g_cfg, dokan_options);
  if (r) {
    return r;
  }

  ZeroMemory(dokan_operations, sizeof(DOKAN_OPERATIONS));
  dokan_operations->ZwCreateFile = WinCephCreateFile;
  dokan_operations->Cleanup = WinCephCleanup;
  dokan_operations->CloseFile = WinCephCloseFile;
  dokan_operations->ReadFile = WinCephReadFile;
  dokan_operations->WriteFile = WinCephWriteFile;
  dokan_operations->FlushFileBuffers = WinCephFlushFileBuffers;
  dokan_operations->GetFileInformation = WinCephGetFileInformation;
  dokan_operations->FindFiles = WinCephFindFiles;
  dokan_operations->SetFileAttributes = WinCephSetFileAttributes;
  dokan_operations->SetFileTime = WinCephSetFileTime;
  dokan_operations->DeleteFile = WinCephDeleteFile;
  dokan_operations->DeleteDirectory = WinCephDeleteDirectory;
  dokan_operations->MoveFile = WinCephMoveFile;
  dokan_operations->SetEndOfFile = WinCephSetEndOfFile;
  dokan_operations->SetAllocationSize = WinCephSetAllocationSize;
  dokan_operations->SetFileSecurity = WinCephSetFileSecurity;
  dokan_operations->GetDiskFreeSpace = WinCephGetDiskFreeSpace;
  dokan_operations->GetVolumeInformation = WinCephGetVolumeInformation;
  dokan_operations->Unmounted = WinCephUnmount;

  ceph_create_with_context(&cmount, g_ceph_context);

  r = ceph_mount(cmount, g_cfg->root_path.c_str());
  if (r) {
    derr << "ceph_mount failed. Error: " << r << dendl;
    return cephfs_errno_to_ntstatus_map(r);
  }

  if (g_cfg->win_vol_name.empty()) {
    string ceph_fs_name = g_conf().get_val<string>("client_fs");

    g_cfg->win_vol_name = L"Ceph";
    if (!ceph_fs_name.empty()) {
      g_cfg->win_vol_name += L" - " + to_wstring(ceph_fs_name);
    }
  }

  if (!g_cfg->win_vol_serial) {
    if (get_volume_serial(&g_cfg->win_vol_serial)) {
      return -EINVAL;
    }
  }

  if (g_cfg->max_path_len > 260) {
    dout(0) << "maximum path length set to " << g_cfg->max_path_len 
            << ". Some Windows utilities may not be able to handle "
            << "paths that exceed MAX_PATH (260) characters. "
            << "CreateDirectoryW, used by Powershell, has also been "
            << "observed to fail when paths exceed 16384 characters."
            << dendl;
  }

  atexit(unmount_atexit);
  dout(0) << "Mounted cephfs directory: " << g_cfg->root_path.c_str()
          <<". Mountpoint: " << to_string(g_cfg->mountpoint) << dendl;

  DokanInit();

  DWORD status = DokanMain(dokan_options, dokan_operations);
  switch (static_cast<int>(status)) {
  case DOKAN_SUCCESS:
    dout(2) << "Dokan has returned successfully" << dendl;
    break;
  case DOKAN_ERROR:
    derr << "Received generic dokan error." << dendl;
    break;
  case DOKAN_DRIVE_LETTER_ERROR:
    derr << "Invalid drive letter or mountpoint." << dendl;
    break;
  case DOKAN_DRIVER_INSTALL_ERROR:
    derr << "Can't initialize Dokan driver." << dendl;
    break;
  case DOKAN_START_ERROR:
    derr << "Dokan failed to start" << dendl;
    break;
  case DOKAN_MOUNT_ERROR:
    derr << "Dokan mount error." << dendl;
    break;
  case DOKAN_MOUNT_POINT_ERROR:
    derr << "Invalid mountpoint." << dendl;
    break;
  default:
    derr << "Unknown Dokan error: " << status << dendl;
    break;
  }

  DokanShutdown();

  free(dokan_options);
  free(dokan_operations);
  return 0;
}

boost::intrusive_ptr<CephContext> do_global_init(
  int argc, const char **argv, Command cmd)
{
  auto args = argv_to_vec(argc, argv);

  code_environment_t code_env;
  int flags;

  switch (cmd) {
    case Command::Map:
      code_env = CODE_ENVIRONMENT_DAEMON;
      flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
      break;
    default:
      code_env = CODE_ENVIRONMENT_UTILITY;
      flags = CINIT_FLAG_NO_MON_CONFIG;
      break;
  }

  global_pre_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, code_env, flags);
  // Avoid cluttering the console when spawning a mapping that will run
  // in the background.
  if (g_conf()->daemonize) {
    flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
  }
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         code_env, flags, FALSE);

  // There's no fork on Windows, we should be safe calling this anytime.
  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  return cct;
}

int main(int argc, const char** argv)
{
  SetConsoleOutputCP(CP_UTF8);

  if (!SetConsoleCtrlHandler((PHANDLER_ROUTINE)ConsoleHandler, TRUE)) {
    cerr << "Couldn't initialize console event handler." << std::endl;
    return -EINVAL;
  }

  g_cfg = new Config;

  Command cmd = Command::None;
  auto args = argv_to_vec(argc, argv);
  std::ostringstream err_msg;
  int r = parse_args(args, &err_msg, &cmd, g_cfg);
  if (r) {
    std::cerr << err_msg.str() << std::endl;
    return r;
  }

  switch (cmd) {
    case Command::Version:
      std::cout << pretty_version_to_str() << std::endl;
      return 0;
    case Command::Help:
      print_usage();
      return 0;
    default:
      break;
  }

  auto cct = do_global_init(argc, argv, cmd);

  switch (cmd) {
    case Command::Map:
      return do_map();
    case Command::Unmap:
      return do_unmap(g_cfg->mountpoint);
    default:
      print_usage();
      break;
  }

  return 0;
}
