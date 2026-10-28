import { InjectionToken } from '@angular/core';

/** Shared by SMB and NFS — both support CephFS and RGW backends. */
export enum StorageBackend {
  CEPHFS = 'cephfs',
  RGW = 'rgw'
}

export type SmbContext =
  | { backend: StorageBackend.CEPHFS; basePath: 'cephfs/smb' }
  | { backend: StorageBackend.RGW; basePath: 'rgw/smb' };

export const SMB_CONTEXT = new InjectionToken<SmbContext>('SMB_CONTEXT');

export const SMB_CEPHFS_CONTEXT: SmbContext = {
  backend: StorageBackend.CEPHFS,
  basePath: 'cephfs/smb'
};

export const SMB_RGW_CONTEXT: SmbContext = {
  backend: StorageBackend.RGW,
  basePath: 'rgw/smb'
};

export const SMB_BASE_CEPHFS = SMB_CEPHFS_CONTEXT.basePath;
export const SMB_BASE_RGW = SMB_RGW_CONTEXT.basePath;
