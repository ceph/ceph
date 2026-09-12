export const getSmbBasePath = (url: string): string =>
  url.startsWith('/rgw/smb') ? 'rgw/smb' : 'cephfs/smb';

export const isRgwSmbRoute = (url: string): boolean => url.startsWith('/rgw/smb');

export const getClusterPath = (url: string): string => `${getSmbBasePath(url)}/cluster`;

export const getSharePath = (url: string): string => `${getSmbBasePath(url)}/share`;

export const getJoinAuthPath = (url: string): string =>
  `${getSmbBasePath(url)}/active-directory`;

export const getUsersGroupsPath = (url: string): string => `${getSmbBasePath(url)}/standalone`;
