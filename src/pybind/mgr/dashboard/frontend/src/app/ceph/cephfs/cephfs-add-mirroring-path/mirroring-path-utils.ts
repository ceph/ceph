import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';

import { MirroringPathSelection, PathEntry } from './mirroring-path.model';

export function normalizeMirroringPath(path: string): string {
  if (!path || path === '/') {
    return '/';
  }
  return path.replace(/\/+$/, '');
}

export function toGroupParam(groupName: string): string {
  return !groupName || groupName === DEFAULT_SUBVOLUME_GROUP ? '' : groupName;
}

export function buildGroupPath(groupName: string): string {
  if (!groupName || groupName === DEFAULT_SUBVOLUME_GROUP) {
    return '/volumes';
  }
  return `/volumes/${groupName}`;
}

export function buildSubvolumePath(groupName: string, subvolName: string): string {
  if (!groupName || groupName === DEFAULT_SUBVOLUME_GROUP) {
    return `/volumes/${subvolName}`;
  }
  return `/volumes/${groupName}/${subvolName}`;
}

export function splitResolvedSubvolumePath(
  resolvedPath: string
): {
  parentPath: string;
  lastSegment: string;
} {
  const segments = normalizeMirroringPath(resolvedPath).split('/').filter(Boolean);
  const lastSegment = segments.pop() ?? '';
  const parentPath = segments.length ? `/${segments.join('/')}` : '/';
  return { parentPath, lastSegment };
}

export function withResolvedDisplayPath(entry: PathEntry): PathEntry {
  const groupLevel = entry.levels.find((level) => level.kind === 'group');
  const subvolLevel = entry.levels.find((level) => level.kind === 'subvolume');

  if (!groupLevel?.selected) {
    return {
      ...entry,
      fullPath: '',
      subvolumePath: undefined,
      resolvedSubvolumeRoot: undefined,
      subvol: undefined,
      group: undefined
    };
  }

  const group = toGroupParam(groupLevel.selected);

  if (!subvolLevel?.selected) {
    return {
      ...entry,
      group,
      subvol: undefined,
      subvolumePath: undefined,
      resolvedSubvolumeRoot: undefined,
      fullPath: buildGroupPath(groupLevel.selected)
    };
  }

  const dirSegments = entry.levels
    .filter((level) => level.kind === 'dir' && level.selected)
    .map((level) => level.selected);

  if (entry.subvolumePath && dirSegments.length) {
    return {
      ...entry,
      group,
      subvol: subvolLevel.selected,
      fullPath: `${entry.subvolumePath}/${dirSegments.join('/')}`
    };
  }

  return {
    ...entry,
    group,
    subvol: subvolLevel.selected,
    fullPath: buildSubvolumePath(groupLevel.selected, subvolLevel.selected)
  };
}

export function getMirrorPath(entry: PathEntry): string {
  if (!entry.fullPath) {
    return '';
  }
  const dirSegments = entry.levels
    .filter((level) => level.kind === 'dir' && level.selected)
    .map((level) => level.selected);
  if (entry.subvol && !dirSegments.length && entry.resolvedSubvolumeRoot) {
    return entry.resolvedSubvolumeRoot;
  }
  return entry.fullPath;
}

export function toMirroringPathSelections(entries: PathEntry[]): MirroringPathSelection[] {
  return entries
    .map((entry) => {
      const path = getMirrorPath(entry);
      if (!path) {
        return null;
      }
      const selection: MirroringPathSelection = { path };
      if (entry.subvol) {
        selection.subvol = entry.subvol;
        if (entry.group) {
          selection.group = entry.group;
        }
      }
      return selection;
    })
    .filter((selection): selection is MirroringPathSelection => selection !== null);
}

export function folderPathFromFileInput(file: File): string {
  const relativePath =
    (file as File & { webkitRelativePath?: string }).webkitRelativePath || file.name;
  return `/${relativePath.split('/')[0]}`;
}
