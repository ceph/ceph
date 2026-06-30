import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';

import { PathEntry } from './mirroring-path.model';

export class MirroringPathUtils {
  static normalizeMirroringPath(path: string): string {
    if (!path || path === '/') {
      return '/';
    }
    return path.trim().replace(/\/+$/, '');
  }

  static normalizeGroupOptionName(groupName: string): string {
    return !groupName ? DEFAULT_SUBVOLUME_GROUP : groupName;
  }

  static buildGroupPath(groupName: string): string {
    return `/volumes/${groupName || DEFAULT_SUBVOLUME_GROUP}`;
  }

  static buildSubvolumePath(groupName: string, subvolName: string): string {
    return `/volumes/${groupName || DEFAULT_SUBVOLUME_GROUP}/${subvolName}`;
  }

  static splitResolvedSubvolumePath(resolvedPath: string): string {
    const segments = MirroringPathUtils.normalizeMirroringPath(resolvedPath)
      .split('/')
      .filter(Boolean);
    segments.pop();
    return segments.length ? `/${segments.join('/')}` : '/';
  }

  static subvolumeNeedsDirSelection(
    groupName: string,
    subvolName: string,
    resolvedPath?: string | null
  ): boolean {
    if (!resolvedPath) {
      return false;
    }
    return (
      MirroringPathUtils.normalizeMirroringPath(resolvedPath) !==
      MirroringPathUtils.normalizeMirroringPath(
        MirroringPathUtils.buildSubvolumePath(groupName, subvolName)
      )
    );
  }

  static withResolvedDisplayPath(entry: PathEntry): PathEntry {
    const groupLevel = entry.levels.find((level) => level.kind === 'group');
    const subvolLevel = entry.levels.find((level) => level.kind === 'subvolume');

    if (!groupLevel?.selected) {
      return {
        ...entry,
        fullPath: '',
        subvolumePath: undefined,
        resolvedSubvolumeRoot: undefined
      };
    }

    if (!subvolLevel?.selected) {
      return {
        ...entry,
        subvolumePath: undefined,
        resolvedSubvolumeRoot: undefined,
        fullPath: MirroringPathUtils.buildGroupPath(groupLevel.selected)
      };
    }

    const dirSegments = entry.levels
      .filter((level) => level.kind === 'dir' && level.selected)
      .map((level) => level.selected);

    const logicalSubvolumePath = MirroringPathUtils.buildSubvolumePath(
      groupLevel.selected,
      subvolLevel.selected
    );

    if (!dirSegments.length) {
      return {
        ...entry,
        fullPath: logicalSubvolumePath
      };
    }

    if (entry.subvolumePath) {
      return {
        ...entry,
        fullPath: MirroringPathUtils.joinMirroringPath(entry.subvolumePath, dirSegments.join('/'))
      };
    }

    return {
      ...entry,
      fullPath: MirroringPathUtils.joinMirroringPath(logicalSubvolumePath, dirSegments.join('/'))
    };
  }

  static getMirrorPath(entry: PathEntry): string {
    const groupLevel = entry.levels.find((level) => level.kind === 'group');
    const subvolLevel = entry.levels.find((level) => level.kind === 'subvolume');
    const dirSegments = entry.levels
      .filter((level) => level.kind === 'dir' && level.selected)
      .map((level) => level.selected);

    if (subvolLevel?.selected && groupLevel?.selected && entry.resolvedSubvolumeRoot) {
      const needsDir = MirroringPathUtils.subvolumeNeedsDirSelection(
        groupLevel.selected,
        subvolLevel.selected,
        entry.resolvedSubvolumeRoot
      );

      if (!dirSegments.length) {
        return needsDir ? '' : entry.resolvedSubvolumeRoot;
      }

      const parentPath =
        entry.subvolumePath ??
        MirroringPathUtils.splitResolvedSubvolumePath(entry.resolvedSubvolumeRoot);
      return MirroringPathUtils.normalizeMirroringPath(
        MirroringPathUtils.joinMirroringPath(parentPath, dirSegments.join('/'))
      );
    }

    if (!entry.fullPath) {
      return '';
    }

    return MirroringPathUtils.normalizeMirroringPath(entry.fullPath);
  }

  static toTrackedPathSet(paths: string[]): Set<string> {
    return new Set(paths.map(MirroringPathUtils.normalizeMirroringPath).filter(Boolean));
  }

  static mirroringPathsOverlap(a: string, b: string): boolean {
    const left = MirroringPathUtils.normalizeMirroringPath(a);
    const right = MirroringPathUtils.normalizeMirroringPath(b);
    if (!left || !right) {
      return false;
    }
    return left === right || left.startsWith(`${right}/`) || right.startsWith(`${left}/`);
  }

  static isMirroringPathTracked(path: string, trackedPaths: Set<string>): boolean {
    const normalized = MirroringPathUtils.normalizeMirroringPath(path);
    if (!normalized || !trackedPaths.size) {
      return false;
    }

    for (const tracked of trackedPaths) {
      if (MirroringPathUtils.mirroringPathsOverlap(normalized, tracked)) {
        return true;
      }
    }

    return false;
  }

  static isGroupPathMirrored(groupPath: string, trackedPaths: Set<string>): boolean {
    const normalized = MirroringPathUtils.normalizeMirroringPath(groupPath);
    if (!normalized || !trackedPaths.size) {
      return false;
    }
    return trackedPaths.has(normalized);
  }

  static joinMirroringPath(parentPath: string, name: string): string {
    const parent = MirroringPathUtils.normalizeMirroringPath(parentPath);
    if (parent === '/') {
      return `/${name}`;
    }
    return `${parent}/${name}`;
  }

  static parseMirrorDirectoryList(response: unknown): string[] {
    if (Array.isArray(response)) {
      return response.filter((path): path is string => typeof path === 'string');
    }
    if (typeof response === 'string') {
      try {
        return MirroringPathUtils.parseMirrorDirectoryList(JSON.parse(response));
      } catch {
        return [];
      }
    }
    return [];
  }

  static isAlreadyTrackedMirrorError(message: string): boolean {
    return /already tracked/i.test(message ?? '');
  }
}
