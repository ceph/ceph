import { PathEntry } from './mirroring-path.model';

export const FS_ROOT = '/';
/** Internal select value representing the filesystem root path. */
export const FS_ROOT_PATH_SENTINEL = '__fs_root__';

export class MirroringPathUtils {
  static normalizePath(path: string): string {
    if (!path) {
      return '';
    }
    if (path === '/') {
      return '/';
    }
    return path.trim().replace(/\/+$/, '');
  }

  static joinPath(parent: string, name: string): string {
    const normalizedParent = MirroringPathUtils.normalizePath(parent);
    if (normalizedParent === '/') {
      return `/${name}`;
    }
    return `${normalizedParent}/${name}`;
  }

  static buildPathFromSegments(segments: string[]): string {
    const selected = segments.filter(Boolean);
    if (!selected.length) {
      return '';
    }
    if (selected.some((segment) => MirroringPathUtils.isRootSelection(segment))) {
      return FS_ROOT;
    }
    return MirroringPathUtils.normalizePath(`/${selected.join('/')}`);
  }

  static formatLevelOption(option: string): string {
    return MirroringPathUtils.isRootSelection(option) ? FS_ROOT : option;
  }

  static isRootSelection(value: string): boolean {
    return value === FS_ROOT_PATH_SENTINEL || value === FS_ROOT;
  }

  static isRootPathEntry(entry: PathEntry): boolean {
    if (MirroringPathUtils.normalizePath(entry.fullPath) === FS_ROOT) {
      return true;
    }
    return entry.levels.some((level) => MirroringPathUtils.isRootSelection(level.selected));
  }

  static buildPathFromLevels(levels: PathEntry['levels'], upToLevelIndex: number): string {
    const segments = levels
      .slice(0, upToLevelIndex)
      .map((level) => level.selected)
      .filter(Boolean);
    return MirroringPathUtils.buildPathFromSegments(segments);
  }

  static getSelectedSegments(entry: PathEntry): string[] {
    return entry.levels.map((level) => level.selected).filter(Boolean);
  }

  static pathsOverlap(a: string, b: string): boolean {
    const left = MirroringPathUtils.normalizePath(a);
    const right = MirroringPathUtils.normalizePath(b);
    if (!left || !right) {
      return false;
    }
    if (left === right) {
      return true;
    }
    if (left === FS_ROOT || right === FS_ROOT) {
      return true;
    }
    return left.startsWith(`${right}/`) || right.startsWith(`${left}/`);
  }

  /** Path is already mirrored, or nested under an existing mirror path. */
  static isPathTracked(path: string, trackedPaths: Set<string>): boolean {
    const normalized = MirroringPathUtils.normalizePath(path);
    if (!normalized || !trackedPaths.size) {
      return false;
    }
    for (const tracked of trackedPaths) {
      const trackedPath = MirroringPathUtils.normalizePath(tracked);
      if (normalized === trackedPath || normalized.startsWith(`${trackedPath}/`)) {
        return true;
      }
    }
    return false;
  }

  /** Path cannot be mirrored because it overlaps an existing mirror path. */
  static conflictsWithMirroredPath(path: string, trackedPaths: Set<string>): boolean {
    const normalized = MirroringPathUtils.normalizePath(path);
    if (!normalized || !trackedPaths.size) {
      return false;
    }
    for (const tracked of trackedPaths) {
      if (MirroringPathUtils.pathsOverlap(normalized, MirroringPathUtils.normalizePath(tracked))) {
        return true;
      }
    }
    return false;
  }

  static conflictsWithOtherRowSelection(
    path: string,
    otherPath: string,
    options: { allowAncestor?: boolean } = {}
  ): boolean {
    const normalized = MirroringPathUtils.normalizePath(path);
    const other = MirroringPathUtils.normalizePath(otherPath);
    if (!normalized || !other) {
      return false;
    }
    if (normalized === other || normalized.startsWith(`${other}/`)) {
      return true;
    }
    if (!options.allowAncestor && other.startsWith(`${normalized}/`)) {
      return true;
    }
    return false;
  }

  static isAlreadyTrackedMirrorError(message: string): boolean {
    return /already tracked/i.test(message ?? '');
  }
}
