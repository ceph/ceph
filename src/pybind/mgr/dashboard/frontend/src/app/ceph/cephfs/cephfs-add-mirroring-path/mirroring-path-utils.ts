import { PathEntry } from './mirroring-path.model';

const VOLUMES_ROOT = '/volumes';

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
    if (!segments.length) {
      return '';
    }
    return `${VOLUMES_ROOT}/${segments.join('/')}`;
  }

  static getSelectedSegments(entry: PathEntry): string[] {
    return entry.levels.map((level) => level.selected).filter(Boolean);
  }

  static isEntrySelectionComplete(entry: PathEntry): boolean {
    if (!entry.fullPath) {
      return false;
    }

    const lastSelectedIndex = entry.levels.reduce(
      (lastIndex, level, index) => (level.selected ? index : lastIndex),
      -1
    );
    if (lastSelectedIndex < 0) {
      return false;
    }

    for (let i = lastSelectedIndex + 1; i < entry.levels.length; i++) {
      const level = entry.levels[i];
      if (!level.selected && level.options.length) {
        return false;
      }
    }

    return true;
  }

  static pathsOverlap(a: string, b: string): boolean {
    const left = MirroringPathUtils.normalizePath(a);
    const right = MirroringPathUtils.normalizePath(b);
    if (!left || !right) {
      return false;
    }
    return left === right || left.startsWith(`${right}/`) || right.startsWith(`${left}/`);
  }

  static pathsConflictForNavigation(a: string, b: string): boolean {
    const left = MirroringPathUtils.normalizePath(a);
    const right = MirroringPathUtils.normalizePath(b);
    if (!left || !right) {
      return false;
    }
    return left === right || left.startsWith(`${right}/`);
  }

  static isPathTracked(
    path: string,
    trackedPaths: Set<string>,
    forNavigation = false
  ): boolean {
    const normalized = MirroringPathUtils.normalizePath(path);
    if (!normalized || !trackedPaths.size) {
      return false;
    }
    for (const tracked of trackedPaths) {
      const conflicts = forNavigation
        ? MirroringPathUtils.pathsConflictForNavigation(normalized, tracked)
        : MirroringPathUtils.pathsOverlap(normalized, tracked);
      if (conflicts) {
        return true;
      }
    }
    return false;
  }

  static isAlreadyTrackedMirrorError(message: string): boolean {
    return /already tracked/i.test(message ?? '');
  }
}
