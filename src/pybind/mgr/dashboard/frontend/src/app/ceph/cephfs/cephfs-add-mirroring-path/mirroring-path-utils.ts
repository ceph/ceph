import { MirroringPathSelection, PathEntry } from './mirroring-path.model';

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

  static pathsOverlap(a: string, b: string): boolean {
    const left = MirroringPathUtils.normalizePath(a);
    const right = MirroringPathUtils.normalizePath(b);
    if (!left || !right) {
      return false;
    }
    return left === right || left.startsWith(`${right}/`) || right.startsWith(`${left}/`);
  }

  static isPathTracked(path: string, trackedPaths: Set<string>): boolean {
    const normalized = MirroringPathUtils.normalizePath(path);
    if (!normalized || !trackedPaths.size) {
      return false;
    }
    for (const tracked of trackedPaths) {
      if (MirroringPathUtils.pathsOverlap(normalized, tracked)) {
        return true;
      }
    }
    return false;
  }

  static isAlreadyTrackedMirrorError(message: string): boolean {
    return /already tracked/i.test(message ?? '');
  }

  static toMirroringPathSelections(entries: PathEntry[]): MirroringPathSelection[] {
    return entries
      .map((entry) => {
        const path = MirroringPathUtils.normalizePath(entry.fullPath);
        if (!path) {
          return null;
        }
        const selection: MirroringPathSelection = { path };
        const segments = path.split('/').filter(Boolean);
        if (segments[0] === 'volumes' && segments.length >= 3) {
          selection.group = segments[1];
          selection.subvol = segments[2];
        }
        return selection;
      })
      .filter((selection): selection is MirroringPathSelection => !!selection);
  }

  static findPathSelection(
    path: string,
    selections: MirroringPathSelection[]
  ): MirroringPathSelection {
    return selections.find((selection) => selection.path === path) ?? { path };
  }
}
