import { MirroringPathUtils } from './mirroring-path-utils';
import { PathEntry } from './mirroring-path.model';

describe('MirroringPathUtils', () => {
  describe('pathsOverlap', () => {
    it('should detect exact, ancestor, and descendant overlap', () => {
      expect(MirroringPathUtils.pathsOverlap('/volumes/g1/sv1', '/volumes/g1/sv1')).toBe(true);
      expect(MirroringPathUtils.pathsOverlap('/volumes/g1/sv1/dir', '/volumes/g1/sv1')).toBe(true);
      expect(MirroringPathUtils.pathsOverlap('/volumes/g1/sv1', '/volumes/g1/sv1/dir')).toBe(true);
      expect(MirroringPathUtils.pathsOverlap('/volumes/g1/sv1', '/volumes/g2/sv1')).toBe(false);
    });
  });

  describe('isPathTracked', () => {
    it('should match tracked paths and their descendants', () => {
      const tracked = new Set(['/volumes/g1/sv1']);
      expect(MirroringPathUtils.isPathTracked('/volumes/g1/sv1', tracked)).toBe(true);
      expect(MirroringPathUtils.isPathTracked('/volumes/g1/sv1/dir', tracked)).toBe(true);
      expect(MirroringPathUtils.isPathTracked('/volumes/g1/sv2', tracked)).toBe(false);
      expect(MirroringPathUtils.isPathTracked('/volumes/g1', tracked)).toBe(false);
    });
  });

  describe('conflictsWithMirroredPath', () => {
    it('should detect ancestor and descendant conflicts with mirrored paths', () => {
      const tracked = new Set(['/volumes/g1/sv1']);
      expect(MirroringPathUtils.conflictsWithMirroredPath('/volumes/g1/sv1', tracked)).toBe(true);
      expect(MirroringPathUtils.conflictsWithMirroredPath('/volumes/g1/sv1/dir', tracked)).toBe(true);
      expect(MirroringPathUtils.conflictsWithMirroredPath('/volumes/g1', tracked)).toBe(true);
      expect(MirroringPathUtils.conflictsWithMirroredPath('/volumes/g1/sv2', tracked)).toBe(false);
    });
  });

  describe('conflictsWithOtherRowSelection', () => {
    it('should allow ancestor navigation but block final ancestor selections', () => {
      expect(
        MirroringPathUtils.conflictsWithOtherRowSelection(
          '/volumes/g1',
          '/volumes/g1/sv1',
          { allowAncestor: true }
        )
      ).toBe(false);
      expect(
        MirroringPathUtils.conflictsWithOtherRowSelection(
          '/volumes/g1',
          '/volumes/g1/sv1',
          { allowAncestor: false }
        )
      ).toBe(true);
      expect(
        MirroringPathUtils.conflictsWithOtherRowSelection(
          '/volumes/g1/sv2',
          '/volumes/g1/sv1',
          { allowAncestor: false }
        )
      ).toBe(false);
      expect(
        MirroringPathUtils.conflictsWithOtherRowSelection(
          '/volumes/g1/sv1',
          '/volumes/g1/sv1',
          { allowAncestor: true }
        )
      ).toBe(true);
    });
  });

  describe('buildPathFromSegments', () => {
    it('should build a path from selected segments', () => {
      expect(MirroringPathUtils.buildPathFromSegments(['g1', 'sv1'])).toBe('/volumes/g1/sv1');
      expect(MirroringPathUtils.buildPathFromSegments([])).toBe('');
    });
  });

  describe('getSelectedSegments', () => {
    it('should return only selected level values', () => {
      const entry: PathEntry = {
        fullPath: '/volumes/g1/sv1',
        expanded: true,
        levels: [
          { options: ['g1'], selected: 'g1' },
          { options: ['sv1', 'sv2'], selected: 'sv1' },
          { options: [], selected: '' }
        ]
      };

      expect(MirroringPathUtils.getSelectedSegments(entry)).toEqual(['g1', 'sv1']);
    });
  });

  describe('isAlreadyTrackedMirrorError', () => {
    it('should detect already tracked mirror errors', () => {
      expect(
        MirroringPathUtils.isAlreadyTrackedMirrorError(
          'directory /volumes/Group4/A1/uuid is already tracked'
        )
      ).toBe(true);
      expect(MirroringPathUtils.isAlreadyTrackedMirrorError('permission denied')).toBe(false);
    });
  });
});
