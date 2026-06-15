import { MirroringPathUtils } from './mirroring-path-utils';

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
    });
  });

  describe('buildPathFromSegments', () => {
    it('should build a path from selected segments', () => {
      expect(MirroringPathUtils.buildPathFromSegments(['g1', 'sv1'])).toBe('/volumes/g1/sv1');
      expect(MirroringPathUtils.buildPathFromSegments([])).toBe('');
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
