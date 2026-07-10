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
    });

    it('should allow navigating through parent paths when a child is tracked', () => {
      const tracked = new Set(['/volumes/g1/sv1']);
      expect(MirroringPathUtils.isPathTracked('/volumes/g1', tracked, true)).toBe(false);
      expect(MirroringPathUtils.isPathTracked('/volumes/g1/sv1', tracked, true)).toBe(true);
      expect(MirroringPathUtils.isPathTracked('/volumes/g1/sv2', tracked, true)).toBe(false);
    });
  });

  describe('pathsConflictForNavigation', () => {
    it('should hide exact matches and descendants but allow ancestor navigation', () => {
      expect(
        MirroringPathUtils.pathsConflictForNavigation('/volumes/g1/sv1', '/volumes/g1/sv1')
      ).toBe(true);
      expect(
        MirroringPathUtils.pathsConflictForNavigation('/volumes/g1/sv1/dir', '/volumes/g1/sv1')
      ).toBe(true);
      expect(
        MirroringPathUtils.pathsConflictForNavigation('/volumes/g1', '/volumes/g1/sv1')
      ).toBe(false);
      expect(
        MirroringPathUtils.pathsConflictForNavigation('/volumes/g1/sv2', '/volumes/g1/sv1')
      ).toBe(false);
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

  describe('isEntrySelectionValid', () => {
    it('should accept a subvolume group without selecting a subvolume', () => {
      const groupOnly: PathEntry = {
        fullPath: '/volumes/g1',
        expanded: true,
        levels: [
          { options: ['g1'], selected: 'g1' },
          { options: ['sv1', 'sv2'], selected: '' }
        ]
      };
      const empty: PathEntry = {
        fullPath: '',
        expanded: true,
        levels: [{ options: ['g1'], selected: '' }]
      };

      expect(MirroringPathUtils.isEntrySelectionValid(groupOnly)).toBe(true);
      expect(MirroringPathUtils.isEntrySelectionValid(empty)).toBe(false);
    });
  });

  describe('isEntrySelectionComplete', () => {
    it('should require all pending child levels to be selected', () => {
      const incomplete: PathEntry = {
        fullPath: '/volumes/g1',
        expanded: true,
        levels: [
          { options: ['g1'], selected: 'g1' },
          { options: ['sv1', 'sv2'], selected: '' }
        ]
      };
      const complete: PathEntry = {
        fullPath: '/volumes/g1/sv1',
        expanded: true,
        levels: [
          { options: ['g1'], selected: 'g1' },
          { options: ['sv1', 'sv2'], selected: 'sv1' }
        ]
      };

      expect(MirroringPathUtils.isEntrySelectionComplete(incomplete)).toBe(false);
      expect(MirroringPathUtils.isEntrySelectionComplete(complete)).toBe(true);
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
