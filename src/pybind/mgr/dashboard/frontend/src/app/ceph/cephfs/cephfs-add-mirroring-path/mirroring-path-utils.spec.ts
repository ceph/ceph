import { MirroringPathUtils } from './mirroring-path-utils';
import { PathEntry } from './mirroring-path.model';

describe('MirroringPathUtils', () => {
  describe('mirroringPathsOverlap', () => {
    it('should detect exact, ancestor, and descendant overlap', () => {
      expect(MirroringPathUtils.mirroringPathsOverlap('/volumes/g1/sv1', '/volumes/g1/sv1')).toBe(
        true
      );
      expect(
        MirroringPathUtils.mirroringPathsOverlap('/volumes/g1/sv1/dir', '/volumes/g1/sv1')
      ).toBe(true);
      expect(
        MirroringPathUtils.mirroringPathsOverlap('/volumes/g1/sv1', '/volumes/g1/sv1/dir')
      ).toBe(true);
      expect(MirroringPathUtils.mirroringPathsOverlap('/volumes/g1/sv1', '/volumes/g2/sv1')).toBe(
        false
      );
    });
  });

  describe('isMirroringPathTracked', () => {
    it('should match resolved subvolume paths against tracked paths', () => {
      const tracked = MirroringPathUtils.toTrackedPathSet([
        '/volumes/cs/g1/64446b51-d39b-436b-991f-0f8e713067ff'
      ]);
      expect(MirroringPathUtils.isMirroringPathTracked('/volumes/g1/my-subvol', tracked)).toBe(
        false
      );
      expect(
        MirroringPathUtils.isMirroringPathTracked(
          '/volumes/cs/g1/64446b51-d39b-436b-991f-0f8e713067ff',
          tracked
        )
      ).toBe(true);
    });
  });

  describe('isGroupPathMirrored', () => {
    it('should only match exact group paths', () => {
      const tracked = MirroringPathUtils.toTrackedPathSet(['/volumes/g1/sv1']);
      expect(MirroringPathUtils.isGroupPathMirrored('/volumes/g1', tracked)).toBe(false);
      expect(MirroringPathUtils.isGroupPathMirrored('/volumes/g1/sv1', tracked)).toBe(true);
    });
  });

  describe('getMirrorPath', () => {
    it('should require a dir selection before returning a resolved subvolume path', () => {
      const entry: PathEntry = {
        fullPath: '/volumes/Group1/B1iu7',
        expanded: true,
        subvolumePath: '/volumes/Group1/B1iu7',
        resolvedSubvolumeRoot: '/volumes/Group1/B1iu7/0ef49a41-2569-41d7-80a6-7849910e62cb',
        levels: [
          { options: ['Group1'], selected: 'Group1', kind: 'group' },
          { options: ['B1iu7'], selected: 'B1iu7', kind: 'subvolume' }
        ]
      };

      expect(MirroringPathUtils.getMirrorPath(entry)).toBe('');

      entry.levels.push({
        options: ['0ef49a41-2569-41d7-80a6-7849910e62cb'],
        selected: '0ef49a41-2569-41d7-80a6-7849910e62cb',
        kind: 'dir'
      });

      expect(MirroringPathUtils.getMirrorPath(entry)).toBe(
        '/volumes/Group1/B1iu7/0ef49a41-2569-41d7-80a6-7849910e62cb'
      );
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

  describe('parseMirrorDirectoryList', () => {
    it('should parse array and json string responses', () => {
      expect(MirroringPathUtils.parseMirrorDirectoryList(['/volumes/a'])).toEqual(['/volumes/a']);
      expect(MirroringPathUtils.parseMirrorDirectoryList(JSON.stringify(['/volumes/b']))).toEqual([
        '/volumes/b'
      ]);
    });
  });
});
