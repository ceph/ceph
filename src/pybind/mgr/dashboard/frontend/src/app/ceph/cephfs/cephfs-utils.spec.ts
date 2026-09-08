import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import {
  getUnmanagedDisable,
  hasMirroringMdsCaps,
  isUnmanagedSelection,
  unmanagedRecreateHint,
  UNMANAGED_STATE
} from './cephfs-utils';

describe('cephfs-utils', () => {
  const resource = 'subvolume';

  function selectionWith(state?: string): CdTableSelection {
    const selection = new CdTableSelection();
    if (state) {
      selection.selected = [{ name: 'sv1', info: { state } }];
    }
    return selection;
  }

  describe('isUnmanagedSelection', () => {
    it('should detect unmanaged selections', () => {
      expect(isUnmanagedSelection(selectionWith(UNMANAGED_STATE))).toBe(true);
      expect(isUnmanagedSelection(selectionWith('complete'))).toBe(false);
      expect(isUnmanagedSelection(new CdTableSelection())).toBe(false);
    });
  });

  describe('getUnmanagedDisable', () => {
    it('should disable when there is no single selection', () => {
      expect(getUnmanagedDisable(new CdTableSelection(), resource)).toBe(true);
    });

    it('should guide recreate for unmanaged selections', () => {
      expect(getUnmanagedDisable(selectionWith(UNMANAGED_STATE), resource)).toBe(
        unmanagedRecreateHint(resource)
      );
    });

    it('should allow managed selections', () => {
      expect(getUnmanagedDisable(selectionWith('complete'), resource)).toBe(false);
    });
  });

  describe('hasMirroringMdsCaps', () => {
    it('accepts allow rwps fsname=<fs>', () => {
      expect(hasMirroringMdsCaps('allow rwps fsname=myfs', 'myfs')).toBe(true);
    });

    it('accepts allow *', () => {
      expect(hasMirroringMdsCaps('allow *', 'myfs')).toBe(true);
    });

    it('accepts unrestricted allow rwps', () => {
      expect(hasMirroringMdsCaps('allow rwps', 'myfs')).toBe(true);
    });

    it('accepts path=/', () => {
      expect(hasMirroringMdsCaps('allow rwps fsname=myfs path=/', 'myfs')).toBe(true);
    });

    it('accepts a matching grant among multiple grants', () => {
      expect(hasMirroringMdsCaps('allow r fsname=otherfs, allow rwps fsname=myfs', 'myfs')).toBe(
        true
      );
    });

    it('rejects allow r fsname=<fs>', () => {
      expect(hasMirroringMdsCaps('allow r fsname=myfs', 'myfs')).toBe(false);
    });

    it('rejects missing snapshot or pin flags', () => {
      expect(hasMirroringMdsCaps('allow rwp fsname=myfs', 'myfs')).toBe(false);
      expect(hasMirroringMdsCaps('allow rws fsname=myfs', 'myfs')).toBe(false);
    });

    it('rejects the wrong filesystem or a restricted path', () => {
      expect(hasMirroringMdsCaps('allow rwps fsname=otherfs', 'myfs')).toBe(false);
      expect(hasMirroringMdsCaps('allow rwps fsname=myfs path=/foo', 'myfs')).toBe(false);
    });

    it('rejects empty caps', () => {
      expect(hasMirroringMdsCaps('', 'myfs')).toBe(false);
      expect(hasMirroringMdsCaps(undefined, 'myfs')).toBe(false);
    });
  });
});
