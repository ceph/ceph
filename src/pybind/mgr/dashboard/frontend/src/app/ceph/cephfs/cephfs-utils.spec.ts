import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import {
  getUnmanagedDisable,
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
});
