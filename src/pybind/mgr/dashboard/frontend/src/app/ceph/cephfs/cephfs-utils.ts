import { Icons } from '~/app/shared/enum/icons.enum';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

export const UNMANAGED_STATE = 'unmanaged';

export function isUnmanagedSelection(selection: CdTableSelection): boolean {
  return Boolean(
    selection?.hasSingleSelection && selection.first()?.info?.state === UNMANAGED_STATE
  );
}

export function unmanagedRecreateHint(resource: string): string {
  return $localize`Recreate this ${resource} to manage it`;
}

/**
 * Disable mutating actions when the selection is missing or unmanaged.
 */
export function getUnmanagedDisable(
  selection: CdTableSelection,
  resource: string
): boolean | string {
  if (!selection?.hasSingleSelection) {
    return true;
  }
  if (isUnmanagedSelection(selection)) {
    return unmanagedRecreateHint(resource);
  }
  return false;
}

export function createRecreateAction(
  actionLabels: ActionLabelsI18n,
  click: () => void
): CdTableAction {
  return {
    name: actionLabels.RECREATE,
    permission: 'create',
    icon: Icons.refresh,
    click,
    visible: isUnmanagedSelection
  };
}
