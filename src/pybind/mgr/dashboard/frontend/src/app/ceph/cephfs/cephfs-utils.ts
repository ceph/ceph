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

/**
 * True when MDS caps match `ceph fs authorize <fs> <client> / rwps`
 * (`allow rwps fsname=<fs>`). `allow *` and unrestricted `allow rwps` also pass.
 */
export function hasMirroringMdsCaps(mdsCaps?: string | null, fsName?: string | null): boolean {
  if (!mdsCaps || !fsName) {
    return false;
  }
  return mdsCaps.split(',').some((grant) => isMirroringMdsGrant(grant.trim(), fsName));
}

function isMirroringMdsGrant(grant: string, fsName: string): boolean {
  if (!grant.startsWith('allow ')) {
    return false;
  }
  const [perms, ...rest] = grant.slice(6).split(/\s+/).filter(Boolean);
  if (!perms || (perms !== '*' && !['r', 'w', 'p', 's'].every((flag) => perms.includes(flag)))) {
    return false;
  }
  const fs = capAttr(rest, 'fsname');
  if (fs && fs !== fsName && fs !== '*' && fs !== 'all') {
    return false;
  }
  const path = capAttr(rest, 'path');
  return !path || path === '/';
}

function capAttr(tokens: string[], key: string): string | undefined {
  const prefix = `${key}=`;
  return tokens.find((token) => token.startsWith(prefix))?.slice(prefix.length);
}
