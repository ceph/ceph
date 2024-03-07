import { RbdService } from '~/app/shared/api/rbd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

export class RbdSnapshotActionsModel {
  create: CdTableAction;
  rename: CdTableAction;
  protect: CdTableAction;
  unprotect: CdTableAction;
  clone: CdTableAction;
  copy: CdTableAction;
  rollback: CdTableAction;
  deleteSnap: CdTableAction;
  ordering: CdTableAction[];

  cloneFormatVersion = 1;

  constructor(
    actionLabels: ActionLabelsI18n,
    public featuresName: string[],
    rbdService: RbdService
  ) {
    rbdService.cloneFormatVersion().subscribe((version: number) => {
      this.cloneFormatVersion = version;
    });

    this.create = {
      permission: 'create',
      icon: Icons.add,
      name: actionLabels.CREATE
    };
    this.rename = {
      permission: 'update',
      icon: Icons.edit,
      name: actionLabels.RENAME,
      disable: (selection: CdTableSelection) =>
        this.disableForMirrorSnapshot(selection) || !selection.hasSingleSelection
    };
    this.protect = {
      permission: 'update',
      icon: Icons.lock,
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && !selection.first().is_protected,
      name: actionLabels.PROTECT,
      disable: (selection: CdTableSelection) =>
        this.disableForMirrorSnapshot(selection) ||
        this.getProtectDisableDesc(selection, this.featuresName)
    };
    this.unprotect = {
      permission: 'update',
      icon: Icons.unlock,
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && selection.first().is_protected,
      name: actionLabels.UNPROTECT,
      disable: (selection: CdTableSelection) => this.disableForMirrorSnapshot(selection)
    };
    this.clone = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        this.getCloneDisableDesc(selection) || this.disableForMirrorSnapshot(selection),
      icon: Icons.clone,
      name: actionLabels.CLONE
    };
    this.copy = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection ||
        selection.first().cdExecuting ||
        this.disableForMirrorSnapshot(selection),
      icon: Icons.copy,
      name: actionLabels.COPY
    };
    this.rollback = {
      permission: 'update',
      icon: Icons.undo,
      name: actionLabels.ROLLBACK,
      disable: (selection: CdTableSelection) =>
        this.disableForMirrorSnapshot(selection) || !selection.hasSingleSelection
    };
    this.deleteSnap = {
      permission: 'delete',
      icon: Icons.destroy,
      disable: (selection: CdTableSelection) => {
        const first = selection.first();
        return (
          !selection.hasSingleSelection ||
          first.cdExecuting ||
          first.is_protected ||
          this.disableForMirrorSnapshot(selection)
        );
      },
      name: actionLabels.DELETE
    };

    this.ordering = [
      this.create,
      this.rename,
      this.protect,
      this.unprotect,
      this.clone,
      this.copy,
      this.rollback,
      this.deleteSnap
    ];
  }

  getProtectDisableDesc(selection: CdTableSelection, featuresName: string[]): boolean | string {
    if (selection.hasSingleSelection && !selection.first().cdExecuting) {
      if (!featuresName?.includes('layering')) {
        return $localize`The layering feature needs to be enabled on parent image`;
      }
      return false;
    }
    return true;
  }

  getCloneDisableDesc(selection: CdTableSelection): boolean | string {
    if (selection.hasSingleSelection && !selection.first().cdExecuting) {
      if (this.cloneFormatVersion === 1 && !selection.first().is_protected) {
        return $localize`Snapshot must be protected in order to clone.`;
      }
      return false;
    }
    return true;
  }

  disableForMirrorSnapshot(selection: CdTableSelection) {
    return (
      selection.hasSingleSelection &&
      selection.first().mirror_mode === 'snapshot' &&
      selection.first().name.includes('.mirror.')
    );
  }
}
