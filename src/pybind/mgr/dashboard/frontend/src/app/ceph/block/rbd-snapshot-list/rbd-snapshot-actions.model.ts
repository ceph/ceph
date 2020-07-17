import * as _ from 'lodash';

import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

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

  constructor(actionLabels: ActionLabelsI18n, featuresName: string[]) {
    this.create = {
      permission: 'create',
      icon: Icons.add,
      name: actionLabels.CREATE
    };
    this.rename = {
      permission: 'update',
      icon: Icons.edit,
      name: actionLabels.RENAME
    };
    this.protect = {
      permission: 'update',
      icon: Icons.lock,
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && !selection.first().is_protected,
      name: actionLabels.PROTECT
    };
    this.unprotect = {
      permission: 'update',
      icon: Icons.unlock,
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && selection.first().is_protected,
      name: actionLabels.UNPROTECT
    };
    this.clone = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection ||
        selection.first().cdExecuting ||
        !_.isUndefined(this.getCloneDisableDesc(featuresName)),
      disableDesc: () => this.getCloneDisableDesc(featuresName),
      icon: Icons.clone,
      name: actionLabels.CLONE
    };
    this.copy = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      icon: Icons.copy,
      name: actionLabels.COPY
    };
    this.rollback = {
      permission: 'update',
      icon: Icons.undo,
      name: actionLabels.ROLLBACK
    };
    this.deleteSnap = {
      permission: 'delete',
      icon: Icons.destroy,
      disable: (selection: CdTableSelection) => {
        const first = selection.first();
        return !selection.hasSingleSelection || first.cdExecuting || first.is_protected;
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

  getCloneDisableDesc(featuresName: string[]): string | undefined {
    if (!featuresName?.includes('layering')) {
      return $localize`Parent image must support Layering`;
    }

    return undefined;
  }
}
