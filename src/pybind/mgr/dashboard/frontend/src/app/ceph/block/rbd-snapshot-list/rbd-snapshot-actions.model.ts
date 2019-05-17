import { I18n } from '@ngx-translate/i18n-polyfill';

import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

export class RbdSnapshotActionsModel {
  i18n: I18n;

  create: CdTableAction;
  rename: CdTableAction;
  protect: CdTableAction;
  unprotect: CdTableAction;
  clone: CdTableAction;
  copy: CdTableAction;
  rollback: CdTableAction;
  deleteSnap: CdTableAction;
  ordering: CdTableAction[];

  constructor(i18n: I18n, actionLabels: ActionLabelsI18n) {
    this.i18n = i18n;

    this.create = {
      permission: 'create',
      icon: 'fa-plus',
      name: actionLabels.CREATE
    };
    this.rename = {
      permission: 'update',
      icon: 'fa-pencil',
      name: actionLabels.RENAME
    };
    this.protect = {
      permission: 'update',
      icon: 'fa-lock',
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && !selection.first().is_protected,
      name: actionLabels.PROTECT
    };
    this.unprotect = {
      permission: 'update',
      icon: 'fa-unlock',
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && selection.first().is_protected,
      name: actionLabels.UNPROTECT
    };
    this.clone = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      icon: 'fa-clone',
      name: actionLabels.CLONE
    };
    this.copy = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      icon: 'fa-copy',
      name: actionLabels.COPY
    };
    this.rollback = {
      permission: 'update',
      icon: 'fa-undo',
      name: actionLabels.ROLLBACK
    };
    this.deleteSnap = {
      permission: 'delete',
      icon: 'fa-times',
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
}
