import { I18n } from '@ngx-translate/i18n-polyfill';

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

  constructor(i18n: I18n) {
    this.i18n = i18n;

    this.create = {
      permission: 'create',
      icon: 'fa-plus',
      name: this.i18n('Create')
    };
    this.rename = {
      permission: 'update',
      icon: 'fa-pencil',
      name: this.i18n('Rename')
    };
    this.protect = {
      permission: 'update',
      icon: 'fa-lock',
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && !selection.first().is_protected,
      name: this.i18n('Protect')
    };
    this.unprotect = {
      permission: 'update',
      icon: 'fa-unlock',
      visible: (selection: CdTableSelection) =>
        selection.hasSingleSelection && selection.first().is_protected,
      name: this.i18n('Unprotect')
    };
    this.clone = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      icon: 'fa-clone',
      name: this.i18n('Clone')
    };
    this.copy = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      icon: 'fa-copy',
      name: this.i18n('Copy')
    };
    this.rollback = {
      permission: 'update',
      icon: 'fa-undo',
      name: this.i18n('Rollback')
    };
    this.deleteSnap = {
      permission: 'delete',
      icon: 'fa-times',
      disable: (selection: CdTableSelection) => {
        const first = selection.first();
        return !selection.hasSingleSelection || first.cdExecuting || first.is_protected;
      },
      name: this.i18n('Delete')
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
