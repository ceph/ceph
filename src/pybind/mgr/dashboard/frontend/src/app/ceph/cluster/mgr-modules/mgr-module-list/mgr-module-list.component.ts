import { Component, ViewChild } from '@angular/core';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-mgr-module-list',
  templateUrl: './mgr-module-list.component.html',
  styleUrls: ['./mgr-module-list.component.scss']
})
export class MgrModuleListComponent extends ListWithDetails {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  modules: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private mgrModuleService: MgrModuleService
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().configOpt;
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Enabled`,
        prop: 'enabled',
        flexGrow: 1,
        cellClass: 'text-center',
        cellTransformation: CellTemplate.checkIcon
      },
      {
        name: $localize`Always-On`,
        prop: 'always_on',
        flexGrow: 1,
        cellClass: 'text-center',
        cellTransformation: CellTemplate.checkIcon
      }
    ];
    const getModuleUri = () =>
      this.selection.first() && encodeURIComponent(this.selection.first().name);
    this.tableActions = [
      {
        name: $localize`Edit`,
        permission: 'update',
        disable: () => {
          if (!this.selection.hasSelection) {
            return true;
          }
          // Disable the 'edit' button when the module has no options.
          return Object.values(this.selection.first().options).length === 0;
        },
        routerLink: () => `/mgr-modules/edit/${getModuleUri()}`,
        icon: Icons.edit
      },
      {
        name: $localize`Enable`,
        permission: 'update',
        click: () => this.updateModuleState(),
        disable: () => this.isTableActionDisabled('enabled'),
        icon: Icons.start
      },
      {
        name: $localize`Disable`,
        permission: 'update',
        click: () => this.updateModuleState(),
        disable: () => this.getTableActionDisabledDesc(),
        icon: Icons.stop
      }
    ];
  }

  getModuleList(context: CdTableFetchDataContext) {
    this.mgrModuleService.list().subscribe(
      (resp: object[]) => {
        this.modules = resp;
      },
      () => {
        context.error();
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  /**
   * Check if the table action is disabled.
   * @param state The expected module state, e.g. ``enabled`` or ``disabled``.
   * @returns If the specified state is validated to true or no selection is
   *   done, then ``true`` is returned, otherwise ``false``.
   */
  isTableActionDisabled(state: 'enabled' | 'disabled') {
    if (!this.selection.hasSelection) {
      return true;
    }
    const selected = this.selection.first();
    // Make sure the user can't modify the run state of the 'Dashboard' module.
    // This check is only done in the UI because the REST API should still be
    // able to do so.
    if (selected.name === 'dashboard') {
      return true;
    }
    // Always-on modules can't be disabled.
    if (selected.always_on) {
      return true;
    }
    switch (state) {
      case 'enabled':
        return selected.enabled;
      case 'disabled':
        return !selected.enabled;
    }
  }

  getTableActionDisabledDesc(): string | boolean {
    if (this.selection.first()?.always_on) {
      return $localize`This Manager module is always on.`;
    }

    return this.isTableActionDisabled('disabled');
  }

  /**
   * Update the selected Ceph Mgr module state to enabled or disabled.
   */
  updateModuleState() {
    if (!this.selection.hasSelection) {
      return;
    }
    const selected = this.selection.first();
    this.mgrModuleService.updateModuleState(selected.name, selected.enabled, this.table);
  }
}
