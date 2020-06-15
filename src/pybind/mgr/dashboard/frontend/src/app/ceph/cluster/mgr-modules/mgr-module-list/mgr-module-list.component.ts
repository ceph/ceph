import { Component, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BlockUI, NgBlockUI } from 'ng-block-ui';
import { timer as observableTimer } from 'rxjs';

import { MgrModuleService } from '../../../../shared/api/mgr-module.service';
import { ListWithDetails } from '../../../../shared/classes/list-with-details.class';
import { TableComponent } from '../../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { Icons } from '../../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permission } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-mgr-module-list',
  templateUrl: './mgr-module-list.component.html',
  styleUrls: ['./mgr-module-list.component.scss']
})
export class MgrModuleListComponent extends ListWithDetails {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  @BlockUI()
  blockUI: NgBlockUI;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  modules: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private i18n: I18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().configOpt;
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'name',
        flexGrow: 1
      },
      {
        name: this.i18n('Enabled'),
        prop: 'enabled',
        flexGrow: 1,
        cellClass: 'text-center',
        cellTransformation: CellTemplate.checkIcon
      },
      {
        name: this.i18n('Always-On'),
        prop: 'always_on',
        isHidden: true,
        flexGrow: 1,
        cellClass: 'text-center',
        cellTransformation: CellTemplate.checkIcon
      }
    ];
    const getModuleUri = () =>
      this.selection.first() && encodeURIComponent(this.selection.first().name);
    this.tableActions = [
      {
        name: this.i18n('Edit'),
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
        name: this.i18n('Enable'),
        permission: 'update',
        click: () => this.updateModuleState(),
        disable: () => this.isTableActionDisabled('enabled'),
        icon: Icons.start
      },
      {
        name: this.i18n('Disable'),
        permission: 'update',
        click: () => this.updateModuleState(),
        disable: () => this.isTableActionDisabled('disabled'),
        disableDesc: () => this.getTableActionDisabledDesc(),
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

  getTableActionDisabledDesc(): string | undefined {
    if (this.selection.hasSelection) {
      const selected = this.selection.first();
      if (selected.always_on) {
        return this.i18n('This Manager module is always on.');
      }
    }

    return undefined;
  }

  /**
   * Update the Ceph Mgr module state to enabled or disabled.
   */
  updateModuleState() {
    if (!this.selection.hasSelection) {
      return;
    }

    let $obs;
    const fnWaitUntilReconnected = () => {
      observableTimer(2000).subscribe(() => {
        // Trigger an API request to check if the connection is
        // re-established.
        this.mgrModuleService.list().subscribe(
          () => {
            // Resume showing the notification toasties.
            this.notificationService.suspendToasties(false);
            // Unblock the whole UI.
            this.blockUI.stop();
            // Reload the data table content.
            this.table.refreshBtn();
          },
          () => {
            fnWaitUntilReconnected();
          }
        );
      });
    };

    // Note, the Ceph Mgr is always restarted when a module
    // is enabled/disabled.
    const module = this.selection.first();
    if (module.enabled) {
      $obs = this.mgrModuleService.disable(module.name);
    } else {
      $obs = this.mgrModuleService.enable(module.name);
    }
    $obs.subscribe(
      () => undefined,
      () => {
        // Suspend showing the notification toasties.
        this.notificationService.suspendToasties(true);
        // Block the whole UI to prevent user interactions until
        // the connection to the backend is reestablished
        this.blockUI.start(this.i18n('Reconnecting, please wait ...'));
        fnWaitUntilReconnected();
      }
    );
  }
}
