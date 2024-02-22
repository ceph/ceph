import { Component, NgZone, OnInit, ViewChild } from '@angular/core';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Account } from '../models/rgw-user-accounts';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { Router } from '@angular/router';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { Observable, Subscriber, forkJoin as observableForkJoin } from 'rxjs';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

const BASE_URL = 'rgw/accounts';

@Component({
  selector: 'cd-rgw-user-accounts',
  templateUrl: './rgw-user-accounts.component.html',
  styleUrls: ['./rgw-user-accounts.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwUserAccountsComponent extends ListWithDetails implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  permission: Permission;
  tableActions: CdTableAction[] = [];
  columns: CdTableColumn[] = [];
  accounts: Account[] = [];
  selection: CdTableSelection = new CdTableSelection();
  declare staleTimeout: number;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private rgwUserAccountsService: RgwUserAccountsService,
    private cdsModalService: ModalCdsService,
    private taskWrapper: TaskWrapperService,
    protected ngZone: NgZone
  ) {
    super();
  }

  ngOnInit() {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Tenant`,
        prop: 'tenant',
        flexGrow: 1
      },
      {
        name: $localize`Account id`,
        prop: 'id',
        flexGrow: 1
      },
      {
        name: $localize`Email address`,
        prop: 'email',
        flexGrow: 1
      },
      {
        name: $localize`Max users`,
        prop: 'max_users',
        flexGrow: 1,
        cellTransformation: CellTemplate.map,
        customTemplateConfig: {
          '-1': $localize`Disabled`,
          0: $localize`Unlimited`
        }
      },
      {
        name: $localize`Max roles`,
        prop: 'max_roles',
        flexGrow: 1,
        cellTransformation: CellTemplate.map,
        customTemplateConfig: {
          '-1': $localize`Disabled`,
          0: $localize`Unlimited`
        }
      },
      {
        name: $localize`Max groups`,
        prop: 'max_groups',
        flexGrow: 1,
        cellTransformation: CellTemplate.map,
        customTemplateConfig: {
          '-1': $localize`Disabled`,
          0: $localize`Unlimited`
        }
      },
      {
        name: $localize`Max. buckets`,
        prop: 'max_buckets',
        flexGrow: 1,
        cellTransformation: CellTemplate.map,
        customTemplateConfig: {
          '-1': $localize`Disabled`,
          0: $localize`Unlimited`
        }
      },
      {
        name: $localize`Max access keys`,
        prop: 'max_access_keys',
        flexGrow: 1,
        cellTransformation: CellTemplate.map,
        customTemplateConfig: {
          '-1': $localize`Disabled`,
          0: $localize`Unlimited`
        }
      }
    ];
    const getEditURL = () => {
      if (this.selection.first().groupName && this.selection.first().id) {
        return `${URLVerbs.EDIT}/${this.selection.first().id}
        }`;
      }
      return `${URLVerbs.EDIT}/${this.selection.first().id}`;
    };
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      click: () => this.router.navigate([`${BASE_URL}/${URLVerbs.CREATE}`]),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      click: () => this.router.navigate([`${BASE_URL}/${getEditURL()}`]),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteAction(),
      name: this.actionLabels.DELETE
    };
    this.tableActions = [addAction, editAction, deleteAction];
    this.setTableRefreshTimeout();
  }

  getAccountsList(context?: CdTableFetchDataContext) {
    this.setTableRefreshTimeout();
    this.rgwUserAccountsService.list(true).subscribe({
      next: (accounts: Account[]) => {
        this.accounts = accounts;
      },
      error: () => {
        if (context) {
          context.error();
        }
      }
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    const account_name = this.selection.first().name;
    this.cdsModalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Account`,
      itemNames: [account_name],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask(BASE_URL, {
                account_names: [account_name]
              }),
              call: observableForkJoin(
                this.selection.selected.map((account: Account) => {
                  return this.rgwUserAccountsService.remove(account.id);
                })
              )
            })
            .subscribe({
              error: (error: any) => {
                // Forward the error to the observer.
                observer.error(error);
                // Reload the data table content because some deletions might
                // have been executed successfully in the meanwhile.
                this.table.refreshBtn();
              },
              complete: () => {
                // Notify the observer that we are done.
                observer.complete();
                // Reload the data table content.
                this.table.refreshBtn();
              }
            });
        });
      }
    });
  }
}
