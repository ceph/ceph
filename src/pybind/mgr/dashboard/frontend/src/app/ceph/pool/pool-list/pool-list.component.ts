import { Component, OnInit, ViewChild } from '@angular/core';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';

import { PoolService } from '../../../shared/api/pool.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { Pool } from '../pool';

@Component({
  selector: 'cd-pool-list',
  templateUrl: './pool-list.component.html',
  providers: [TaskListService],
  styleUrls: ['./pool-list.component.scss']
})
export class PoolListComponent implements OnInit {
  @ViewChild(TableComponent)
  table: TableComponent;

  pools: Pool[] = [];
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  modalRef: BsModalRef;
  executingTasks: ExecutingTask[] = [];
  permission: Permission;
  tableActions: CdTableAction[];
  viewCacheStatusList: any[];

  constructor(
    private poolService: PoolService,
    private taskWrapper: TaskWrapperService,
    private authStorageService: AuthStorageService,
    private taskListService: TaskListService,
    private modalService: BsModalService
  ) {
    this.permission = this.authStorageService.getPermissions().pool;
    this.tableActions = [
      { permission: 'create', icon: 'fa-plus', routerLink: () => '/pool/add', name: 'Add' },
      {
        permission: 'update',
        icon: 'fa-pencil',
        routerLink: () => '/pool/edit/' + this.selection.first().pool_name,
        name: 'Edit'
      },
      {
        permission: 'delete',
        icon: 'fa-trash-o',
        click: () => this.deletePoolModal(),
        name: 'Delete'
      }
    ];
    this.columns = [
      {
        prop: 'pool_name',
        name: 'Name',
        flexGrow: 3,
        cellTransformation: CellTemplate.executing
      },
      {
        prop: 'type',
        name: 'Type',
        flexGrow: 2
      },
      {
        prop: 'application_metadata',
        name: 'Applications',
        flexGrow: 3
      },
      {
        prop: 'pg_placement_num',
        name: 'Placement Groups',
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'size',
        name: 'Replica Size',
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'last_change',
        name: 'Last Change',
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'erasure_code_profile',
        name: 'Erasure Coded Profile',
        flexGrow: 2
      },
      {
        prop: 'crush_rule',
        name: 'Crush Ruleset',
        flexGrow: 2
      }
    ];
  }

  ngOnInit() {
    this.taskListService.init(
      () => this.poolService.getList(),
      undefined,
      (pools) => (this.pools = pools),
      () => {
        this.table.reset(); // Disable loading indicator.
        this.viewCacheStatusList = [{ status: ViewCacheStatus.ValueException }];
      },
      (task) => task.name.startsWith('pool/'),
      (pool, task) => task.metadata['pool_name'] === pool.pool_name,
      { default: (task: ExecutingTask) => new Pool(task.metadata['pool_name']) }
    );
  }

  updateSelection(selection: CdTableSelection) {
    if (selection.hasSingleSelection && Object.keys(selection.first()).length === 3) {
      selection.selected = [];
      selection.update();
    }
    this.selection = selection;
  }

  deletePoolModal() {
    const name = this.selection.first().pool_name;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: 'Pool',
        submitActionObservable: () =>
          this.taskWrapper.wrapTaskAroundCall({
            task: new FinishedTask('pool/delete', { pool_name: name }),
            call: this.poolService.delete(name)
          })
      }
    });
  }
}
