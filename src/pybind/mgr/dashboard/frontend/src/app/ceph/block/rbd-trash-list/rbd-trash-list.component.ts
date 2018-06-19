import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import * as _ from 'lodash';
import * as moment from 'moment';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { RbdService } from '../../../shared/api/rbd.service';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { CdDatePipe } from '../../../shared/pipes/cd-date.pipe';
import { TaskListService } from '../../../shared/services/task-list.service';

@Component({
  selector: 'cd-rbd-trash-list',
  templateUrl: './rbd-trash-list.component.html',
  styleUrls: ['./rbd-trash-list.component.scss'],
  providers: [TaskListService]
})
export class RbdTrashListComponent implements OnInit {
  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('expiresTpl')
  expiresTpl: TemplateRef<any>;

  columns: CdTableColumn[];
  executingTasks: ExecutingTask[] = [];
  images: any;
  modalRef: BsModalRef;
  retries: number;
  selection = new CdTableSelection();
  viewCacheStatusList: any[];

  constructor(
    private rbdService: RbdService,
    private modalService: BsModalService,
    private cdDatePipe: CdDatePipe,
    private taskListService: TaskListService
  ) {}

  ngOnInit() {
    this.columns = [
      {
        name: 'ID',
        prop: 'id',
        flexGrow: 1,
        cellTransformation: CellTemplate.executing
      },
      {
        name: 'Name',
        prop: 'name',
        flexGrow: 1
      },
      {
        name: 'Pool',
        prop: 'pool_name',
        flexGrow: 1
      },
      {
        name: 'Status',
        prop: 'deferment_end_time',
        flexGrow: 1,
        cellTemplate: this.expiresTpl
      },
      {
        name: 'Deleted At',
        prop: 'deletion_time',
        flexGrow: 1,
        pipe: this.cdDatePipe
      }
    ];

    this.taskListService.init(
      () => this.rbdService.listTrash(),
      (resp) => this.prepareResponse(resp),
      (images) => (this.images = images),
      () => this.onFetchError(),
      this.taskFilter,
      this.itemFilter,
      undefined
    );
  }

  prepareResponse(resp: any[]): any[] {
    let images = [];
    const viewCacheStatusMap = {};
    resp.forEach((pool) => {
      if (_.isUndefined(viewCacheStatusMap[pool.status])) {
        viewCacheStatusMap[pool.status] = [];
      }
      viewCacheStatusMap[pool.status].push(pool.pool_name);
      images = images.concat(pool.value);
    });

    const viewCacheStatusList = [];
    _.forEach(viewCacheStatusMap, (value: any, key) => {
      viewCacheStatusList.push({
        status: parseInt(key, 10),
        statusFor:
          (value.length > 1 ? 'pools ' : 'pool ') +
          '<strong>' +
          value.join('</strong>, <strong>') +
          '</strong>'
      });
    });
    this.viewCacheStatusList = viewCacheStatusList;
    images.forEach((image) => {
      image.cdIsExpired = moment().isAfter(image.deferment_end_time);
    });
    return images;
  }

  onFetchError() {
    this.table.reset(); // Disable loading indicator.
    this.viewCacheStatusList = [{ status: ViewCacheStatus.ValueException }];
  }

  itemFilter(entry, task) {
    return entry.id === task.metadata['image_id'];
  }

  taskFilter(task) {
    return ['rbd/trash/remove', 'rbd/trash/restore'].includes(task.name);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
