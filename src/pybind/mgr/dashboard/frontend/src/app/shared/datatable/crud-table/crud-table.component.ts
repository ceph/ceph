import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { CrudMetadata } from '~/app/shared/models/crud-table-metadata';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { TimerService } from '~/app/shared/services/timer.service';
import { CephUserService } from '../../api/ceph-user.service';
import { ConfirmationModalComponent } from '../../components/confirmation-modal/confirmation-modal.component';
import { CdTableSelection } from '../../models/cd-table-selection';
import { FinishedTask } from '../../models/finished-task';
import { Permission, Permissions } from '../../models/permissions';
import { AuthStorageService } from '../../services/auth-storage.service';
import { TaskWrapperService } from '../../services/task-wrapper.service';
import { ModalService } from '../../services/modal.service';
import { CriticalConfirmationModalComponent } from '../../components/critical-confirmation-modal/critical-confirmation-modal.component';

@Component({
  selector: 'cd-crud-table',
  templateUrl: './crud-table.component.html',
  styleUrls: ['./crud-table.component.scss']
})
export class CRUDTableComponent implements OnInit {
  @ViewChild('badgeDictTpl')
  public badgeDictTpl: TemplateRef<any>;
  @ViewChild('dateTpl')
  public dateTpl: TemplateRef<any>;
  @ViewChild('durationTpl')
  public durationTpl: TemplateRef<any>;
  @ViewChild('exportDataModalTpl')
  public authxEportTpl: TemplateRef<any>;

  data$: Observable<any>;
  meta$: Observable<CrudMetadata>;
  meta: CrudMetadata;
  permissions: Permissions;
  permission: Permission;
  selection = new CdTableSelection();
  expandedRow: { [key: string]: any } = {};
  modalRef: NgbModalRef;
  tabs = {};
  resource: string;
  modalState = {};

  constructor(
    private authStorageService: AuthStorageService,
    private timerService: TimerService,
    private dataGatewayService: DataGatewayService,
    private taskWrapper: TaskWrapperService,
    private cephUserService: CephUserService,
    private activatedRoute: ActivatedRoute,
    private modalService: ModalService,
    private router: Router
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    /* The following should be simplified with a wrapper that
    converts .data to @Input args. For example:
    https://medium.com/@andrewcherepovskiy/passing-route-params-into-angular-components-input-properties-fc85c34c9aca
    */
    this.activatedRoute.data.subscribe((data: any) => {
      const resource: string = data.resource;
      this.tabs = data.tabs;
      this.dataGatewayService
        .list(`ui-${resource}`)
        .subscribe((response: CrudMetadata) => this.processMeta(response));
      this.data$ = this.timerService.get(() => this.dataGatewayService.list(resource));
    });
    this.activatedRoute.data.subscribe((data: any) => {
      this.resource = data.resource;
    });
  }

  processMeta(meta: CrudMetadata) {
    const toCamelCase = (test: string) =>
      test
        .split('-')
        .reduce(
          (res: string, word: string, i: number) =>
            i === 0
              ? word.toLowerCase()
              : `${res}${word.charAt(0).toUpperCase()}${word.substr(1).toLowerCase()}`,
          ''
        );
    this.permission = this.permissions[toCamelCase(meta.permissions[0])];
    const templates = {
      badgeDict: this.badgeDictTpl,
      date: this.dateTpl,
      duration: this.durationTpl
    };
    meta.table.columns.forEach((element, index) => {
      if (element['cellTemplate'] !== undefined) {
        meta.table.columns[index]['cellTemplate'] = templates[element['cellTemplate'] as string];
      }
    });
    // isHidden flag does not work as expected somehow so the best ways to enforce isHidden is
    // to filter the columns manually instead of letting isHidden flag inside table.component to
    // work.
    meta.table.columns = meta.table.columns.filter((col: any) => {
      return !col['isHidden'];
    });

    this.meta = meta;
    for (let i = 0; i < this.meta.actions.length; i++) {
      let action = this.meta.actions[i];
      if (action.disable) {
        action.disable = (selection) => !selection.hasSelection;
      }
      if (action.click.toString() !== '') {
        action.click = this[this.meta.actions[i].click.toString()].bind(this);
      }
    }
  }

  delete() {
    const selectedKey = this.selection.first()[this.meta.columnKey];
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: $localize`${this.meta.columnKey}`,
      itemNames: [selectedKey],
      submitAction: () => {
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('crud-component/id', selectedKey),
            call: this.dataGatewayService.delete(this.resource, selectedKey)
          })
          .subscribe({
            error: () => {
              this.modalRef.close();
            },
            complete: () => {
              this.modalRef.close();
            }
          });
      }
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  setExpandedRow(event: any) {
    for (let i = 0; i < this.meta.detail_columns.length; i++) {
      let column = this.meta.detail_columns[i];
      let columnDetail = event?.[column];
      this.expandedRow[column] = this.formatColumnDetails(columnDetail);
    }
  }

  edit() {
    let key = '';
    if (this.selection.hasSelection) {
      key = this.selection.first()[this.meta.columnKey];
    }
    this.router.navigate(['/cluster/user/edit'], { queryParams: { key: key } });
  }

  authExport() {
    let entities: string[] = [];
    this.selection.selected.forEach((row) => entities.push(row.entity));
    this.cephUserService.export(entities).subscribe((data: string) => {
      const modalVariables = {
        titleText: $localize`Ceph user export data`,
        buttonText: $localize`Close`,
        bodyTpl: this.authxEportTpl,
        showSubmit: true,
        showCancel: false,
        onSubmit: () => {
          this.modalRef.close();
        }
      };
      this.modalState['authExportData'] = data.trim();
      this.modalRef = this.modalService.show(ConfirmationModalComponent, modalVariables);
    });
  }

  /**
   * Custom string replacer function for JSON.stringify
   *
   * This is specifically for objects inside an array.
   * The custom replacer recursively stringifies deep nested objects
   **/
  stringReplacer(_key: string, value: any) {
    try {
      const parsedValue = JSON.parse(value);
      return parsedValue;
    } catch (e) {
      return value;
    }
  }

  /**
   * returns a json string for arrays and string
   * returns the same value for the rest
   **/
  formatColumnDetails(details: any) {
    if (Array.isArray(details) || typeof details === 'string') {
      return JSON.stringify(details, this.stringReplacer, 2);
    }
    return details;
  }
}
