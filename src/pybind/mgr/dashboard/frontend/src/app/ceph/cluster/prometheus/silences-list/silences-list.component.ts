import { Component, OnInit } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';
import { SortDirection, SortPropDir } from '@swimlane/ngx-datatable';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { Observable, Subscriber } from 'rxjs';

import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permission } from '../../../../shared/models/permissions';
import { PrometheusSilence } from '../../../../shared/models/prometheus-silence';
import { CdDatePipe } from '../../../../shared/pipes/cd-date.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-silences-list',
  templateUrl: './silences-list.component.html',
  styleUrls: ['./silences-list.component.scss']
})
export class SilencesListComponent implements OnInit {
  silences: PrometheusSilence[] = [];
  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  permission: Permission;
  selection = new CdTableSelection();
  modalRef: BsModalRef;
  customCss = {
    'label label-danger': 'active',
    'label label-warning': 'pending',
    'label label-default': 'expired'
  };
  sorts: SortPropDir[] = [{ prop: 'endsAt', dir: SortDirection.desc }];
  private connected = true;

  constructor(
    private authStorageService: AuthStorageService,
    private prometheusService: PrometheusService,
    private modalService: BsModalService,
    private i18n: I18n,
    private cdDatePipe: CdDatePipe
  ) {
    this.permission = this.authStorageService.getPermissions().prometheus;
  }

  ngOnInit() {
    this.tableActions = [
      {
        permission: 'create',
        icon: 'fa-plus',
        routerLink: () => '/silence/add',
        canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection,
        name: this.i18n('Add')
      },
      {
        permission: 'create',
        canBePrimary: (selection: CdTableSelection) =>
          selection.hasSingleSelection && selection.first().status.state === 'expired',
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection ||
          selection.first().cdExecuting ||
          (selection.first().cdExecuting && selection.first().status.state === 'expired') ||
          selection.first().status.state !== 'expired',
        icon: 'fa-copy',
        routerLink: () => '/silence/recreate/' + this.selection.first().id,
        name: this.i18n('Recreate')
      },
      {
        permission: 'update',
        icon: 'fa-pencil',
        canBePrimary: (selection: CdTableSelection) =>
          selection.hasSingleSelection && selection.first().status.state !== 'expired',
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection ||
          selection.first().cdExecuting ||
          (selection.first().cdExecuting && selection.first().status.state !== 'expired') ||
          selection.first().status.state === 'expired',
        routerLink: () => '/silence/edit/' + this.selection.first().id,
        name: this.i18n('Edit')
      },
      {
        permission: 'delete',
        icon: 'fa-trash-o',
        canBePrimary: (selection: CdTableSelection) =>
          selection.hasSingleSelection && selection.first().status.state !== 'expired',
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection ||
          selection.first().cdExecuting ||
          selection.first().status.state === 'expired',
        click: () => this.expireSilence(),
        name: this.i18n('Expire')
      }
    ];
    this.columns = [
      {
        name: this.i18n('ID'),
        prop: 'id',
        flexGrow: 2
      },
      {
        name: this.i18n('Created by'),
        prop: 'createdBy',
        flexGrow: 2
      },
      {
        name: this.i18n('Started'),
        prop: 'startsAt',
        pipe: this.cdDatePipe
      },
      {
        name: this.i18n('Updated'),
        prop: 'updatedAt',
        pipe: this.cdDatePipe
      },
      {
        name: this.i18n('Ends'),
        prop: 'endsAt',
        pipe: this.cdDatePipe
      },
      {
        name: this.i18n('Status'),
        prop: 'status.state',
        cellTransformation: CellTemplate.classAdding
      }
    ];
  }

  refresh() {
    this.prometheusService.ifAlertmanagerConfigured((url) => {
      if (this.connected) {
        this.prometheusService.getSilences().subscribe(
          (silences) => {
            this.silences = silences;
          },
          (resp) => {
            const errorMsg = `Please check if <a target="_blank" href="${url}">Prometheus Alertmanager</a> is still running`;
            resp['application'] = 'Prometheus';
            if (resp.status === 500) {
              resp.error.detail = errorMsg;
            }
            this.connected = false;
          }
        );
      }
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  expireSilence() {
    const id = this.selection.first().id;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: this.i18n('silence'),
        actionDescription: this.i18n('expire'),
        submitActionObservable: () =>
          new Observable((observer: Subscriber<any>) => {
            this.prometheusService.expireSilence(id).subscribe(
              null,
              (resp) => {
                resp['application'] = 'Prometheus';
                observer.error(resp);
              },
              () => {
                observer.complete();
                this.refresh();
              }
            );
          })
      }
    });
  }
}
