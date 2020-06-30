import { Component } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';
import { SortDirection, SortPropDir } from '@swimlane/ngx-datatable';
import { Observable, Subscriber } from 'rxjs';

import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { ListWithDetails } from '../../../../shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import {
  ActionLabelsI18n,
  SucceededActionLabelsI18n
} from '../../../../shared/constants/app.constants';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { Icons } from '../../../../shared/enum/icons.enum';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { AlertmanagerSilence } from '../../../../shared/models/alertmanager-silence';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permission } from '../../../../shared/models/permissions';
import { CdDatePipe } from '../../../../shared/pipes/cd-date.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { ModalService } from '../../../../shared/services/modal.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { URLBuilderService } from '../../../../shared/services/url-builder.service';

const BASE_URL = 'monitoring/silence';

@Component({
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  selector: 'cd-silences-list',
  templateUrl: './silence-list.component.html',
  styleUrls: ['./silence-list.component.scss']
})
export class SilenceListComponent extends ListWithDetails {
  silences: AlertmanagerSilence[] = [];
  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  permission: Permission;
  selection = new CdTableSelection();
  modalRef: NgbModalRef;
  customCss = {
    'badge badge-danger': 'active',
    'badge badge-warning': 'pending',
    'badge badge-default': 'expired'
  };
  sorts: SortPropDir[] = [{ prop: 'endsAt', dir: SortDirection.desc }];

  constructor(
    private authStorageService: AuthStorageService,
    private i18n: I18n,
    private cdDatePipe: CdDatePipe,
    private prometheusService: PrometheusService,
    private modalService: ModalService,
    private notificationService: NotificationService,
    private urlBuilder: URLBuilderService,
    private actionLabels: ActionLabelsI18n,
    private succeededLabels: SucceededActionLabelsI18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().prometheus;
    const selectionExpired = (selection: CdTableSelection) =>
      selection.first() && selection.first().status && selection.first().status.state === 'expired';
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        routerLink: () => this.urlBuilder.getCreate(),
        preserveFragment: true,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection,
        name: this.actionLabels.CREATE
      },
      {
        permission: 'create',
        canBePrimary: (selection: CdTableSelection) =>
          selection.hasSingleSelection && selectionExpired(selection),
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection ||
          selection.first().cdExecuting ||
          (selection.first().cdExecuting && selectionExpired(selection)) ||
          !selectionExpired(selection),
        icon: Icons.copy,
        routerLink: () => this.urlBuilder.getRecreate(this.selection.first().id),
        preserveFragment: true,
        name: this.actionLabels.RECREATE
      },
      {
        permission: 'update',
        icon: Icons.edit,
        canBePrimary: (selection: CdTableSelection) =>
          selection.hasSingleSelection && !selectionExpired(selection),
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection ||
          selection.first().cdExecuting ||
          (selection.first().cdExecuting && !selectionExpired(selection)) ||
          selectionExpired(selection),
        routerLink: () => this.urlBuilder.getEdit(this.selection.first().id),
        preserveFragment: true,
        name: this.actionLabels.EDIT
      },
      {
        permission: 'delete',
        icon: Icons.trash,
        canBePrimary: (selection: CdTableSelection) =>
          selection.hasSingleSelection && !selectionExpired(selection),
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection ||
          selection.first().cdExecuting ||
          selectionExpired(selection),
        click: () => this.expireSilence(),
        name: this.actionLabels.EXPIRE
      }
    ];
    this.columns = [
      {
        name: this.i18n('ID'),
        prop: 'id',
        flexGrow: 3
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
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.prometheusService.getSilences().subscribe(
        (silences) => {
          this.silences = silences;
        },
        () => {
          this.prometheusService.disableAlertmanagerConfig();
        }
      );
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  expireSilence() {
    const id = this.selection.first().id;
    const i18nSilence = this.i18n('Silence');
    const applicationName = 'Prometheus';
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: i18nSilence,
      itemNames: [id],
      actionDescription: this.actionLabels.EXPIRE,
      submitActionObservable: () =>
        new Observable((observer: Subscriber<any>) => {
          this.prometheusService.expireSilence(id).subscribe(
            () => {
              this.notificationService.show(
                NotificationType.success,
                `${this.succeededLabels.EXPIRED} ${i18nSilence} ${id}`,
                undefined,
                undefined,
                applicationName
              );
            },
            (resp) => {
              resp['application'] = applicationName;
              observer.error(resp);
            },
            () => {
              observer.complete();
              this.refresh();
            }
          );
        })
    });
  }
}
