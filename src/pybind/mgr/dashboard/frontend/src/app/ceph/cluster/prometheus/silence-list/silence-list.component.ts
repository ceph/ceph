import { Component, Inject } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { SortDirection, SortPropDir } from '@swimlane/ngx-datatable';
import { Observable, Subscriber } from 'rxjs';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n, SucceededActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { AlertmanagerSilence } from '~/app/shared/models/alertmanager-silence';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { PrometheusListHelper } from '../prometheus-list-helper';

const BASE_URL = 'monitoring/silences';

@Component({
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  selector: 'cd-silences-list',
  templateUrl: './silence-list.component.html',
  styleUrls: ['./silence-list.component.scss']
})
export class SilenceListComponent extends PrometheusListHelper {
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
    private cdDatePipe: CdDatePipe,
    private modalService: ModalService,
    private notificationService: NotificationService,
    private urlBuilder: URLBuilderService,
    private actionLabels: ActionLabelsI18n,
    private succeededLabels: SucceededActionLabelsI18n,
    @Inject(PrometheusService) prometheusService: PrometheusService
  ) {
    super(prometheusService);
    this.permission = this.authStorageService.getPermissions().prometheus;
    const selectionExpired = (selection: CdTableSelection) =>
      selection.first() && selection.first().status && selection.first().status.state === 'expired';
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        routerLink: () => this.urlBuilder.getCreate(),
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
        name: $localize`ID`,
        prop: 'id',
        flexGrow: 3
      },
      {
        name: $localize`Created by`,
        prop: 'createdBy',
        flexGrow: 2
      },
      {
        name: $localize`Started`,
        prop: 'startsAt',
        pipe: this.cdDatePipe
      },
      {
        name: $localize`Updated`,
        prop: 'updatedAt',
        pipe: this.cdDatePipe
      },
      {
        name: $localize`Ends`,
        prop: 'endsAt',
        pipe: this.cdDatePipe
      },
      {
        name: $localize`Status`,
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
    const i18nSilence = $localize`Silence`;
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
