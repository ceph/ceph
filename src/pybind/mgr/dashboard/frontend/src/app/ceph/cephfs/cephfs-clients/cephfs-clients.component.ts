import { Component, Input, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { CephfsService } from '../../../shared/api/cephfs.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-cephfs-clients',
  templateUrl: './cephfs-clients.component.html',
  styleUrls: ['./cephfs-clients.component.scss']
})
export class CephfsClientsComponent implements OnInit {
  @Input()
  id: number;

  permission: Permission;
  tableActions: CdTableAction[];
  modalRef: BsModalRef;
  clients: any;
  viewCacheStatus: ViewCacheStatus;
  selection = new CdTableSelection();

  constructor(
    private cephfsService: CephfsService,
    private modalService: BsModalService,
    private notificationService: NotificationService,
    private authStorageService: AuthStorageService,
    private i18n: I18n,
    private actionLabels: ActionLabelsI18n
  ) {
    this.permission = this.authStorageService.getPermissions().cephfs;
    const evictAction: CdTableAction = {
      permission: 'update',
      icon: Icons.signOut,
      click: () => this.evictClientModal(),
      name: this.actionLabels.EVICT
    };
    this.tableActions = [evictAction];
  }

  ngOnInit() {
    this.clients = {
      columns: [
        { prop: 'id', name: this.i18n('id') },
        { prop: 'type', name: this.i18n('type') },
        { prop: 'state', name: this.i18n('state') },
        { prop: 'version', name: this.i18n('version') },
        { prop: 'hostname', name: this.i18n('Host') },
        { prop: 'root', name: this.i18n('root') }
      ],
      data: []
    };

    this.clients.data = [];
    this.viewCacheStatus = ViewCacheStatus.ValueNone;
  }

  refresh() {
    this.cephfsService.getClients(this.id).subscribe((data: any) => {
      this.viewCacheStatus = data.status;
      this.clients.data = data.data;
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  evictClient(clientId: number) {
    this.cephfsService.evictClient(this.id, clientId).subscribe(
      () => {
        this.refresh();
        this.modalRef.hide();
        this.notificationService.show(
          NotificationType.success,
          this.i18n('Evicted client "{{clientId}}"', { clientId: clientId })
        );
      },
      () => {
        this.modalRef.content.stopLoadingSpinner();
      }
    );
  }

  evictClientModal() {
    const clientId = this.selection.first().id;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: 'client',
        actionDescription: 'evict',
        submitAction: () => this.evictClient(clientId)
      }
    });
  }
}
