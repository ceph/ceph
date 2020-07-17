import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-cephfs-clients',
  templateUrl: './cephfs-clients.component.html',
  styleUrls: ['./cephfs-clients.component.scss']
})
export class CephfsClientsComponent implements OnInit {
  @Input()
  id: number;

  @Input()
  clients: {
    data: any[];
    status: ViewCacheStatus;
  };

  @Output()
  triggerApiUpdate = new EventEmitter();

  columns: CdTableColumn[];

  permission: Permission;
  tableActions: CdTableAction[];
  modalRef: NgbModalRef;

  selection = new CdTableSelection();

  constructor(
    private cephfsService: CephfsService,
    private modalService: ModalService,
    private notificationService: NotificationService,
    private authStorageService: AuthStorageService,
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
    this.columns = [
      { prop: 'id', name: $localize`id` },
      { prop: 'type', name: $localize`type` },
      { prop: 'state', name: $localize`state` },
      { prop: 'version', name: $localize`version` },
      { prop: 'hostname', name: $localize`Host` },
      { prop: 'root', name: $localize`root` }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  evictClient(clientId: number) {
    this.cephfsService.evictClient(this.id, clientId).subscribe(
      () => {
        this.triggerApiUpdate.emit();
        this.modalRef.close();
        this.notificationService.show(
          NotificationType.success,
          $localize`Evicted client '${clientId}'`
        );
      },
      () => {
        this.modalRef.componentInstance.stopLoadingSpinner();
      }
    );
  }

  evictClientModal() {
    const clientId = this.selection.first().id;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'client',
      itemNames: [clientId],
      actionDescription: 'evict',
      submitAction: () => this.evictClient(clientId)
    });
  }
}
