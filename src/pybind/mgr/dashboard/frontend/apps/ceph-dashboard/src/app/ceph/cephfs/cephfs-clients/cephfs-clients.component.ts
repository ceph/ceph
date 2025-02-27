import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { BaseModal } from 'carbon-components-angular';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-cephfs-clients',
  templateUrl: './cephfs-clients.component.html',
  styleUrls: ['./cephfs-clients.component.scss']
})
export class CephfsClientsComponent extends BaseModal implements OnInit {
  @Input()
  id: number;

  @Input()
  clients: {
    data: any[];
    status: TableStatusViewCache;
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
    private modalService: ModalCdsService,
    private notificationService: NotificationService,
    private authStorageService: AuthStorageService,
    private actionLabels: ActionLabelsI18n
  ) {
    super();
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
        this.closeModal();
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
    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: 'client',
      itemNames: [clientId],
      actionDescription: 'evict',
      submitAction: () => this.evictClient(clientId)
    });
  }
}
