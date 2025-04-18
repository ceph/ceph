import { Component, Input, OnInit, SimpleChanges } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { PoolEditPeerModalComponent } from '../../../pool-edit-peer-modal/pool-edit-peer-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Observable, Subscriber } from 'rxjs';

@Component({
  selector: 'cd-pool-peers',
  templateUrl: './pool-peers.component.html',
  styleUrls: ['./pool-peers.component.scss']
})
export class PoolPeersComponent implements OnInit {
  @Input()
  selection: any;
  peerData: any;
  peerDatainfo: any = [];
  columns: {};
  tableActions: CdTableAction[];
  permission: Permission;
  tableStatus: any;

  constructor(
    private authStorageService: AuthStorageService,
    private modalService: ModalCdsService,
    private rbdMirroringService: RbdMirroringService,
    private taskWrapper: TaskWrapperService
  ) {
    this.permission = this.authStorageService.getPermissions().rbdMirroring;

    this.tableActions = [
      {
        permission: 'update',
        icon: Icons.exchange,
        name: $localize`Edit Peer`,
        click: () => this.editPeersModal()
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        name: $localize`Delete Peer`,
        click: () => this.deletePeersModal()
      }
    ];
  }
  ngOnChanges(changes: SimpleChanges) {
    const poolName = changes['selection'].currentValue.name;
    this.fetchPeerData(poolName);
  }
  ngOnInit() {
    this.columns = [
      { prop: 'cluster_name', name: $localize`Cluster Name`, flexGrow: 4 },
      { prop: 'client_name', name: $localize`CephX ID`, flexGrow: 4 },
      { prop: 'peer_uuid', name: $localize`Peer ID`, flexGrow: 4 }
    ];
  }
  editPeersModal() {
    const selection = this.selection.first();

    const initialState = {
      poolName: this.peerData.pool_name,
      peerUUID: selection.peer_uuid,
      mode: 'edit'
    };

    this.modalService.show(PoolEditPeerModalComponent, initialState);
  }

  deletePeersModal() {
    const selection = this.selection.first();
    const poolName = this.peerData.pool_name;
    const peerUUID = selection.peer_uuid;

    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`mirror peer`,
      itemNames: [`${poolName} (${peerUUID})`],
      submitActionObservable: () =>
        new Observable((observer: Subscriber<any>) => {
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('rbd/mirroring/peer/delete', {
                pool_name: poolName
              }),
              call: this.rbdMirroringService.deletePeer(poolName, peerUUID)
            })
            .subscribe({
              error: (resp) => observer.error(resp),
              complete: () => {
                this.rbdMirroringService.refresh();
                observer.complete();
              }
            });
        })
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
  fetchPeerData(poolName: any) {
    this.rbdMirroringService.getPeerForPool(poolName).subscribe((resp) => {
      if (resp) {
        this.peerData = resp;
        this.peerDatainfo = resp['info'];
      } else {
        this.peerData = [];
        this.peerDatainfo = [];
      }
    });
  }
}
