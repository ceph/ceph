import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { Observable, Subscriber, Subscription } from 'rxjs';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { Icons } from '../../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { Permission } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { ModalService } from '../../../../shared/services/modal.service';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';
import { PoolEditModeModalComponent } from '../pool-edit-mode-modal/pool-edit-mode-modal.component';
import { PoolEditPeerModalComponent } from '../pool-edit-peer-modal/pool-edit-peer-modal.component';

@Component({
  selector: 'cd-mirroring-pools',
  templateUrl: './pool-list.component.html',
  styleUrls: ['./pool-list.component.scss']
})
export class PoolListComponent implements OnInit, OnDestroy {
  @ViewChild('healthTmpl', { static: true })
  healthTmpl: TemplateRef<any>;

  subs: Subscription;

  permission: Permission;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();

  modalRef: NgbModalRef;

  data: [];
  columns: {};

  constructor(
    private authStorageService: AuthStorageService,
    private rbdMirroringService: RbdMirroringService,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService
  ) {
    this.data = [];
    this.permission = this.authStorageService.getPermissions().rbdMirroring;

    const editModeAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      click: () => this.editModeModal(),
      name: $localize`Edit Mode`,
      canBePrimary: () => true
    };
    const addPeerAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: $localize`Add Peer`,
      click: () => this.editPeersModal('add'),
      disable: () => !this.selection.first() || this.selection.first().mirror_mode === 'disabled',
      visible: () => !this.getPeerUUID(),
      canBePrimary: () => false
    };
    const editPeerAction: CdTableAction = {
      permission: 'update',
      icon: Icons.exchange,
      name: $localize`Edit Peer`,
      click: () => this.editPeersModal('edit'),
      visible: () => !!this.getPeerUUID()
    };
    const deletePeerAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      name: $localize`Delete Peer`,
      click: () => this.deletePeersModal(),
      visible: () => !!this.getPeerUUID()
    };
    this.tableActions = [editModeAction, addPeerAction, editPeerAction, deletePeerAction];
  }

  ngOnInit() {
    this.columns = [
      { prop: 'name', name: $localize`Name`, flexGrow: 2 },
      { prop: 'mirror_mode', name: $localize`Mode`, flexGrow: 2 },
      { prop: 'leader_id', name: $localize`Leader`, flexGrow: 2 },
      { prop: 'image_local_count', name: $localize`# Local`, flexGrow: 2 },
      { prop: 'image_remote_count', name: $localize`# Remote`, flexGrow: 2 },
      {
        prop: 'health',
        name: $localize`Health`,
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      this.data = data.content_data.pools;
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }

  editModeModal() {
    const initialState = {
      poolName: this.selection.first().name
    };
    this.modalRef = this.modalService.show(PoolEditModeModalComponent, initialState);
  }

  editPeersModal(mode: string) {
    const initialState = {
      poolName: this.selection.first().name,
      mode: mode
    };
    if (mode === 'edit') {
      initialState['peerUUID'] = this.getPeerUUID();
    }
    this.modalRef = this.modalService.show(PoolEditPeerModalComponent, initialState);
  }

  deletePeersModal() {
    const poolName = this.selection.first().name;
    const peerUUID = this.getPeerUUID();

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
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

  getPeerUUID(): any {
    const selection = this.selection.first();
    const pool = this.data.find((o) => selection && selection.name === o['name']);
    if (pool && pool['peer_uuids']) {
      return pool['peer_uuids'][0];
    }

    return undefined;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
