import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { Subscription } from 'rxjs';

import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';
import { URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { PoolEditPeerModalComponent } from '../pool-edit-peer-modal/pool-edit-peer-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

const BASE_URL = '/block/mirroring';
@Component({
  selector: 'cd-mirroring-pools',
  templateUrl: './pool-list.component.html',
  styleUrls: ['./pool-list.component.scss']
})
export class PoolListComponent implements OnInit, OnDestroy {
  @ViewChild('healthTmpl', { static: true })
  healthTmpl: TemplateRef<any>;
  @ViewChild('localTmpl', { static: true })
  localTmpl: TemplateRef<any>;
  @ViewChild('remoteTmpl', { static: true })
  remoteTmpl: TemplateRef<any>;
  subs: Subscription;
  permission: Permission;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  expandedRow: any;
  data: [];
  columns: {};
  tableStatus = new TableStatusViewCache();
  poolName: any;

  constructor(
    private authStorageService: AuthStorageService,
    private rbdMirroringService: RbdMirroringService,
    private modalService: ModalCdsService,
    private router: Router
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

    this.tableActions = [editModeAction, addPeerAction];
  }

  ngOnInit() {
    this.columns = [
      { prop: 'name', name: $localize`Name`, flexGrow: 2 },
      { prop: 'mirror_mode', name: $localize`Mode`, flexGrow: 2 },
      { prop: 'leader_id', name: $localize`Leader`, flexGrow: 2 },
      {
        prop: 'image_local_count',
        name: $localize`# Local`,
        headerTemplate: this.localTmpl,
        flexGrow: 2
      },
      {
        prop: 'image_remote_count',
        name: $localize`# Remote`,
        headerTemplate: this.remoteTmpl,
        flexGrow: 2
      },
      {
        prop: 'health',
        name: $localize`Health`,
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      this.data = data.content_data.pools;
      this.tableStatus = new TableStatusViewCache(data.status);
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }

  editModeModal() {
    this.router.navigate([
      BASE_URL,
      { outlets: { modal: [URLVerbs.EDIT, this.selection.first().name] } }
    ]);
  }

  editPeersModal(mode: string) {
    const initialState = {
      poolName: this.selection.first().name,
      mode: mode
    };
    this.modalService.show(PoolEditPeerModalComponent, initialState);
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

  setExpandedRow(expandedRow: any) {
    this.expandedRow = expandedRow;
  }
}
