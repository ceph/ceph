import { Component, OnDestroy, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { Subscription } from 'rxjs';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { Icons } from '../../../../shared/enum/icons.enum';
import { ViewCacheStatus } from '../../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permission } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { Pool } from '../../../pool/pool';
import { BootstrapCreateModalComponent } from '../bootstrap-create-modal/bootstrap-create-modal.component';
import { BootstrapImportModalComponent } from '../bootstrap-import-modal/bootstrap-import-modal.component';
import { EditSiteNameModalComponent } from '../edit-site-name-modal/edit-site-name-modal.component';

@Component({
  selector: 'cd-mirroring',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.scss']
})
export class OverviewComponent implements OnInit, OnDestroy {
  permission: Permission;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  modalRef: BsModalRef;
  peersExist = true;
  siteName: any;
  status: ViewCacheStatus;
  private subs = new Subscription();

  constructor(
    private authStorageService: AuthStorageService,
    private rbdMirroringService: RbdMirroringService,
    private modalService: BsModalService,
    private i18n: I18n
  ) {
    this.permission = this.authStorageService.getPermissions().rbdMirroring;

    const editSiteNameAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      click: () => this.editSiteNameModal(),
      name: this.i18n('Edit Site Name'),
      canBePrimary: () => true,
      disable: () => false
    };
    const createBootstrapAction: CdTableAction = {
      permission: 'update',
      icon: Icons.upload,
      click: () => this.createBootstrapModal(),
      name: this.i18n('Create Bootstrap Token'),
      disable: () => false
    };
    const importBootstrapAction: CdTableAction = {
      permission: 'update',
      icon: Icons.download,
      click: () => this.importBootstrapModal(),
      name: this.i18n('Import Bootstrap Token'),
      disable: () => this.peersExist
    };
    this.tableActions = [editSiteNameAction, createBootstrapAction, importBootstrapAction];
  }

  ngOnInit() {
    this.subs.add(this.rbdMirroringService.startPolling());
    this.subs.add(
      this.rbdMirroringService.subscribeSummary((data) => {
        this.status = data.content_data.status;
        this.siteName = data.site_name;

        this.peersExist = !!data.content_data.pools.find((o: Pool) => o['peer_uuids'].length > 0);
      })
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  editSiteNameModal() {
    const initialState = {
      siteName: this.siteName
    };
    this.modalRef = this.modalService.show(EditSiteNameModalComponent, { initialState });
  }

  createBootstrapModal() {
    const initialState = {
      siteName: this.siteName
    };
    this.modalRef = this.modalService.show(BootstrapCreateModalComponent, { initialState });
  }

  importBootstrapModal() {
    const initialState = {
      siteName: this.siteName
    };
    this.modalRef = this.modalService.show(BootstrapImportModalComponent, { initialState });
  }
}
