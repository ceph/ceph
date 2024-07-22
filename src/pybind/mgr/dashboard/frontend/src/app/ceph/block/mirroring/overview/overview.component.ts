import { Component, OnDestroy, OnInit } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { Subscription } from 'rxjs';

import { Pool } from '~/app/ceph/pool/pool';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ViewCacheStatus } from '~/app/shared/enum/view-cache-status.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { BootstrapCreateModalComponent } from '../bootstrap-create-modal/bootstrap-create-modal.component';
import { BootstrapImportModalComponent } from '../bootstrap-import-modal/bootstrap-import-modal.component';

@Component({
  selector: 'cd-mirroring',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.scss']
})
export class OverviewComponent implements OnInit, OnDestroy {
  rbdmirroringForm: CdFormGroup;
  permission: Permission;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  modalRef: NgbModalRef;
  peersExist = true;
  siteName: any;
  status: ViewCacheStatus;
  private subs = new Subscription();
  editing = false;

  icons = Icons;

  constructor(
    private authStorageService: AuthStorageService,
    private rbdMirroringService: RbdMirroringService,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService
  ) {
    this.permission = this.authStorageService.getPermissions().rbdMirroring;

    const createBootstrapAction: CdTableAction = {
      permission: 'update',
      icon: Icons.upload,
      click: () => this.createBootstrapModal(),
      name: $localize`Create Bootstrap Token`,
      canBePrimary: () => true,
      disable: () => false
    };
    const importBootstrapAction: CdTableAction = {
      permission: 'update',
      icon: Icons.download,
      click: () => this.importBootstrapModal(),
      name: $localize`Import Bootstrap Token`,
      disable: () => false
    };
    this.tableActions = [createBootstrapAction, importBootstrapAction];
  }

  ngOnInit() {
    this.createForm();
    this.subs.add(this.rbdMirroringService.startPolling());
    this.subs.add(
      this.rbdMirroringService.subscribeSummary((data) => {
        this.status = data.content_data.status;
        this.peersExist = !!data.content_data.pools.find((o: Pool) => o['peer_uuids'].length > 0);
      })
    );
    this.rbdMirroringService.getSiteName().subscribe((response: any) => {
      this.siteName = response.site_name;
      this.rbdmirroringForm.get('siteName').setValue(this.siteName);
    });
  }

  private createForm() {
    this.rbdmirroringForm = new CdFormGroup({
      siteName: new UntypedFormControl({ value: '', disabled: true })
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  updateSiteName() {
    if (this.editing) {
      const action = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('rbd/mirroring/site_name/edit', {}),
        call: this.rbdMirroringService.setSiteName(this.rbdmirroringForm.getValue('siteName'))
      });

      action.subscribe({
        complete: () => {
          this.rbdMirroringService.refresh();
        }
      });
    }
    this.editing = !this.editing;
  }

  createBootstrapModal() {
    const initialState = {
      siteName: this.siteName
    };
    this.modalRef = this.modalService.show(BootstrapCreateModalComponent, initialState);
  }

  importBootstrapModal() {
    const initialState = {
      siteName: this.siteName
    };
    this.modalRef = this.modalService.show(BootstrapImportModalComponent, initialState);
  }
}
