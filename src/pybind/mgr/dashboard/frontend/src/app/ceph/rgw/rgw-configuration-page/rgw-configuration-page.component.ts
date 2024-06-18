import { Component, EventEmitter, OnInit, Output } from '@angular/core';

import { NgbActiveModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { Permissions } from '~/app/shared/models/permissions';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalService } from '~/app/shared/services/modal.service';
import { RgwConfigModalComponent } from '../rgw-config-modal/rgw-config-modal.component';

@Component({
  selector: 'cd-rgw-configuration-page',
  templateUrl: './rgw-configuration-page.component.html',
  styleUrls: ['./rgw-configuration-page.component.scss']
})
export class RgwConfigurationPageComponent extends ListWithDetails implements OnInit {
  readonly vaultAddress = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{4}$/;

  kmsProviders: string[];

  columns: Array<CdTableColumn> = [];

  configForm: CdFormGroup;
  permissions: Permissions;
  encryptionConfigValues: any = [];
  selection: CdTableSelection = new CdTableSelection();

  @Output()
  submitAction = new EventEmitter();
  authMethods: string[];
  secretEngines: string[];
  tableActions: CdTableAction[];
  bsModalRef: NgbModalRef;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService,
    public authStorageService: AuthStorageService,
    private modalService: ModalService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Encryption Type`,
        prop: 'encryption_type',
        flexGrow: 1
      },
      {
        name: $localize`Key Management Service Provider`,
        prop: 'backend',
        flexGrow: 1
      },
      {
        name: $localize`Authentication Method`,
        prop: 'auth',
        flexGrow: 1
      },
      {
        name: $localize`Secret Engine`,
        prop: 'secret_engine',
        flexGrow: 1
      },
      {
        name: $localize`Secret Path`,
        prop: 'prefix',
        flexGrow: 1
      },
      {
        name: $localize`Address`,
        prop: 'addr',
        flexGrow: 1
      }
    ];
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE,
      click: () => this.openRgwConfigModal()
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      name: this.actionLabels.EDIT,
      click: () => this.openRgwConfigModal(true)
    };
    this.tableActions = [addAction, editAction];
    this.rgwBucketService.getEncryptionConfig().subscribe((data: any) => {
      this.encryptionConfigValues = data;
    });
  }

  openRgwConfigModal(edit?: boolean) {
    if (edit) {
      const initialState = {
        editing: true,
        encryptionConfigValues: this.selection.first()
      };
      this.bsModalRef = this.modalService.show(RgwConfigModalComponent, initialState, {
        size: 'lg'
      });
    } else {
      this.bsModalRef = this.modalService.show(RgwConfigModalComponent, {
        size: 'lg'
      });
    }
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
  }
}
