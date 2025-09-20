import { Component, EventEmitter, OnInit, Output, ViewChild } from '@angular/core';

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
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { ENCRYPTION_TYPE } from '../models/rgw-bucket-encryption';
import {
  KmipConfig,
  VaultConfig,
  encryptionDatafromAPI
} from '~/app/shared/models/rgw-encryption-config-keys';

@Component({
  selector: 'cd-rgw-configuration-page',
  templateUrl: './rgw-configuration-page.component.html',
  styleUrls: ['./rgw-configuration-page.component.scss']
})
export class RgwConfigurationPageComponent extends ListWithDetails implements OnInit {
  readonly vaultAddress = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{4}$/;
  @ViewChild(TableComponent)
  table: TableComponent;

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
  filteredEncryptionConfigValues: {};
  excludeProps: any[] = [];
  disableCreate = true;
  allEncryptionValues: any;

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
        name: $localize`Address`,
        prop: 'addr',
        flexGrow: 1
      }
    ];
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        name: this.actionLabels.CREATE,
        click: () => this.openRgwConfigModal(false),
        disable: () => this.disableCreate
      },
      {
        permission: 'update',
        icon: Icons.edit,
        name: this.actionLabels.EDIT,
        click: () => this.openRgwConfigModal(true)
      }
    ];

    this.excludeProps = this.columns.map((column) => column.prop);
    this.excludeProps.push('unique_id');
  }

  getBackend(encryptionData: encryptionDatafromAPI, encryptionType: ENCRYPTION_TYPE) {
    const backendSet = new Set(Object.keys(encryptionData[encryptionType]));
    const result =
      encryptionType === ENCRYPTION_TYPE.SSE_KMS
        ? backendSet.has('kmip') && backendSet.has('vault')
        : backendSet.has('vault');
    return result;
  }

  openRgwConfigModal(edit: boolean) {
    if (edit) {
      const initialState = {
        action: 'edit',
        editing: true,
        selectedEncryptionConfigValues: this.selection.first(),
        table: this.table
      };
      this.bsModalRef = this.modalService.show(RgwConfigModalComponent, initialState, {
        size: 'lg'
      });
    } else {
      const initialState = {
        action: 'create',
        allEncryptionConfigValues: this.allEncryptionValues
      };
      this.bsModalRef = this.modalService.show(RgwConfigModalComponent, initialState, {
        size: 'lg'
      });
    }
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  setExpandedRow(expandedRow: VaultConfig | KmipConfig) {
    super.setExpandedRow(expandedRow);
  }

  flattenData(data: encryptionDatafromAPI) {
    const combinedArray = [];
    for (const kmsData of Object.values(data[ENCRYPTION_TYPE.SSE_KMS])) {
      combinedArray.push(kmsData);
    }
    for (const s3Data of Object.values(data[ENCRYPTION_TYPE.SSE_S3])) {
      combinedArray.push(s3Data);
    }
    return combinedArray;
  }

  fetchData() {
    this.rgwBucketService.getEncryptionConfig().subscribe((data: encryptionDatafromAPI) => {
      this.allEncryptionValues = data;
      const kmsBackends = this.getBackend(data, ENCRYPTION_TYPE.SSE_KMS);
      const s3Backends = this.getBackend(data, ENCRYPTION_TYPE.SSE_S3);
      this.disableCreate = kmsBackends && s3Backends;
      this.encryptionConfigValues = this.flattenData(data);
    });
  }
}
