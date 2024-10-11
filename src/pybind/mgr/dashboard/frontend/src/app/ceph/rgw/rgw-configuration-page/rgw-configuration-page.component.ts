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
import { rgwBucketEncryptionModel } from '../models/rgw-bucket-encryption';
import { TableComponent } from '~/app/shared/datatable/table/table.component';

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
  disableCreate = false;
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

  getBackend(encryptionData: { [x: string]: any[] }, encryptionType: string) {
    return new Set(encryptionData[encryptionType].map((item) => item.backend));
  }

  areAllAllowedBackendsPresent(allowedBackends: any[], backendsSet: Set<any>) {
    return allowedBackends.every((backend) => backendsSet.has(backend));
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

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
  }

  fetchData() {
    this.rgwBucketService.getEncryptionConfig().subscribe((data: any) => {
      this.allEncryptionValues = data;
      const allowedBackends = rgwBucketEncryptionModel.kmsProviders;

      const kmsBackends = this.getBackend(data, 'SSE_KMS');
      const s3Backends = this.getBackend(data, 'SSE_S3');

      const allKmsBackendsPresent = this.areAllAllowedBackendsPresent(allowedBackends, kmsBackends);
      const allS3BackendsPresent = this.areAllAllowedBackendsPresent(allowedBackends, s3Backends);

      this.disableCreate = allKmsBackendsPresent && allS3BackendsPresent;
      this.encryptionConfigValues = Object.values(data).flat();
    });
  }
}
