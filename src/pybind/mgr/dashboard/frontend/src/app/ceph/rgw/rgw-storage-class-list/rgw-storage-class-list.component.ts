import { Component, OnInit } from '@angular/core';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { StorageClass, ZoneGroupDetails } from '../models/rgw-storage-class.model';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { RgwStorageClassService } from '~/app/shared/api/rgw-storage-class.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Permission } from '~/app/shared/models/permissions';
import { BucketTieringUtils } from '../utils/rgw-bucket-tiering';

import { Router } from '@angular/router';

const BASE_URL = 'rgw/tiering';
@Component({
  selector: 'cd-rgw-storage-class-list',
  templateUrl: './rgw-storage-class-list.component.html',
  styleUrls: ['./rgw-storage-class-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwStorageClassListComponent extends ListWithDetails implements OnInit {
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  permission: Permission;
  tableActions: CdTableAction[];
  storageClassList: StorageClass[] = [];

  constructor(
    private rgwZonegroupService: RgwZonegroupService,
    public actionLabels: ActionLabelsI18n,
    private cdsModalService: ModalCdsService,
    private taskWrapper: TaskWrapperService,
    private authStorageService: AuthStorageService,
    private rgwStorageClassService: RgwStorageClassService,
    private router: Router,
    private urlBuilder: URLBuilderService
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Storage Class`,
        prop: 'storage_class',
        flexGrow: 2
      },
      {
        name: $localize`Zone Group`,
        prop: 'zonegroup_name',
        flexGrow: 2
      },
      {
        name: $localize`Placement Target`,
        prop: 'placement_target',
        flexGrow: 2
      },
      {
        name: $localize`Target Region`,
        prop: 'region',
        flexGrow: 2
      },
      {
        name: $localize`Target Endpoint`,
        prop: 'endpoint',
        flexGrow: 2
      }
    ];
    const getStorageUri = () =>
      this.selection.first() &&
      `${encodeURI(this.selection.first().zonegroup_name)}/${encodeURI(
        this.selection.first().placement_target
      )}/${encodeURI(this.selection.first().storage_class)}`;
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.router.navigate([this.urlBuilder.getCreate()]),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        routerLink: () => [`/rgw/tiering/edit/${getStorageUri()}`]
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeStorageClassModal()
      }
    ];
  }

  loadStorageClass(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.rgwZonegroupService.getAllZonegroupsInfo().subscribe(
        (data: ZoneGroupDetails) => {
          this.storageClassList = [];
          const tierObj = BucketTieringUtils.filterAndMapTierTargets(data);
          this.storageClassList.push(...tierObj);
          resolve();
        },
        (error) => {
          reject(error);
        }
      );
    });
  }

  removeStorageClassModal() {
    const storage_class = this.selection.first().storage_class;
    const placement_target = this.selection.first().placement_target;
    this.cdsModalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Tiering Storage Class`,
      itemNames: [storage_class],
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('rgw/zonegroup/storage-class', {
            placement_target: placement_target,
            storage_class: storage_class
          }),
          call: this.rgwStorageClassService.removeStorageClass(placement_target, storage_class)
        })
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
  }
}
