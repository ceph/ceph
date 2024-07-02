import { TitleCasePipe } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';

const BASE_URL = 'rgw/multisite/sync-policy';

@Component({
  selector: 'cd-rgw-multisite-sync-policy',
  templateUrl: './rgw-multisite-sync-policy.component.html',
  styleUrls: ['./rgw-multisite-sync-policy.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwMultisiteSyncPolicyComponent implements OnInit {
  columns: Array<CdTableColumn> = [];
  syncPolicyData: any = [];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;

  constructor(
    private rgwMultisiteService: RgwMultisiteService,
    private titleCasePipe: TitleCasePipe,
    private actionLabels: ActionLabelsI18n,
    private urlBuilder: URLBuilderService,
    private authStorageService: AuthStorageService
  ) {}

  ngOnInit(): void {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: $localize`Group Name`,
        prop: 'groupName',
        flexGrow: 1
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 1,
        cellTransformation: CellTemplate.tooltip,
        customTemplateConfig: {
          map: {
            Enabled: { class: 'badge-success', tooltip: 'sync is allowed and enabled' },
            Allowed: { class: 'badge-info', tooltip: 'sync is allowed' },
            Forbidden: {
              class: 'badge-warning',
              tooltip:
                'sync (as defined by this group) is not allowed and can override other groups'
            }
          }
        },
        pipe: this.titleCasePipe
      },
      {
        name: $localize`Zonegroup`,
        prop: 'zonegroup',
        flexGrow: 1
      },
      {
        name: $localize`Bucket`,
        prop: 'bucket',
        flexGrow: 1
      }
    ];
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE
    };
    this.tableActions = [addAction];
    this.rgwMultisiteService
      .getSyncPolicy('', '', true)
      .subscribe((allSyncPolicyData: Array<Object>) => {
        if (allSyncPolicyData && allSyncPolicyData.length > 0) {
          allSyncPolicyData.forEach((policy) => {
            this.syncPolicyData.push({
              groupName: policy['id'],
              status: policy['status'],
              bucket: policy['bucketName'],
              zonegroup: ''
            });
          });
          this.syncPolicyData = [...this.syncPolicyData];
        }
      });
  }
}
