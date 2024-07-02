import { TitleCasePipe } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';

@Component({
  selector: 'cd-rgw-multisite-sync-policy',
  templateUrl: './rgw-multisite-sync-policy.component.html',
  styleUrls: ['./rgw-multisite-sync-policy.component.scss']
})
export class RgwMultisiteSyncPolicyComponent implements OnInit {
  columns: Array<CdTableColumn> = [];
  syncPolicyData: any = [];

  constructor(
    private rgwMultisiteService: RgwMultisiteService,
    private titleCasePipe: TitleCasePipe
  ) {}

  ngOnInit(): void {
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
