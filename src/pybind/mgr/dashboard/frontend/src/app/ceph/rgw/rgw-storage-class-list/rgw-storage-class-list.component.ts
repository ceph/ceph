import { Component, OnInit } from '@angular/core';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import {
  StorageClass,
  CLOUD_TIER,
  ZoneGroup,
  TierTarget,
  Target,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';

@Component({
  selector: 'cd-rgw-storage-class-list',
  templateUrl: './rgw-storage-class-list.component.html',
  styleUrls: ['./rgw-storage-class-list.component.scss']
})
export class RgwStorageClassListComponent extends ListWithDetails implements OnInit {
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  storageClassList: StorageClass[] = [];

  constructor(private rgwZonegroupService: RgwZonegroupService) {
    super();
  }

  ngOnInit() {
    this.columns = [
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
        name: $localize`Storage Class`,
        prop: 'storage_class',
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
  }

  loadStorageClass(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.rgwZonegroupService.getAllZonegroupsInfo().subscribe(
        (data: ZoneGroupDetails) => {
          this.storageClassList = [];

          const tierObj = data.zonegroups.flatMap((zoneGroup: ZoneGroup) =>
            zoneGroup.placement_targets
              .filter((target: Target) => target.tier_targets)
              .flatMap((target: Target) =>
                target.tier_targets
                  .filter((tierTarget: TierTarget) => tierTarget.val.tier_type === CLOUD_TIER)
                  .map((tierTarget: TierTarget) => {
                    return this.getTierTargets(tierTarget, zoneGroup.name, target.name);
                  })
              )
          );
          this.storageClassList.push(...tierObj);
          resolve();
        },
        (error) => {
          reject(error);
        }
      );
    });
  }

  getTierTargets(tierTarget: TierTarget, zoneGroup: string, targetName: string) {
    if (tierTarget.val.tier_type !== CLOUD_TIER) return null;
    return {
      zonegroup_name: zoneGroup,
      placement_target: targetName,
      storage_class: tierTarget.val.storage_class,
      ...tierTarget.val.s3
    };
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
  }
}
