import {
  Component,
  inject,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  SimpleChanges,
  ViewEncapsulation
} from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import {
  ALLOW_READ_THROUGH_TEXT,
  HOST_STYLE,
  MULTIPART_MIN_PART_TEXT,
  MULTIPART_SYNC_THRESHOLD_TEXT,
  RETAIN_HEAD_OBJECT_TEXT,
  StorageClassDetails,
  TARGET_ACCESS_KEY_TEXT,
  TARGET_PATH_TEXT,
  TARGET_SECRET_KEY_TEXT,
  TIER_TYPE_DISPLAY,
  TIER_TYPE,
  GLACIER_RESTORE_DAY_TEXT,
  GLACIER_RESTORE_TIER_TYPE_TEXT,
  RESTORE_DAYS_TEXT,
  READTHROUGH_RESTORE_DAYS_TEXT,
  RESTORE_STORAGE_CLASS_TEXT,
  ZONEGROUP_TEXT,
  ACL,
  GroupedACLs,
  AllZonesResponse
} from '../models/rgw-storage-class.model';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { BucketTieringUtils } from '../utils/rgw-bucket-tiering';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
@Component({
  selector: 'cd-rgw-storage-class-resource-sidebar',
  templateUrl: './rgw-storage-class-resource-sidebar.component.html',
  styleUrls: ['./rgw-storage-class-resource-sidebar.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwStorageClassResourceSidebarComponent implements OnChanges, OnInit, OnDestroy {
  @Input()
  selection!: StorageClassDetails;
  private sub = new Subscription();
  readonly basePath = '/rgw/storage-class';
  isResourcePage = false;
  storageClassTitle = '';
  zonegroupName = '';
  placementTarget = '';
  sidebarItems: SidebarItem[] = [];
  columns: CdTableColumn[] = [];
  allowReadThroughText = ALLOW_READ_THROUGH_TEXT;
  retainHeadObjectText = RETAIN_HEAD_OBJECT_TEXT;
  multipartMinPartText = MULTIPART_MIN_PART_TEXT;
  multipartSyncThreholdText = MULTIPART_SYNC_THRESHOLD_TEXT;
  targetAccessKeyText = TARGET_ACCESS_KEY_TEXT;
  targetSecretKeyText = TARGET_SECRET_KEY_TEXT;
  targetPathText = TARGET_PATH_TEXT;
  hostStyleText = HOST_STYLE;
  TIER_TYPE_DISPLAY = TIER_TYPE_DISPLAY;
  TIER_TYPE = TIER_TYPE;
  glacierRestoreDayText = GLACIER_RESTORE_DAY_TEXT;
  glacierRestoreTiertypeText = GLACIER_RESTORE_TIER_TYPE_TEXT;
  restoreDaysText = RESTORE_DAYS_TEXT;
  readthroughrestoreDaysText = READTHROUGH_RESTORE_DAYS_TEXT;
  restoreStorageClassText = RESTORE_STORAGE_CLASS_TEXT;
  zoneGroupText = ZONEGROUP_TEXT;
  groupedACLs: GroupedACLs = {};
  localStorageClassDetails = { zone_name: '', data_pool: '' };
  loading = false;

  private rgwZoneService = inject(RgwZoneService);
  private route = inject(ActivatedRoute);

  ngOnChanges(changes: SimpleChanges): void {
    if (
      changes['selection'] &&
      changes['selection'].currentValue?.tier_type?.toLowerCase() === TIER_TYPE.LOCAL &&
      changes['selection'].firstChange
    ) {
      // The idea here is to not call the API if we already have the zone_name and data_pool
      // When the details view is expanded and table refreshes data then this API should not be called again
      const { zone_name, data_pool } = this.localStorageClassDetails;
      if (!zone_name || !data_pool) {
        this.getZoneInfo();
      }
    }
  }

  ngOnInit() {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.zonegroupName = pm.get('zonegroup_name') ?? '';
        this.placementTarget = pm.get('placement_target') ?? '';
        this.storageClassTitle = pm.get('storage_class') ?? '';
        this.isResourcePage =
          !!this.zonegroupName && !!this.placementTarget && !!this.storageClassTitle;

        if (this.isResourcePage) {
          this.buildSidebarItems();
        }
      })
    );

    this.groupedACLs = this.groupByType(this.selection?.acl_mappings);
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  isTierMatch(...types: string[]): boolean {
    const tier_type = this.selection?.tier_type?.toLowerCase();
    return types.some((type) => type.toLowerCase() === tier_type);
  }

  getZoneInfo() {
    this.loading = true;
    this.rgwZoneService.getAllZonesInfo().subscribe(
      (data: object) => {
        const zones = (data as AllZonesResponse)?.zones ?? [];
        this.localStorageClassDetails = BucketTieringUtils.getZoneInfoHelper(zones, this.selection);
        this.loading = false;
      },
      () => {
        this.loading = false;
      }
    );
  }

  groupByType(acls?: ACL[]): GroupedACLs {
    return (acls || []).reduce((groupAcls: GroupedACLs, item: ACL) => {
      const type = item.val?.type?.toUpperCase();
      groupAcls[type] = groupAcls[type] ?? [];
      groupAcls[type].push({
        source_id: item.val?.source_id,
        dest_id: item.val?.dest_id
      });
      return groupAcls;
    }, {});
  }

  private buildSidebarItems(): void {
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [
          this.basePath,
          this.zonegroupName,
          this.placementTarget,
          this.storageClassTitle,
          'overview'
        ],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Policy`,
        route: [
          this.basePath,
          this.zonegroupName,
          this.placementTarget,
          this.storageClassTitle,
          'policy'
        ],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}
