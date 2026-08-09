import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';
import { ViewCacheStatus } from '~/app/shared/enum/view-cache-status.enum';
import { CephfsDetail } from '~/app/shared/models/cephfs.model';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CephfsResourceStateService } from '~/app/shared/services/cephfs-resource-state.service';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';

@Component({
  selector: 'cd-cephfs-resource-page',
  templateUrl: './cephfs-resource-page.component.html',
  styleUrls: ['./cephfs-resource-page.component.scss'],
  standalone: false
})
export class CephfsResourcePageComponent implements OnInit, OnDestroy {
  @ViewChild('poolUsageTpl', { static: true })
  poolUsageTpl: TemplateRef<any>;
  @ViewChild('activityTmpl', { static: true })
  activityTmpl: TemplateRef<any>;

  private sub = new Subscription();
  private tabsSub = new Subscription();

  section = '';
  id = 0;
  fsName = '';
  selection: CephfsDetail | null = null;
  notFound = false;
  overviewFields: OverviewField[] = [];
  permissions: any;
  columns: {
    ranks: CdTableColumn[];
    pools: CdTableColumn[];
  };
  standbys: any[] = [];

  objectValues = Object.values;

  clients: Record<string, any> = {
    data: [],
    status: new TableStatusViewCache(ViewCacheStatus.ValueNone)
  };

  details: Record<string, any> = {
    standbys: '',
    pools: [],
    ranks: [],
    mdsCounters: {},
    name: ''
  };

  constructor(
    private route: ActivatedRoute,
    private authStorageService: AuthStorageService,
    private cephfsService: CephfsService,
    private cephfsResourceStateService: CephfsResourceStateService,
    private cdDatePipe: CdDatePipe,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.sub.add(
      this.route.data.subscribe((data) => {
        this.section = data['section'] ?? 'overview';
      })
    );

    const parentRoute = this.route.parent;
    if (parentRoute) {
      this.sub.add(
        parentRoute.paramMap.subscribe((pm: ParamMap) => {
          this.id = Number(pm.get('id') ?? 0);
        })
      );
    }

    this.sub.add(
      this.cephfsResourceStateService.filesystem$.subscribe((filesystem) => {
        this.applyFilesystem(filesystem);
      })
    );

    this.columns = {
      ranks: [
        { prop: 'rank', name: $localize`Rank` },
        { prop: 'state', name: $localize`State` },
        { prop: 'mds', name: $localize`Daemon` },
        { prop: 'activity', name: $localize`Activity`, cellTemplate: this.activityTmpl },
        { prop: 'dns', name: $localize`Dentries`, pipe: this.dimless },
        { prop: 'inos', name: $localize`Inodes`, pipe: this.dimless },
        { prop: 'dirs', name: $localize`Dirs`, pipe: this.dimless },
        { prop: 'caps', name: $localize`Caps`, pipe: this.dimless }
      ],
      pools: [
        { prop: 'pool', name: $localize`Pool` },
        { prop: 'type', name: $localize`Type` },
        { prop: 'size', name: $localize`Size`, pipe: this.dimlessBinary },
        {
          name: $localize`Usage`,
          cellTemplate: this.poolUsageTpl,
          comparator: (_valueA: any, _valueB: any, rowA: any, rowB: any) => {
            const valA = rowA.used / rowA.avail;
            const valB = rowB.used / rowB.avail;

            if (valA === valB) {
              return 0;
            }

            if (valA > valB) {
              return 1;
            } else {
              return -1;
            }
          }
        } as CdTableColumn
      ]
    };
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
    this.tabsSub.unsubscribe();
  }

  refresh(): void {
    if (!this.id) {
      return;
    }

    this.loadTabs(this.id);
  }

  private applyFilesystem(filesystem: CephfsDetail | null): void {
    this.selection = filesystem;
    this.notFound = !filesystem;

    if (!filesystem) {
      this.fsName = '';
      this.overviewFields = [];
      this.details = this.getDefaultDetails();
      this.standbys = [];
      this.clients = {
        data: [],
        status: new TableStatusViewCache(ViewCacheStatus.ValueNone)
      };
      return;
    }

    this.fsName = filesystem.mdsmap?.fs_name || filesystem.cephfs?.name || '';
    this.overviewFields = this.buildOverviewFields(filesystem);
    this.loadTabs(filesystem.id || this.id);
  }

  private loadTabs(id: number): void {
    this.tabsSub.unsubscribe();
    this.tabsSub = new Subscription();

    this.tabsSub.add(
      this.cephfsService.getTabs(id).subscribe(
        (data: any) => {
          this.details = this.buildDetails(data);
          this.setStandbys();
          this.clients = data?.clients;
          this.clients.status = new TableStatusViewCache(this.clients?.status);
        },
        () => {
          this.clients.status = new TableStatusViewCache(ViewCacheStatus.ValueException);
        }
      )
    );
  }

  private buildDetails(data: any): Record<string, any> {
    const details = this.getDefaultDetails();
    details.standbys = data?.standbys;
    details.pools = data?.pools;
    details.ranks = data?.ranks;
    details.mdsCounters = data?.mds_counters ?? {};
    details.name = data?.name || this.fsName;
    return details;
  }

  private getDefaultDetails(): Record<string, any> {
    return {
      standbys: '',
      pools: [],
      ranks: [],
      mdsCounters: {},
      name: ''
    };
  }

  private buildOverviewFields(filesystem: CephfsDetail): OverviewField[] {
    const fsName = filesystem.mdsmap?.fs_name || filesystem.cephfs?.name || '';
    const enabled = this.getEnabledState(filesystem);

    const created = filesystem.mdsmap?.created;

    return [
      { label: $localize`Name`, value: fsName },
      {
        label: $localize`Enabled`,
        value: enabled ? $localize`Enabled` : $localize`Disabled`,
        type: 'status',
        status: enabled ? 'success' : 'danger'
      },
      {
        label: $localize`Created`,
        value: created ? this.cdDatePipe.transform(created) : '-'
      }
    ];
  }

  private getEnabledState(filesystem: CephfsDetail): boolean {
    return !!filesystem?.mdsmap?.enabled;
  }

  private setStandbys(): void {
    this.standbys = [
      {
        key: $localize`Standby daemons`,
        value: this.details.standbys
      }
    ];
  }

  trackByFn(_index: any, item: any) {
    return item.name;
  }
}
