import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { HttpParams } from '@angular/common/http';
import { Subscription, forkJoin } from 'rxjs';

import { OsdService } from '~/app/shared/api/osd.service';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { Osd } from '~/app/shared/models/osd.model';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-osd-resource-sidebar',
  templateUrl: './osd-resource-sidebar.component.html',
  styleUrls: ['./osd-resource-sidebar.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class OsdResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  public readonly basePath = '/osd/view';
  private readonly disabledFlags: string[] = [
    'sortbitwise',
    'purged_snapdirs',
    'recovery_deletes',
    'pglog_hardlimit'
  ];

  osdId = '';
  sidebarItems: SidebarItem[] = [];
  headerTags: string[] = [];
  permissions: Permissions;

  constructor(
    private route: ActivatedRoute,
    private authStorageService: AuthStorageService,
    private osdService: OsdService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.osdId = pm.get('id') ?? '';
        this.buildSidebarItems(this.permissions);
        this.loadHeaderTags();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  get title(): string {
    return this.osdId ? `OSD ${this.osdId}` : '';
  }

  private buildSidebarItems(permissions: Permissions): void {
    const items: SidebarItem[] = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.osdId, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Storage Devices`,
        route: [this.basePath, this.osdId, 'storage-devices'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Attributes`,
        route: [this.basePath, this.osdId, 'attributes'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Performance Counters`,
        route: [this.basePath, this.osdId, 'performance-counters'],
        routerLinkActiveOptions: { exact: true }
      }
    ];

    if (permissions.grafana?.read) {
      items.push({
        label: $localize`Performance`,
        route: [this.basePath, this.osdId, 'performance'],
        routerLinkActiveOptions: { exact: true }
      });
    }

    this.sidebarItems = items;
  }

  private loadHeaderTags(): void {
    const parsedId = Number(this.osdId);
    if (!Number.isFinite(parsedId)) {
      this.headerTags = [];
      return;
    }

    const params = new HttpParams().set('offset', '0').set('limit', '10000').set('sort', '+id');

    this.sub.add(
      forkJoin([this.osdService.getList(params).observable, this.osdService.getFlags()]).subscribe(
        ([osds, clusterFlags]) => {
          const clusterFlagList = Array.isArray(clusterFlags) ? clusterFlags : [];
          const osdList = Array.isArray(osds) ? osds : [];
          const selectedOsd = osdList.find((item: Osd) => item.id === parsedId);
          if (!selectedOsd) {
            this.headerTags = [];
            return;
          }

          const statusTags = this.collectStates(selectedOsd);
          const classTag = selectedOsd.tree?.device_class;
          const indivStateFlags: string[] = Array.isArray(selectedOsd?.state)
            ? selectedOsd.state
            : [];
          const indivFlags: string[] = [];
          for (const flag of indivStateFlags) {
            if (flag === 'noup' || flag === 'nodown' || flag === 'noin' || flag === 'noout') {
              indivFlags.push(flag);
            }
          }

          const clusterWideFlags: string[] = [];
          for (const flag of clusterFlagList) {
            if (!this.disabledFlags.includes(flag)) {
              clusterWideFlags.push(flag);
            }
          }

          this.headerTags = [
            ...(classTag ? [classTag] : []),
            ...statusTags,
            ...clusterWideFlags,
            ...indivFlags
          ];
        }
      )
    );
  }

  private collectStates(osd: Osd): string[] {
    const states = [osd?.['in'] ? 'in' : 'out'];
    if (osd?.['up']) {
      states.push('up');
    } else if (Array.isArray(osd?.state) && osd.state.indexOf('destroyed') !== -1) {
      states.push('destroyed');
    } else {
      states.push('down');
    }
    return states;
  }
}
