import { Component, inject, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
@Component({
  selector: 'cd-rgw-storage-class-resource-sidebar',
  templateUrl: './rgw-storage-class-resource-sidebar.component.html',
  styleUrls: ['./rgw-storage-class-resource-sidebar.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwStorageClassResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  readonly basePath = '/rgw/storage-class';
  isResourcePage = false;
  storageClassTitle = '';
  zonegroupName = '';
  placementTarget = '';
  sidebarItems: SidebarItem[] = [];

  private route = inject(ActivatedRoute);

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
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
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
