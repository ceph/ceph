import { Component, DestroyRef, inject, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, ParamMap, Router } from '@angular/router';

import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NFSCluster } from '../models/nfs-cluster-config';
import { getFsalFromRoute, getPathfromFsal } from '../utils';

@Component({
  selector: 'cd-nfs-cluster-resource-sidebar',
  templateUrl: './nfs-cluster-resource-sidebar.component.html',
  styleUrls: ['./nfs-cluster-resource-sidebar.component.scss'],
  providers: [NfsClusterResourceStateService],
  standalone: false
})
export class NfsClusterResourceSidebarComponent implements OnInit {
  private destroyRef = inject(DestroyRef);

  clusterId = '';
  clusterName = '';
  selection: NFSCluster | undefined;
  sidebarItems: SidebarItem[] = [];

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private nfsClusterResourceStateService: NfsClusterResourceStateService
  ) {}

  ngOnInit(): void {
    this.nfsClusterResourceStateService.cluster$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((cluster: NFSCluster | null) => {
        this.selection = cluster || undefined;
        this.clusterName = cluster?.name || this.clusterId;
      });

    this.route.paramMap.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((pm: ParamMap) => {
      this.clusterId = pm.get('cluster_id') ?? '';
      this.clusterName = this.clusterId;
      this.buildSidebarItems();
      this.nfsClusterResourceStateService.load(this.clusterId);
    });
  }

  private buildSidebarItems(): void {
    const nfsBasePath = `/${getPathfromFsal(getFsalFromRoute(this.router.url))}/nfs/cluster`;
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [nfsBasePath, this.clusterId, 'overview'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}
