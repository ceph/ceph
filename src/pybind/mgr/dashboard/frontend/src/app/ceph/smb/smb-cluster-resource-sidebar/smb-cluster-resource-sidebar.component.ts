import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { SmbClusterResourceStateService } from '~/app/shared/services/smb-cluster-resource-state.service';
import { SMBCluster } from '../smb.model';

@Component({
  selector: 'cd-smb-cluster-resource-sidebar',
  templateUrl: './smb-cluster-resource-sidebar.component.html',
  styleUrls: ['./smb-cluster-resource-sidebar.component.scss'],
  providers: [SmbClusterResourceStateService],
  standalone: false
})
export class SmbClusterResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  clusterId = '';
  clusterName = '';
  selection: SMBCluster | undefined;
  sidebarItems: SidebarItem[] = [];

  constructor(
    private route: ActivatedRoute,
    private smbClusterResourceStateService: SmbClusterResourceStateService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.smbClusterResourceStateService.cluster$.subscribe((cluster: SMBCluster | null) => {
        this.selection = cluster || undefined;
        this.clusterName = cluster?.cluster_id || this.clusterId;
      })
    );

    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.clusterId = pm.get('cluster_id') ?? '';
        this.clusterName = this.clusterId;
        this.buildSidebarItems();
        this.smbClusterResourceStateService.load(this.clusterId);
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
        route: ['/cephfs/smb/cluster', this.clusterId, 'overview'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}
