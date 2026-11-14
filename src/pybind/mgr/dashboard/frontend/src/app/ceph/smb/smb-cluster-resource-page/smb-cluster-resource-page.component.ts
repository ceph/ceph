import { Component, inject, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { SMB_CONTEXT } from '~/app/shared/constants/smb.constant';
import { SmbClusterResourceStateService } from '~/app/shared/services/smb-cluster-resource-state.service';
import { CLIENT_COMPAT, SMBCluster } from '../smb.model';

@Component({
  selector: 'cd-smb-cluster-resource-page',
  templateUrl: './smb-cluster-resource-page.component.html',
  styleUrls: ['./smb-cluster-resource-page.component.scss'],
  standalone: false
})
export class SmbClusterResourcePageComponent implements OnInit, OnDestroy {
  private readonly ctx = inject(SMB_CONTEXT);
  private sub = new Subscription();

  section = '';
  clusterId = '';
  selection: SMBCluster | undefined;
  loadError = false;
  isOverviewLoading = false;
  overviewFields: OverviewField[] = [];

  constructor(
    private route: ActivatedRoute,
    private smbClusterResourceStateService: SmbClusterResourceStateService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.data.subscribe((data) => {
        this.section = data['section'] ?? 'overview';
      })
    );

    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.clusterId = pm.get('cluster_id') ?? '';
        this.isOverviewLoading = !!this.clusterId;
        this.loadError = false;
      })
    );

    this.sub.add(
      this.smbClusterResourceStateService.cluster$.subscribe((cluster: SMBCluster | null) => {
        this.applyCluster(cluster);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyCluster(cluster: SMBCluster | null): void {
    this.isOverviewLoading = false;
    if (!this.clusterId) {
      this.selection = undefined;
      this.loadError = false;
      this.overviewFields = [];
      return;
    }

    this.selection = cluster || undefined;
    this.loadError = !cluster;
    this.overviewFields = cluster ? this.buildOverviewFields(cluster) : [];
  }

  private buildOverviewFields(cluster: SMBCluster): OverviewField[] {
    const fields: OverviewField[] = [
      {
        label: $localize`Name`,
        value: cluster.cluster_id
      },
      {
        label: $localize`Authentication Mode`,
        value: cluster.auth_mode
      }
    ];
    if (this.ctx.showMacOsCompat) {
      fields.push({
        label: $localize`Compatibility`,
        value: cluster.client_compat || CLIENT_COMPAT.Default
      });
    }
    return fields;
  }
}
