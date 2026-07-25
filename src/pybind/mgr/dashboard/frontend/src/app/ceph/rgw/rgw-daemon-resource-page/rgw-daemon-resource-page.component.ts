import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';

import { RgwDaemon, RgwDaemonDetailsResponse } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';

@Component({
  selector: 'cd-rgw-daemon-resource-page',
  templateUrl: './rgw-daemon-resource-page.component.html',
  styleUrls: ['./rgw-daemon-resource-page.component.scss'],
  standalone: false
})
export class RgwDaemonResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = '';
  selection?: RgwDaemon;
  notFound = false;
  metadata: Record<string, unknown> = {};
  daemonDetailsFields: OverviewField[] = [];
  softwareVersionFields: OverviewField[] = [];
  distributionFields: OverviewField[] = [];
  frontendConfigurationFields: OverviewField[] = [];
  kernelOperatingSystemFields: OverviewField[] = [];

  constructor(
    private route: ActivatedRoute,
    private rgwDaemonService: RgwDaemonService
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';

    this.sub.add(
      this.route.parent?.data.subscribe((data) => {
        const daemon = (data?.daemon ?? null) as RgwDaemon | null;
        this.applyDaemon(daemon);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyDaemon(daemon: RgwDaemon | null): void {
    this.notFound = !daemon;
    if (!daemon) {
      this.selection = undefined;
      this.metadata = {};
      this.updateOverviewCards();
      return;
    }

    this.selection = daemon;
    this.metadata = {};
    this.updateOverviewCards();
    this.loadMetadata(daemon.id);
  }

  private loadMetadata(serviceId: string): void {
    if (!serviceId) {
      return;
    }

    this.rgwDaemonService.get(serviceId).subscribe({
      next: (resp: RgwDaemonDetailsResponse) => {
        this.metadata = resp?.rgw_metadata ?? {};
        this.updateOverviewCards();
      },
      error: () => {
        this.metadata = {};
        this.updateOverviewCards();
      }
    });
  }

  get totalMemoryKb(): number {
    return this.toNumber(this.getMetadataValue('mem_total_kb'));
  }

  get swapMemoryKb(): number {
    return this.toNumber(this.getMetadataValue('mem_swap_kb'));
  }

  private updateOverviewCards(): void {
    if (!this.selection) {
      this.daemonDetailsFields = [];
      this.softwareVersionFields = [];
      this.distributionFields = [];
      this.frontendConfigurationFields = [];
      this.kernelOperatingSystemFields = [];
      return;
    }

    const daemon = this.selection;

    this.daemonDetailsFields = [
      { label: $localize`Daemon ID`, value: daemon.id },
      { label: $localize`Hostname`, value: daemon.server_hostname },
      { label: $localize`Port`, value: daemon.port },
      {
        label: $localize`Realm Name`,
        value: this.getMetadataValue('realm_name') ?? daemon.realm_name
      },
      { label: $localize`Realm ID`, value: this.getMetadataValue('realm_id') },
      {
        label: $localize`Zonegroup Name`,
        value: this.getMetadataValue('zonegroup_name') ?? daemon.zonegroup_name
      },
      { label: $localize`Zonegroup ID`, value: daemon.zonegroup_id },

      {
        label: $localize`Zone Name`,
        value: this.getMetadataValue('zone_name') ?? daemon.zone_name
      },
      { label: $localize`Zone ID`, value: this.getMetadataValue('zone_id') }
    ];

    this.softwareVersionFields = [
      { label: $localize`Ceph Release`, value: this.getMetadataValue('ceph_release') },
      { label: $localize`Architecture`, value: this.getMetadataValue('arch') },
      {
        label: $localize`Ceph Version (Short)`,
        value: this.getMetadataValue('ceph_version_short')
      },
      { label: $localize`CPU`, value: this.getMetadataValue('cpu') },
      { label: $localize`Full Version String`, value: daemon.version }
    ];

    this.distributionFields = [
      { label: $localize`Distro`, value: this.getMetadataValue('distro') },
      { label: $localize`Distro Description`, value: this.getMetadataValue('distro_description') },
      { label: $localize`Distro Version`, value: this.getMetadataValue('distro_version') }
    ];

    this.frontendConfigurationFields = [
      { label: $localize`Frontend Config 0`, value: this.getMetadataValue('frontend_config#0') },
      { label: $localize`Frontend Type 0`, value: this.getMetadataValue('frontend_type#0') }
    ];

    this.kernelOperatingSystemFields = [
      { label: $localize`OS`, value: this.getMetadataValue('os') },
      { label: $localize`PID`, value: this.getMetadataValue('pid') },
      { label: $localize`Kernel Version`, value: this.getMetadataValue('kernel_version') },
      { label: $localize`Num Handles`, value: this.getMetadataValue('num_handles') },
      { label: $localize`Kernel Description`, value: this.getMetadataValue('kernel_description') }
    ];
  }

  private getMetadataValue(key: string): string | number | boolean | null | undefined {
    return this.formatMetadataValue(this.metadata[key]);
  }

  private formatMetadataValue(value: unknown): string | number | boolean | null | undefined {
    if (value === null || value === undefined) {
      return value as null | undefined;
    }

    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return value;
    }

    return JSON.stringify(value);
  }

  private toNumber(value: string | number | boolean | null | undefined): number {
    if (typeof value === 'number') {
      return Number.isFinite(value) ? value : 0;
    }

    if (typeof value === 'string') {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : 0;
    }

    return 0;
  }
}
