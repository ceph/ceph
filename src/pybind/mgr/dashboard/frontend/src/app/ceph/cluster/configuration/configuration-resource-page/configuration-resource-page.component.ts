import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';

import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import {
  ConfigValueEntry,
  ConfigurationOption,
  ConfigurationResourceStateService
} from '~/app/shared/services/configuration-resource-state.service';

@Component({
  selector: 'cd-configuration-resource-page',
  templateUrl: './configuration-resource-page.component.html',
  styleUrls: ['./configuration-resource-page.component.scss'],
  standalone: false
})
export class ConfigurationResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = '';
  selection?: ConfigurationOption;
  overviewFields: OverviewField[] = [];
  notFound = false;

  flags = {
    runtime: $localize`The value can be updated at runtime.`,
    no_mon_update: $localize`Daemons/clients do not pull this value from the
      monitor config database. We disallow setting this option via 'ceph config
      set ...'. This option should be configured via ceph.conf or via the
      command line.`,
    startup: $localize`Option takes effect only during daemon startup.`,
    cluster_create: $localize`Option only affects cluster creation.`,
    create: $localize`Option only affects daemon creation.`
  };

  constructor(
    private route: ActivatedRoute,
    private configurationResourceStateService: ConfigurationResourceStateService
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';
    this.sub.add(
      this.configurationResourceStateService.configuration$.subscribe((configOption) => {
        this.applyConfiguration(configOption);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyConfiguration(configOption: ConfigurationOption | null): void {
    if (!configOption) {
      this.selection = undefined;
      this.overviewFields = [];
      this.notFound = true;
      return;
    }

    this.selection = {
      ...configOption,
      services: this.toArray(configOption.services)
    };
    this.overviewFields = this.buildOverviewFields(this.selection);
    this.notFound = false;
  }

  private buildOverviewFields(configOption: ConfigurationOption): OverviewField[] {
    return [
      { label: $localize`Name`, value: configOption.name },
      { label: $localize`Description`, value: configOption.desc },
      { label: $localize`Long description`, value: configOption.long_desc },
      {
        label: $localize`Current values`,
        values: this.getCurrentValues(configOption.value),
        type: 'tags'
      },
      { label: $localize`Default`, value: configOption.default },
      { label: $localize`Daemon default`, value: configOption.daemon_default },
      { label: $localize`Type`, value: configOption.type },
      { label: $localize`Min`, value: configOption.min },
      { label: $localize`Max`, value: configOption.max },
      { label: $localize`Flags`, values: this.toArray(configOption.flags), type: 'tags' },
      { label: $localize`Services`, values: this.toArray(configOption.services), type: 'tags' },
      { label: $localize`Source`, value: configOption.source },
      { label: $localize`Level`, value: configOption.level },
      {
        label: $localize`Can be updated at runtime (editable)`,
        value:
          configOption.can_update_at_runtime === undefined
            ? undefined
            : configOption.can_update_at_runtime
              ? $localize`Yes`
              : $localize`No`
      },
      { label: $localize`Tags`, values: this.toArray(configOption.tags), type: 'tags' },
      {
        label: $localize`Enum values`,
        values: this.toArray(configOption.enum_values),
        type: 'tags'
      },
      { label: $localize`See also`, values: this.toArray(configOption.see_also), type: 'tags' }
    ];
  }

  private getCurrentValues(value: ConfigValueEntry[] | undefined): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value
      .map((conf: ConfigValueEntry) => {
        const section = conf?.section;
        const sectionValue = conf?.value;
        if (!section && sectionValue === undefined) {
          return '';
        }
        return `${section}: ${sectionValue}`;
      })
      .filter(Boolean);
  }

  private toArray(value: unknown): string[] {
    if (Array.isArray(value)) {
      return value.map((entry) => `${entry}`.trim()).filter((entry: string) => entry.length > 0);
    }

    if (typeof value === 'string') {
      return value
        .split(',')
        .map((entry: string) => entry.trim())
        .filter((entry: string) => entry.length > 0);
    }

    return [];
  }
}
