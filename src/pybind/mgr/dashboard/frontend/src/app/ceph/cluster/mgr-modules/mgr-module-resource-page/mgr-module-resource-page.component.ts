import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';

import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { MgrModuleInfo } from '~/app/shared/models/mgr-modules.interface';
import {
  MgrModuleResourceState,
  MgrModuleResourceStateService
} from '~/app/shared/services/mgr-module-resource-state.service';

@Component({
  selector: 'cd-mgr-module-resource-page',
  templateUrl: './mgr-module-resource-page.component.html',
  styleUrls: ['./mgr-module-resource-page.component.scss'],
  standalone: false
})
export class MgrModuleResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = 'overview';
  moduleNameRoute = '';
  notFound = false;
  overviewFields: OverviewField[] = [];

  constructor(
    private route: ActivatedRoute,
    private mgrModuleResourceStateService: MgrModuleResourceStateService
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';

    this.sub.add(
      this.mgrModuleResourceStateService.state$.subscribe(
        (state: MgrModuleResourceState | null) => {
          this.applyState(state);
        }
      )
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyState(state: MgrModuleResourceState | null): void {
    if (!state) {
      this.moduleNameRoute = '';
      this.notFound = true;
      this.overviewFields = [];
      return;
    }

    this.moduleNameRoute = state.moduleNameRoute;
    this.notFound = false;
    this.overviewFields = this.buildOverviewFields(state.moduleInfo, state.moduleConfig);
  }

  private buildOverviewFields(
    moduleInfo: MgrModuleInfo,
    moduleConfig: Record<string, unknown>
  ): OverviewField[] {
    const fields: OverviewField[] = [
      {
        label: $localize`Name`,
        value: moduleInfo.name
      },
      {
        label: $localize`Enabled`,
        value: moduleInfo.enabled ? $localize`Yes` : $localize`No`
      },
      {
        label: $localize`Always-On`,
        value: moduleInfo.always_on ? $localize`Yes` : $localize`No`
      }
    ];

    Object.entries(moduleConfig || {}).forEach(([key, value]) => {
      fields.push({
        label: this.formatConfigLabel(key),
        value: this.formatOverviewValue(value)
      });
    });

    return fields;
  }

  private formatOverviewValue(value: unknown): string | number | boolean | null | undefined {
    if (typeof value === 'object') {
      return JSON.stringify(value);
    }

    return value as string | number | boolean;
  }

  private formatConfigLabel(key: string): string {
    const normalizedKey = key.split('_').filter(Boolean).join(' ').toLowerCase();

    if (!normalizedKey) {
      return '';
    }

    return normalizedKey.charAt(0).toUpperCase() + normalizedKey.slice(1);
  }
}
