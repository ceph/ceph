import { Injectable, OnDestroy } from '@angular/core';
import { ReplaySubject, Subscription } from 'rxjs';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { ConfigFormModel } from '~/app/shared/components/config-option/config-option.model';

export type ConfigValueEntry = {
  section?: string;
  value?: string | number | boolean | null;
};

export type ConfigurationOption = Omit<ConfigFormModel, 'value' | 'services'> & {
  value?: ConfigValueEntry[];
  services?: string | string[];
  flags?: string | string[];
  source?: string;
  level?: string;
  tags?: string | string[];
  enum_values?: string | string[];
  see_also?: string | string[];
};

@Injectable()
export class ConfigurationResourceStateService implements OnDestroy {
  private configurationSource = new ReplaySubject<ConfigurationOption | null>(1);
  private loadSubscription?: Subscription;

  readonly configuration$ = this.configurationSource.asObservable();

  constructor(private configurationService: ConfigurationService) {}

  ngOnDestroy(): void {
    this.loadSubscription?.unsubscribe();
  }

  load(configNameRoute: string): void {
    this.loadSubscription?.unsubscribe();

    if (!configNameRoute) {
      this.configurationSource.next(null);
      return;
    }

    this.loadSubscription = this.configurationService.get(configNameRoute).subscribe({
      next: (configOption: ConfigurationOption) => this.configurationSource.next(configOption),
      error: () => this.configurationSource.next(null)
    });
  }
}
