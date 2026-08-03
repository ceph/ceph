import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { ConfigurationResourceStateService } from '~/app/shared/services/configuration-resource-state.service';

@Component({
  selector: 'cd-configuration-resource-sidebar',
  templateUrl: './configuration-resource-sidebar.component.html',
  styleUrls: ['./configuration-resource-sidebar.component.scss'],
  providers: [ConfigurationResourceStateService],
  standalone: false
})
export class ConfigurationResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  readonly basePath = '/configuration';
  configurationOption = '';
  sidebarItems: SidebarItem[] = [];

  constructor(
    private route: ActivatedRoute,
    private configurationResourceStateService: ConfigurationResourceStateService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.configurationOption = pm.get('name') ?? '';
        this.buildSidebarItems();
        this.configurationResourceStateService.load(this.configurationOption);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  get configurationTitle(): string {
    if (!this.configurationOption) {
      return '';
    }

    try {
      return decodeURIComponent(this.configurationOption);
    } catch {
      return this.configurationOption;
    }
  }

  private buildSidebarItems(): void {
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.configurationOption, 'overview'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}
