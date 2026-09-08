import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { decodeModuleName } from '~/app/shared/models/mgr-modules.interface';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { MgrModuleResourceStateService } from '~/app/shared/services/mgr-module-resource-state.service';

@Component({
  selector: 'cd-mgr-module-resource-sidebar',
  templateUrl: './mgr-module-resource-sidebar.component.html',
  styleUrls: ['./mgr-module-resource-sidebar.component.scss'],
  providers: [MgrModuleResourceStateService],
  standalone: false
})
export class MgrModuleResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  readonly basePath = '/mgr-modules';
  moduleNameRoute = '';
  moduleName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(
    private route: ActivatedRoute,
    private mgrModuleResourceStateService: MgrModuleResourceStateService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.moduleNameRoute = pm.get('name') ?? '';
        this.moduleName = decodeModuleName(this.moduleNameRoute);
        this.buildSidebarItems();
        this.mgrModuleResourceStateService.load(this.moduleNameRoute);
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
        route: [this.basePath, this.moduleNameRoute, 'overview'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}
