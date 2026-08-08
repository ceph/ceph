import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { decodeServiceNameFromRoute } from '~/app/shared/models/service.interface';
import { ServiceResourceStateService } from '~/app/shared/services/service-resource-state.service';

@Component({
  selector: 'cd-service-resource-sidebar',
  templateUrl: './service-resource-sidebar.component.html',
  styleUrls: ['./service-resource-sidebar.component.scss'],
  providers: [ServiceResourceStateService],
  standalone: false
})
export class ServiceResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  readonly basePath = '/services';
  serviceNameRoute = '';
  serviceName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(
    private route: ActivatedRoute,
    private serviceResourceStateService: ServiceResourceStateService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.serviceResourceStateService.service$.subscribe((service) => {
        if (service?.service_name) {
          this.serviceName = service.service_name;
        }
        this.buildSidebarItems(!!service?.certificate?.has_certificate);
      })
    );

    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.serviceNameRoute = pm.get('service_name') ?? '';
        this.serviceName = decodeServiceNameFromRoute(this.serviceNameRoute);
        this.serviceResourceStateService.load(this.serviceNameRoute);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(hasCertificate: boolean): void {
    const items: SidebarItem[] = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.serviceNameRoute, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Daemons`,
        route: [this.basePath, this.serviceNameRoute, 'daemons'],
        routerLinkActiveOptions: { exact: true }
      }
    ];

    if (hasCertificate) {
      items.push({
        label: $localize`Certificate`,
        route: [this.basePath, this.serviceNameRoute, 'certificate'],
        routerLinkActiveOptions: { exact: true }
      });
    }

    items.push({
      label: $localize`Service Events`,
      route: [this.basePath, this.serviceNameRoute, 'events'],
      routerLinkActiveOptions: { exact: true }
    });

    this.sidebarItems = items;
  }
}
