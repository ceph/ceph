import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';

@Component({
  selector: 'cd-nvme-subsystem-view',
  templateUrl: './nvme-subsystem-view.component.html',
  styleUrls: ['./nvme-subsystem-view.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class NvmeSubsystemViewComponent implements OnInit {
  subsystemNQN: string;
  groupName: string;
  public readonly basePath = '/block/nvmeof/subsystems';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute) {}

  ngOnInit() {
    this.route.paramMap.subscribe((pm: ParamMap) => {
      this.subsystemNQN = pm.get('subsystem_nqn') ?? '';
    });
    this.route.queryParams.subscribe((qp) => {
      this.groupName = qp['group'] ?? '';
      this.buildSidebarItems();
    });
  }

  private buildSidebarItems() {
    const extras = { queryParams: { group: this.groupName } };
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.subsystemNQN, 'overview'],
        routeExtras: extras
      },
      {
        label: $localize`Initiators`,
        route: [this.basePath, this.subsystemNQN, 'hosts'],
        routeExtras: extras
      },
      {
        label: $localize`Namespaces`,
        route: [this.basePath, this.subsystemNQN, 'namespaces'],
        routeExtras: extras
      },
      {
        label: $localize`Listeners`,
        route: [this.basePath, this.subsystemNQN, 'listeners'],
        routeExtras: extras
      },
      {
        label: $localize`Performance`,
        route: [this.basePath, this.subsystemNQN, 'performance'],
        routeExtras: extras
      }
    ];
  }
}
