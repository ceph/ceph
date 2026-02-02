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
  public readonly basePath = '/block/nvmeof/subsystems/view';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute) {}

  ngOnInit() {
    this.route.paramMap.subscribe((pm: ParamMap) => {
      this.subsystemNQN = pm.get('subsystem_nqn') ?? '';
      this.groupName = pm.get('group') ?? '';
      this.sidebarItems = [
        {
          label: $localize`Hosts`,
          route: [this.basePath, this.subsystemNQN, this.groupName, 'hosts'],
          routerLinkActiveOptions: { exact: true }
        },
        {
          label: $localize`Namespaces`,
          route: [this.basePath, this.subsystemNQN, this.groupName, 'namespaces']
        },
        {
          label: $localize`Listeners`,
          route: [this.basePath, this.subsystemNQN, this.groupName, 'listeners']
        }
      ];
    });
  }
}
