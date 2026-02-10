import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Observable, of } from 'rxjs';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';

@Component({
  selector: 'cd-nvme-gateway-view',
  templateUrl: './nvme-gateway-view.component.html',
  styleUrls: ['./nvme-gateway-view.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class NvmeGatewayViewComponent implements OnInit {
  groupName: string;
  subsystems$: Observable<NvmeofSubsystem[]> = of([]);
  public readonly basePath = '/block/nvmeof/gateways/view';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute) {}

  ngOnInit() {
    this.route.paramMap.subscribe((pm: ParamMap) => {
      this.groupName = pm.get('group') ?? '';
      this.sidebarItems = [
        {
          label: $localize`Gateway nodes`,
          route: [this.basePath, this.groupName, 'nodes'],
          routerLinkActiveOptions: { exact: true }
        },
        {
          label: $localize`Subsystems`,
          route: [this.basePath, this.groupName, 'subsystems']
        }
      ];
    });
  }
}
