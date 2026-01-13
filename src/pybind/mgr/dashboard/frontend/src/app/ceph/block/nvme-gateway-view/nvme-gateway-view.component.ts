import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Observable, of } from 'rxjs';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';

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
  selectedTab: string | null = 'gateways';
  constructor(private route: ActivatedRoute) {}

  ngOnInit() {
    this.route.paramMap.subscribe((pm: ParamMap) => {
      this.groupName = pm.get('group') ?? '';
    });
  }

  selectTab(tab: string): void {
    this.selectedTab = tab;
  }
}
