import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';

@Component({
  selector: 'cd-nvmeof-subsystem-overview',
  templateUrl: './nvmeof-subsystem-overview.component.html',
  styleUrls: ['./nvmeof-subsystem-overview.component.scss'],
  standalone: false
})
export class NvmeofSubsystemOverviewComponent implements OnInit {
  subsystemNQN: string;
  groupName: string;
  subsystem: NvmeofSubsystem;

  constructor(private route: ActivatedRoute, private nvmeofService: NvmeofService) {}

  ngOnInit() {
    this.route.parent?.params.subscribe((params) => {
      this.subsystemNQN = params['subsystem_nqn'];
      this.fetchIfReady();
    });
    this.route.queryParams.subscribe((qp) => {
      this.groupName = qp['group'];
      this.fetchIfReady();
    });
  }

  private fetchIfReady() {
    if (this.subsystemNQN && this.groupName) {
      this.fetchSubsystem();
    }
  }

  fetchSubsystem() {
    this.nvmeofService
      .getSubsystem(this.subsystemNQN, this.groupName)
      .subscribe((subsystem: NvmeofSubsystem) => {
        this.subsystem = subsystem;
      });
  }
}
