import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-nvmeof-subsystem-performance',
  templateUrl: './nvmeof-subsystem-performance.component.html',
  styleUrls: ['./nvmeof-subsystem-performance.component.scss'],
  standalone: false
})
export class NvmeofSubsystemPerformanceComponent implements OnInit {
  subsystemNQN: string;
  groupName: string;
  permissions: Permissions;

  constructor(private route: ActivatedRoute, private authStorageService: AuthStorageService) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.route.parent?.params.subscribe((params) => {
      this.subsystemNQN = params['subsystem_nqn'];
    });
    this.route.queryParams.subscribe((qp) => {
      this.groupName = qp['group'];
    });
  }
}
