import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-rbd-tabs',
  templateUrl: './rbd-tabs.component.html',
  styleUrls: ['./rbd-tabs.component.scss']
})
export class RbdTabsComponent {
  grafanaPermission: Permission;
  url: string;

  constructor(private authStorageService: AuthStorageService, public router: Router) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }
}
