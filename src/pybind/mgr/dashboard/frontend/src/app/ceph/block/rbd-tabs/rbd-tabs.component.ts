import { Component } from '@angular/core';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-rbd-tabs',
  templateUrl: './rbd-tabs.component.html',
  styleUrls: ['./rbd-tabs.component.scss']
})
export class RbdTabsComponent {
  grafanaPermission: Permission;
  url: string;

  constructor(private authStorageService: AuthStorageService) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }
}
