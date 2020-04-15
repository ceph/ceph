import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-rbd-tabs',
  templateUrl: './rbd-tabs.component.html',
  styleUrls: ['./rbd-tabs.component.scss']
})
export class RbdTabsComponent implements OnInit {
  grafanaPermission: Permission;
  url: string;

  constructor(private authStorageService: AuthStorageService, private router: Router) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnInit() {
    this.url = this.router.url;
  }

  navigateTo(url: string) {
    this.router.navigate([url]);
  }
}
