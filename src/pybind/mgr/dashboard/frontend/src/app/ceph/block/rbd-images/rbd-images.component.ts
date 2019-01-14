import { Component, OnInit } from '@angular/core';

import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-rbd-images',
  templateUrl: './rbd-images.component.html',
  styleUrls: ['./rbd-images.component.scss']
})
export class RbdImagesComponent implements OnInit {
  grafanaPermission: Permission;

  constructor(private authStorageService: AuthStorageService) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnInit() {}
}
