import { Component } from '@angular/core';

import { Icons } from '../../../shared/enum/icons.enum';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-administration',
  templateUrl: './administration.component.html',
  styleUrls: ['./administration.component.scss']
})
export class AdministrationComponent {
  userPermission: Permission;
  configOptPermission: Permission;
  icons = Icons;

  constructor(private authStorageService: AuthStorageService) {
    const permissions = this.authStorageService.getPermissions();
    this.userPermission = permissions.user;
    this.configOptPermission = permissions.configOpt;
  }
}
