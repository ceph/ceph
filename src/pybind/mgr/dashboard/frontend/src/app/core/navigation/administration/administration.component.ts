import { Component, OnInit } from '@angular/core';

import { Icons } from '../../../shared/enum/icons.enum';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-administration',
  templateUrl: './administration.component.html',
  styleUrls: ['./administration.component.scss']
})
export class AdministrationComponent implements OnInit {
  userPermission: Permission;
  icons = Icons;

  constructor(private authStorageService: AuthStorageService) {
    this.userPermission = this.authStorageService.getPermissions().user;
  }

  ngOnInit() {}
}
