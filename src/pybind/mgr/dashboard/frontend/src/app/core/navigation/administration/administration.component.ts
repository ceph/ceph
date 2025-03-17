import { Component } from '@angular/core';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';

@Component({
  selector: 'cd-administration',
  templateUrl: './administration.component.html',
  styleUrls: ['./administration.component.scss']
})
export class AdministrationComponent {
  userPermission: Permission;
  configOptPermission: Permission;

  constructor(private authStorageService: AuthStorageService, private modalService: ModalService) {
    const permissions = this.authStorageService.getPermissions();
    this.userPermission = permissions.user;
    this.configOptPermission = permissions.configOpt;
  }

  closeModal() {
    this.modalService.dismissAll();
  }
}
