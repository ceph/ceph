import { Component, OnInit } from '@angular/core';

import { AuthService } from '~/app/shared/api/auth.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';

@Component({
  selector: 'cd-identity',
  templateUrl: './identity.component.html',
  styleUrls: ['./identity.component.scss']
})
export class IdentityComponent implements OnInit {
  sso: boolean;
  username: string;
  icons = Icons;

  constructor(
    private authStorageService: AuthStorageService,
    private authService: AuthService,
    private modalService: ModalService
  ) {}

  ngOnInit() {
    this.username = this.authStorageService.getUsername();
    this.sso = this.authStorageService.isSSO();
  }

  logout() {
    this.authService.logout();
  }
  closeModal() {
    this.modalService.dismissAll();
  }
}
