import { Component, OnInit } from '@angular/core';

import { AuthService } from '../../../shared/api/auth.service';
import { Icons } from '../../../shared/enum/icons.enum';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-identity',
  templateUrl: './identity.component.html',
  styleUrls: ['./identity.component.scss']
})
export class IdentityComponent implements OnInit {
  sso: boolean;
  username: string;
  icons = Icons;

  constructor(private authStorageService: AuthStorageService, private authService: AuthService) {}

  ngOnInit() {
    this.username = this.authStorageService.getUsername();
    this.sso = this.authStorageService.isSSO();
  }

  logout() {
    this.authService.logout();
  }
}
