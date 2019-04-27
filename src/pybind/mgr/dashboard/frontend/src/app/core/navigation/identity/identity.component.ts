import { Component, OnInit } from '@angular/core';

import { AuthService } from '../../../shared/api/auth.service';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-identity',
  templateUrl: './identity.component.html',
  styleUrls: ['./identity.component.scss']
})
export class IdentityComponent implements OnInit {
  username: string;

  constructor(private authStorageService: AuthStorageService, private authService: AuthService) {}

  ngOnInit() {
    this.username = this.authStorageService.getUsername();
  }

  logout() {
    this.authService.logout();
  }
}
