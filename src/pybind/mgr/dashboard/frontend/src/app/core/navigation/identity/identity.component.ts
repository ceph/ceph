import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { AuthService } from '../../../shared/api/auth.service';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-identity',
  templateUrl: './identity.component.html',
  styleUrls: ['./identity.component.scss']
})
export class IdentityComponent implements OnInit {
  username: string;

  constructor(
    private router: Router,
    private authStorageService: AuthStorageService,
    private authService: AuthService
  ) {}

  ngOnInit() {
    this.username = this.authStorageService.getUsername();
  }

  logout() {
    this.authService.logout(() => {
      this.router.navigate(['/login']);
    });
  }
}
