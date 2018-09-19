import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { AuthService } from '../../../shared/api/auth.service';
@Component({
  selector: 'cd-identity',
  templateUrl: './identity.component.html',
  styleUrls: ['./identity.component.scss']
})
export class IdentityComponent implements OnInit {
  username: string;
  
  constructor(
    private authService: AuthService,
    private router: Router
  ) { 
    this.username = localStorage.getItem("dashboard_username");
  }

  ngOnInit() {}

  logout() {
    this.authService.logout(() => {
      this.router.navigate(['/login']);
    });
  }
}
