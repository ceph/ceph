import { Component } from '@angular/core';

@Component({
  selector: 'cd-sso-not-found',
  templateUrl: './sso-not-found.component.html',
  styleUrls: ['./sso-not-found.component.scss']
})
export class SsoNotFoundComponent {
  logoutUrl: string;

  constructor() {
    this.logoutUrl = `${window.location.origin}/auth/saml2/slo`;
  }
}
