import { Component } from '@angular/core';

import { Router } from '@angular/router';

@Component({
  selector: 'cd-user-tabs',
  templateUrl: './user-tabs.component.html',
  styleUrls: ['./user-tabs.component.scss']
})
export class UserTabsComponent {
  url: string;

  constructor(public router: Router) {}
}
