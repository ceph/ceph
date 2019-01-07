import { Component, Input, OnInit } from '@angular/core';

import { Router } from '@angular/router';

@Component({
  selector: 'cd-user-tabs',
  templateUrl: './user-tabs.component.html',
  styleUrls: ['./user-tabs.component.scss']
})
export class UserTabsComponent implements OnInit {
  url: string;

  /**
   * Change initial active state of a tabset component without tigger event emitted.
   */
  @Input()
  changeInitialActive: Boolean;

  constructor(private router: Router) {}

  ngOnInit() {
    this.url = this.router.url;
  }

  navigateTo(url) {
    if (this.changeInitialActive) {
      this.changeInitialActive = false;
      return;
    }
    this.router.navigate([url]);
  }
}
