import { Component, OnInit } from '@angular/core';

import { Router } from '@angular/router';

@Component({
  selector: 'cd-user-tabs',
  templateUrl: './user-tabs.component.html',
  styleUrls: ['./user-tabs.component.scss']
})
export class UserTabsComponent implements OnInit {
  url: string;

  constructor(private router: Router) {}

  ngOnInit() {
    this.url = this.router.url;
  }

  navigateTo(url) {
    this.router.navigate([url]);
  }
}
