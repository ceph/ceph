import { Component, OnInit } from '@angular/core';

import { Router } from '@angular/router';

@Component({
  selector: 'cd-iscsi-tabs',
  templateUrl: './iscsi-tabs.component.html',
  styleUrls: ['./iscsi-tabs.component.scss']
})
export class IscsiTabsComponent implements OnInit {
  url: string;

  constructor(private router: Router) {}

  ngOnInit() {
    this.url = this.router.url;
  }

  navigateTo(url) {
    this.router.navigate([url]);
  }
}
