import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'cd-iscsi-tabs',
  templateUrl: './iscsi-tabs.component.html',
  styleUrls: ['./iscsi-tabs.component.scss']
})
export class IscsiTabsComponent {
  constructor(public router: Router) {}
}
