import { Component } from '@angular/core';

import { NgbPopoverConfig } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  constructor(config: NgbPopoverConfig) {
    config.autoClose = 'outside';
    config.container = 'body';
    config.placement = 'bottom';
  }
}
