import { Component } from '@angular/core';

import { NgbPopoverConfig, NgbTooltipConfig } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  constructor(popoverConfig: NgbPopoverConfig, tooltipConfig: NgbTooltipConfig) {
    popoverConfig.autoClose = 'outside';
    popoverConfig.container = 'body';
    popoverConfig.placement = 'bottom';

    tooltipConfig.container = 'body';
  }
}
