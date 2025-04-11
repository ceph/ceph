import { Component, HostListener } from '@angular/core';

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

  @HostListener('window:beforeunload', ['$event'])
  unloadNotification($event: any) {
    // Check if any forms are dirty
    const forms = document.getElementsByTagName('form');
    for (let i = 0; i < forms.length; i++) {
      if (forms[i].classList.contains('ng-dirty')) {
        $event.returnValue = true;
        return true;
      }
    }
    return false;
  }
}
