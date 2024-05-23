import { Directive, Host, HostListener, Input, OnInit, Optional } from '@angular/core';

import { NgbNav, NgbNavChangeEvent } from '@ng-bootstrap/ng-bootstrap';

@Directive({
  selector: '[cdStatefulTab]'
})
export class StatefulTabDirective implements OnInit {
  @Input()
  cdStatefulTab: string;
  @Input()
  cdStatefulTabDefault = '';

  private localStorage = window.localStorage;

  constructor(@Optional() @Host() private nav: NgbNav) {}

  ngOnInit() {
    // Is an activate tab identifier stored in the local storage?
    const activeId =
      this.cdStatefulTabDefault || this.localStorage.getItem(`tabset_${this.cdStatefulTab}`);
    if (activeId) {
      this.nav.select(activeId);
    }
  }

  @HostListener('navChange', ['$event'])
  onNavChange(event: NgbNavChangeEvent) {
    // Store the current active tab identifier in the local storage.
    if (this.cdStatefulTab && event.nextId) {
      this.localStorage.setItem(`tabset_${this.cdStatefulTab}`, event.nextId);
    }
  }
}
