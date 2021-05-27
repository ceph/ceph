import { Component, OnInit } from '@angular/core';
import { Title } from '@angular/platform-browser';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';

import { NgbPopoverConfig, NgbTooltipConfig } from '@ng-bootstrap/ng-bootstrap';
import { filter, map } from 'rxjs/operators';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent  implements OnInit {
  constructor(popoverConfig: NgbPopoverConfig, tooltipConfig: NgbTooltipConfig, private titleService: Title,
              private  router: Router,
              private activatedRoute: ActivatedRoute) {
    popoverConfig.autoClose = 'outside';
    popoverConfig.container = 'body';
    popoverConfig.placement = 'bottom';

    tooltipConfig.container = 'body';
  }

  ngOnInit() {
    const appTitle = this.titleService.getTitle();
    this.router
      .events.pipe(
      filter(event => event instanceof NavigationEnd),
      map(() => {
        let child = this.activatedRoute.firstChild;
        while (child.firstChild) {
          child = child.firstChild;
        }
        if (child.snapshot.data['breadcrumbs']) {
          return child.snapshot.data['breadcrumbs'];
        }
        return appTitle;
      })
    ).subscribe((ttl: string) => {
      this.titleService.setTitle(ttl);
    });
  }
}
