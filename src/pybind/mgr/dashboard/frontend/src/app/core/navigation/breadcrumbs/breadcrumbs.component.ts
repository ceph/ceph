/*
The MIT License

Copyright (c) 2017 (null) McNull https://github.com/McNull

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */

import { Component, OnDestroy } from '@angular/core';
import { Title } from '@angular/platform-browser';
import { NavigationEnd, NavigationStart, Router } from '@angular/router';

import { Subscription } from 'rxjs';
import { filter, take } from 'rxjs/operators';

import { IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { BreadcrumbService } from '~/app/shared/services/breadcrumb.service';
import { RouteBreadcrumbsService } from '~/app/shared/services/route-breadcrumbs.service';

@Component({
  selector: 'cd-breadcrumbs',
  templateUrl: './breadcrumbs.component.html',
  styleUrls: ['./breadcrumbs.component.scss'],
  standalone: false
})
export class BreadcrumbsComponent implements OnDestroy {
  crumbs: IBreadcrumb[] = [];
  /**
   * Useful for e2e tests.
   * This allow us to mark the breadcrumb as pending during the navigation from
   * one page to another.
   * This resolves the problem of validating the breadcrumb of a new page and
   * still get the value from the previous
   */
  finished = false;
  subscription: Subscription = new Subscription();
  private tabCrumbSubscription: Subscription;
  private baseCrumbs: IBreadcrumb[] = [];

  constructor(
    private router: Router,
    private titleService: Title,
    private breadcrumbService: BreadcrumbService,
    private routeBreadcrumbsService: RouteBreadcrumbsService
  ) {
    this.refreshBreadcrumbs();

    this.subscription.add(
      this.router.events.pipe(filter((x) => x instanceof NavigationStart)).subscribe(() => {
        this.finished = false;
        this.breadcrumbService.clearTabCrumb();
      })
    );

    this.subscription.add(
      this.router.events
        .pipe(filter((x) => x instanceof NavigationEnd))
        .subscribe(() => this.refreshBreadcrumbs())
    );

    this.tabCrumbSubscription = this.breadcrumbService.tabCrumb$.subscribe((tabCrumb) => {
      if (tabCrumb && this.baseCrumbs.length > 0) {
        this.crumbs = [...this.baseCrumbs.slice(0, -1), tabCrumb];
      } else {
        this.crumbs = [...this.baseCrumbs];
      }
      const title = this.routeBreadcrumbsService.getTitleFromCrumbs(this.crumbs);
      this.titleService.setTitle(title);
    });
  }

  ngOnDestroy(): void {
    this.subscription.unsubscribe();
    this.tabCrumbSubscription.unsubscribe();
  }

  private refreshBreadcrumbs(): void {
    const currentRoot = this.router.routerState.snapshot.root;

    this.routeBreadcrumbsService.resolve(currentRoot).subscribe((crumbs) => {
      this.finished = true;
      this.baseCrumbs = crumbs;
      this.breadcrumbService.tabCrumb$.pipe(take(1)).subscribe((tabCrumb) => {
        this.crumbs =
          tabCrumb && crumbs.length > 0 ? [...crumbs.slice(0, -1), tabCrumb] : [...crumbs];
        const title = this.routeBreadcrumbsService.getTitleFromCrumbs(this.crumbs);
        this.titleService.setTitle(title);
      });
    });
  }
}
