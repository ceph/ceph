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

import { Component, Injector, OnDestroy } from '@angular/core';
import { ActivatedRouteSnapshot, NavigationEnd, NavigationStart, Router } from '@angular/router';

import { concat, from, Observable, of, Subscription } from 'rxjs';
import { distinct, filter, first, flatMap, toArray } from 'rxjs/operators';

import { BreadcrumbsResolver, IBreadcrumb } from '../../../shared/models/breadcrumbs';

@Component({
  selector: 'cd-breadcrumbs',
  templateUrl: './breadcrumbs.component.html',
  styleUrls: ['./breadcrumbs.component.scss']
})
export class BreadcrumbsComponent implements OnDestroy {
  crumbs: IBreadcrumb[] = [];
  /**
   * Usefull for e2e tests.
   * This allow us to mark the breadcrumb as pending during the navigation from
   * one page to another.
   * This resolves the problem of validating the breadcrumb of a new page and
   * still get the value from the previous
   */
  finished = false;
  subscription: Subscription;
  private defaultResolver = new BreadcrumbsResolver();

  constructor(private router: Router, private injector: Injector) {
    this.subscription = this.router.events
      .pipe(filter((x) => x instanceof NavigationStart))
      .subscribe(() => {
        this.finished = false;
      });

    this.subscription = this.router.events
      .pipe(filter((x) => x instanceof NavigationEnd))
      .subscribe(() => {
        const currentRoot = router.routerState.snapshot.root;

        this._resolveCrumbs(currentRoot)
          .pipe(
            flatMap((x) => x),
            distinct((x) => x.text),
            toArray(),
            flatMap((x) => {
              const y = this.postProcess(x);
              return this.wrapIntoObservable<IBreadcrumb[]>(y).pipe(first());
            })
          )
          .subscribe((x) => {
            this.finished = true;
            this.crumbs = x;
          });
      });
  }

  ngOnDestroy(): void {
    this.subscription.unsubscribe();
  }

  private _resolveCrumbs(route: ActivatedRouteSnapshot): Observable<IBreadcrumb[]> {
    let crumbs$: Observable<IBreadcrumb[]>;

    const data = route.routeConfig && route.routeConfig.data;

    if (data && data.breadcrumbs) {
      let resolver: BreadcrumbsResolver;

      if (data.breadcrumbs.prototype instanceof BreadcrumbsResolver) {
        resolver = this.injector.get<BreadcrumbsResolver>(data.breadcrumbs);
      } else {
        resolver = this.defaultResolver;
      }

      const result = resolver.resolve(route);
      crumbs$ = this.wrapIntoObservable<IBreadcrumb[]>(result).pipe(first());
    } else {
      crumbs$ = of([]);
    }

    if (route.firstChild) {
      crumbs$ = concat<IBreadcrumb[]>(crumbs$, this._resolveCrumbs(route.firstChild));
    }

    return crumbs$;
  }

  postProcess(breadcrumbs: IBreadcrumb[]) {
    const result: IBreadcrumb[] = [];
    breadcrumbs.forEach((element) => {
      const split = element.text.split('/');
      if (split.length > 1) {
        element.text = split[split.length - 1];
        for (let i = 0; i < split.length - 1; i++) {
          result.push({ text: split[i], path: null });
        }
      }
      result.push(element);
    });
    return result;
  }

  isPromise(value: any): boolean {
    return value && typeof value.then === 'function';
  }

  wrapIntoObservable<T>(value: T | Promise<T> | Observable<T>): Observable<T> {
    if (value instanceof Observable) {
      return value;
    }

    if (this.isPromise(value)) {
      return from(Promise.resolve(value));
    }

    return of(value as T);
  }
}
