import { Injectable, Injector } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { concat, from, Observable, of } from 'rxjs';
import { distinct, first, map, mergeMap, toArray } from 'rxjs/operators';

import { AppConstants } from '~/app/shared/constants/app.constants';
import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class RouteBreadcrumbsService {
  private readonly defaultResolver = new BreadcrumbsResolver();

  constructor(private injector: Injector) {}

  resolve(route: ActivatedRouteSnapshot): Observable<IBreadcrumb[]> {
    return this.resolveRoute(route).pipe(
      mergeMap((crumbs) => crumbs),
      distinct((crumb) => crumb.text),
      toArray(),
      map((crumbs) => this.postProcess(crumbs))
    );
  }

  getTitleFromCrumbs(crumbs: IBreadcrumb[]): string {
    const currentLocation = crumbs
      .map((crumb: IBreadcrumb) => {
        return crumb.text || '';
      })
      .join(' > ');

    if (currentLocation.length > 0) {
      return `${AppConstants.projectName}: ${currentLocation}`;
    }

    return AppConstants.projectName;
  }

  private resolveRoute(route: ActivatedRouteSnapshot): Observable<IBreadcrumb[]> {
    let crumbs$: Observable<IBreadcrumb[]>;

    const data = route.routeConfig?.data;

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
      crumbs$ = concat<IBreadcrumb[]>(crumbs$, this.resolveRoute(route.firstChild));
    }

    return crumbs$;
  }

  private postProcess(breadcrumbs: IBreadcrumb[]): IBreadcrumb[] {
    const result: IBreadcrumb[] = [];

    breadcrumbs.forEach((element) => {
      const split = element.text.split('/');

      if (!element.disableSplit && split.length > 1) {
        element.text = split[split.length - 1];

        for (let index = 0; index < split.length - 1; index++) {
          result.push({ text: split[index], path: null });
        }
      }

      result.push(element);
    });

    return result;
  }

  private isPromise(value: any): boolean {
    return value && typeof value.then === 'function';
  }

  private wrapIntoObservable<T>(value: T | Promise<T> | Observable<T>): Observable<T> {
    if (value instanceof Observable) {
      return value;
    }

    if (this.isPromise(value)) {
      return from(Promise.resolve(value));
    }

    return of(value as T);
  }
}
