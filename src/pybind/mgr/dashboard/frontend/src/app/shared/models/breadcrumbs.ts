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

import { ActivatedRouteSnapshot, UrlSegment } from '@angular/router';

import { Observable, of } from 'rxjs';

export class BreadcrumbsResolver {
  public resolve(
    route: ActivatedRouteSnapshot
  ): Observable<IBreadcrumb[]> | Promise<IBreadcrumb[]> | IBreadcrumb[] {
    const data = route.routeConfig.data;
    const path = data.path === null ? null : this.getFullPath(route);
    const disableSplit = data.disableSplit || false;

    const text =
      typeof data.breadcrumbs === 'string'
        ? data.breadcrumbs
        : data.breadcrumbs.text || data.text || path;

    const crumbs: IBreadcrumb[] = [{ text: text, path: path, disableSplit: disableSplit }];

    return of(crumbs);
  }

  public getFullPath(route: ActivatedRouteSnapshot): string {
    const relativePath = (segments: UrlSegment[]) =>
      segments.reduce((a, v) => (a += '/' + v.path), '');
    const fullPath = (routes: ActivatedRouteSnapshot[]) =>
      routes.reduce((a, v) => (a += relativePath(v.url)), '');

    return fullPath(route.pathFromRoot);
  }
}

export interface IBreadcrumb {
  text: string;
  path: string;
  disableSplit?: boolean;
}
