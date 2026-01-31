import { TestBed } from '@angular/core/testing';
import { ActivatedRouteSnapshot } from '@angular/router';

import { NvmeGatewayViewBreadcrumbResolver } from './nvme-gateway-view-breadcrumb.resolver';

describe('NvmeGatewayViewBreadcrumbResolver', () => {
  let resolver: NvmeGatewayViewBreadcrumbResolver;
  let route: ActivatedRouteSnapshot;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [NvmeGatewayViewBreadcrumbResolver]
    });
    resolver = TestBed.inject(NvmeGatewayViewBreadcrumbResolver);
    route = new ActivatedRouteSnapshot();
  });

  it('should be created', () => {
    expect(resolver).toBeTruthy();
  });

  it('should resolve breadcrumb with group name from parent params', () => {
    route.params = {};
    Object.defineProperty(route, 'parent', {
      value: { params: { group: 'test-group' } },
      writable: true
    });

    spyOn(resolver, 'getFullPath').and.returnValue('full/path/test-group');

    const result = resolver.resolve(route);

    expect(result).toEqual([{ text: 'test-group', path: 'full/path/test-group' }]);
  });

  it('should resolve breadcrumb with group name from current params', () => {
    route.params = { group: 'test-group' };
    Object.defineProperty(route, 'parent', {
      value: { params: {} },
      writable: true
    });
    spyOn(resolver, 'getFullPath').and.returnValue('full/path/test-group');

    const result = resolver.resolve(route);

    expect(result).toEqual([{ text: 'test-group', path: 'full/path/test-group' }]);
  });
});
