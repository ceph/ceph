import { TestBed } from '@angular/core/testing';
import { ActivatedRouteSnapshot, convertToParamMap } from '@angular/router';

import { NfsClusterResourceBreadcrumbResolver } from './nfs-cluster-resource-breadcrumb.resolver';

describe('NfsClusterResourceBreadcrumbResolver', () => {
  let resolver: NfsClusterResourceBreadcrumbResolver;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [NfsClusterResourceBreadcrumbResolver]
    });

    resolver = TestBed.inject(NfsClusterResourceBreadcrumbResolver);
    jest.spyOn(resolver, 'getFullPath').mockReturnValue('/cephfs/nfs/cluster/demo-nfs-cluster');
  });

  it('should be created', () => {
    expect(resolver).toBeTruthy();
  });

  it('should resolve breadcrumb text from cluster_id', () => {
    const route = {
      paramMap: convertToParamMap({ cluster_id: 'demo-nfs-cluster' })
    } as unknown as ActivatedRouteSnapshot;

    expect(resolver.resolve(route)).toEqual([
      { text: 'demo-nfs-cluster', path: '/cephfs/nfs/cluster/demo-nfs-cluster' }
    ]);
  });

  it('should return empty text when cluster_id is missing', () => {
    const route = {
      paramMap: convertToParamMap({})
    } as unknown as ActivatedRouteSnapshot;

    expect(resolver.resolve(route)).toEqual([
      { text: '', path: '/cephfs/nfs/cluster/demo-nfs-cluster' }
    ]);
  });
});
