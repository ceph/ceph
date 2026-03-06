import { TestBed } from '@angular/core/testing';

import { NvmeSubsystemViewBreadcrumbResolver } from './nvme-subsystem-view-breadcrumb.resolver';

describe('NvmeSubsystemViewBreadcrumbResolver', () => {
  let resolver: NvmeSubsystemViewBreadcrumbResolver;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    resolver = TestBed.inject(NvmeSubsystemViewBreadcrumbResolver);
  });

  it('should be created', () => {
    expect(resolver).toBeTruthy();
  });
});
