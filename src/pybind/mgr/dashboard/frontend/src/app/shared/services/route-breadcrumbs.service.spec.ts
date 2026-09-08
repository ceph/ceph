import { TestBed } from '@angular/core/testing';
import { ActivatedRouteSnapshot } from '@angular/router';
import { of } from 'rxjs';

import { RouteBreadcrumbsService } from './route-breadcrumbs.service';
import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { AppConstants } from '~/app/shared/constants/app.constants';

// --- MOCK CUSTOM RESOLVERS ---
class MockCustomResolver extends BreadcrumbsResolver {
  resolve() {
    return [{ text: 'Custom Resolved', path: '/custom' }];
  }
}

class MockPromiseResolver extends BreadcrumbsResolver {
  resolve() {
    return Promise.resolve([{ text: 'Promise Resolved', path: '/promise' }]);
  }
}

class MockObservableResolver extends BreadcrumbsResolver {
  resolve() {
    return of([{ text: 'Observable Resolved', path: '/observable' }]);
  }
}

describe('RouteBreadcrumbsService', () => {
  let service: RouteBreadcrumbsService;

  // Helper to create mock ActivatedRouteSnapshots
  const createMockRoute = (data?: any, firstChild?: any): ActivatedRouteSnapshot => {
    return {
      routeConfig: { data },
      firstChild
    } as unknown as ActivatedRouteSnapshot;
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        RouteBreadcrumbsService,
        MockCustomResolver,
        MockPromiseResolver,
        MockObservableResolver
      ]
    });
    service = TestBed.inject(RouteBreadcrumbsService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('getTitleFromCrumbs', () => {
    it('should return just the project name if crumbs array is empty', () => {
      const title = service.getTitleFromCrumbs([]);
      expect(title).toBe(AppConstants.projectName);
    });

    it('should combine project name and crumb texts with > separator', () => {
      const crumbs: IBreadcrumb[] = [
        { text: 'Cluster', path: 'cluster' },
        { text: 'Hosts', path: 'hosts' }
      ];

      const title = service.getTitleFromCrumbs(crumbs);
      expect(title).toBe(`${AppConstants.projectName}: Cluster > Hosts`);
    });

    it('should handle crumbs with empty text securely', () => {
      const crumbs: IBreadcrumb[] = [
        { text: '', path: 'empty' },
        { text: 'Valid', path: 'valid' }
      ];

      const title = service.getTitleFromCrumbs(crumbs);
      expect(title).toBe(`${AppConstants.projectName}:  > Valid`);
    });
  });

  describe('resolve', () => {
    let defaultResolverSpy: jest.SpyInstance;

    beforeEach(() => {
      // Mock the default BreadcrumbsResolver to return predictable standard data
      defaultResolverSpy = jest
        .spyOn(BreadcrumbsResolver.prototype, 'resolve')
        .mockImplementation((route: any) => {
          return [{ text: route.routeConfig?.data?.breadcrumbs || 'Default', path: '/default' }];
        });
    });

    it('should return an empty array if there are no breadcrumbs in the route tree', (done) => {
      const route = createMockRoute(null, createMockRoute(null));

      service.resolve(route).subscribe((crumbs) => {
        expect(crumbs).toEqual([]);
        done();
      });
    });

    it('should traverse child routes and concatenate breadcrumbs', (done) => {
      const route = createMockRoute(
        { breadcrumbs: 'Parent' },
        createMockRoute({ breadcrumbs: 'Child' })
      );

      service.resolve(route).subscribe((crumbs) => {
        expect(crumbs).toEqual([
          { text: 'Parent', path: '/default' },
          { text: 'Child', path: '/default' }
        ]);
        done();
      });
    });

    it('should remove duplicate breadcrumbs based on text via RxJS distinct', (done) => {
      const route = createMockRoute(
        { breadcrumbs: 'DuplicateText' },
        createMockRoute({ breadcrumbs: 'DuplicateText' })
      );

      service.resolve(route).subscribe((crumbs) => {
        // Only one of them should survive the 'distinct' operator
        expect(crumbs.length).toBe(1);
        expect(crumbs[0].text).toBe('DuplicateText');
        done();
      });
    });

    it('should use custom class BreadcrumbsResolver from Injector if provided', (done) => {
      const route = createMockRoute({ breadcrumbs: MockCustomResolver });

      service.resolve(route).subscribe((crumbs) => {
        expect(crumbs).toEqual([{ text: 'Custom Resolved', path: '/custom' }]);
        expect(defaultResolverSpy).not.toHaveBeenCalled();
        done();
      });
    });

    it('should correctly wrap and resolve a custom resolver returning a Promise', (done) => {
      const route = createMockRoute({ breadcrumbs: MockPromiseResolver });

      service.resolve(route).subscribe((crumbs) => {
        expect(crumbs).toEqual([{ text: 'Promise Resolved', path: '/promise' }]);
        done();
      });
    });

    it('should correctly wrap and resolve a custom resolver returning an Observable', (done) => {
      const route = createMockRoute({ breadcrumbs: MockObservableResolver });

      service.resolve(route).subscribe((crumbs) => {
        expect(crumbs).toEqual([{ text: 'Observable Resolved', path: '/observable' }]);
        done();
      });
    });

    describe('postProcess splitting logic', () => {
      beforeEach(() => {
        // Return exactly what we provide in data.breadcrumbs for precise postProcess testing
        defaultResolverSpy.mockImplementation((route: any) => {
          return route.routeConfig.data.breadcrumbs;
        });
      });

      it('should split breadcrumbs containing "/" into multiple non-clickable parents', (done) => {
        const route = createMockRoute({
          breadcrumbs: [{ text: 'Storage/Pools', path: '/storage/pools' }]
        });

        service.resolve(route).subscribe((crumbs) => {
          expect(crumbs.length).toBe(2);
          // Split parent (unclickable)
          expect(crumbs[0]).toEqual({ text: 'Storage', path: null });
          // Split child (clickable)
          expect(crumbs[1]).toEqual({ text: 'Pools', path: '/storage/pools' });
          done();
        });
      });

      it('should NOT split breadcrumbs if disableSplit is set to true', (done) => {
        const route = createMockRoute({
          breadcrumbs: [{ text: 'A/B/C', path: '/abc', disableSplit: true }]
        });

        service.resolve(route).subscribe((crumbs) => {
          expect(crumbs.length).toBe(1);
          // Leaves text exactly as is
          expect(crumbs[0]).toEqual({ text: 'A/B/C', path: '/abc', disableSplit: true });
          done();
        });
      });
    });
  });
});
