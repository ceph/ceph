import { Route, Router } from '@angular/router';
import { TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { RoutedBlockModule } from './block.module';

describe('RoutedBlockModule', () => {
  let router: Router;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RouterTestingModule, RoutedBlockModule]
    }).compileComponents();

    router = TestBed.inject(Router);
  });

  const findRoute = (routes: Route[], path: string): Route | undefined => {
    for (const route of routes) {
      if (route.path === path) {
        return route;
      }

      if (route.children?.length) {
        const match = findRoute(route.children, path);
        if (match) {
          return match;
        }
      }
    }

    return undefined;
  };

  it('should route NVMe/TCP setup to the gateway group create flow', () => {
    const nvmeofRoute = findRoute(router.config, 'nvmeof');

    expect(nvmeofRoute?.data?.moduleStatusGuardConfig?.button_route).toBe(
      '/block/nvmeof/gateways/create'
    );
  });
});
