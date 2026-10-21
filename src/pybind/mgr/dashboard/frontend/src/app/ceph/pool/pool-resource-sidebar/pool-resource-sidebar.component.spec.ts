import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { PoolService } from '~/app/shared/api/pool.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { PoolResourceSidebarComponent } from './pool-resource-sidebar.component';
import { of } from 'rxjs';

describe('PoolResourceSidebarComponent', () => {
  let poolResourceSidebarComponent: PoolResourceSidebarComponent;
  let fixture: ComponentFixture<PoolResourceSidebarComponent>;

  const fakeAuthStorageService = {
    getPermissions: () => ({ grafana: { read: true } })
  };

  configureTestBed({
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, RouterTestingModule],
    declarations: [PoolResourceSidebarComponent],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      {
        provide: ActivatedRoute,
        useValue: {
          paramMap: of(convertToParamMap({ name: 'micro_pool' }))
        }
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolResourceSidebarComponent);
    poolResourceSidebarComponent = fixture.componentInstance;
  });

  it('should create', () => {
    expect(poolResourceSidebarComponent).toBeTruthy();
  });

  describe('Pool resource layout', () => {
    beforeEach(() => {
      spyOn(TestBed.inject(PoolService), 'get').and.returnValue(
        of({
          pool_name: 'micro_pool',
          tiers: [
            {
              pool_name: 'tier_pool',
              cache_mode: 'writeback'
            }
          ],
          cdExecuting: true,
          stats: { bytes_used: { latest: 1 } }
        })
      );
      spyOn(TestBed.inject(PoolService), 'getConfiguration').and.returnValue(of([]));
      fixture.detectChanges();
    });

    it('should render the sidebar layout', () => {
      const layout = fixture.nativeElement.querySelector('cd-sidebar-layout');
      expect(layout).toBeTruthy();
    });

    it('should build sidebar items', () => {
      expect(poolResourceSidebarComponent.sidebarItems.map((item) => item.label)).toEqual([
        'Overview',
        'Configuration',
        'Performance'
      ]);
    });

    it('should set the pool name title', () => {
      expect(poolResourceSidebarComponent.poolName).toBe('micro_pool');
    });
  });

  describe('pool migration state', () => {
    const load = (poolData: object) => {
      spyOn(TestBed.inject(PoolService), 'get').and.returnValue(
        of({ pool_name: 'micro_pool', ...poolData })
      );
      spyOn(TestBed.inject(PoolService), 'getConfiguration').and.returnValue(of([]));
      fixture.detectChanges();
    };

    it('should not flag a pool without migration peers as migrating', () => {
      load({});
      expect(poolResourceSidebarComponent.poolIsMigrating).toBe(false);
    });

    it('should treat pool id 0 as a valid migration source', () => {
      load({ migration_src: 0 });
      expect(poolResourceSidebarComponent.poolIsMigrating).toBe(true);
    });

    it('should treat pool id 0 as a valid migration target', () => {
      load({ migration_target: 0 });
      expect(poolResourceSidebarComponent.poolIsMigrating).toBe(true);
    });
  });
});
