import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ChangeDetectorRef } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { RbdConfigurationListComponent } from '~/app/ceph/block/rbd-configuration-list/rbd-configuration-list.component';
import { Permissions } from '~/app/shared/models/permissions';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, Mocks, TabHelper } from '~/testing/unit-test-helper';
import { PoolDetailsComponent } from './pool-details.component';

describe('PoolDetailsComponent', () => {
  let poolDetailsComponent: PoolDetailsComponent;
  let fixture: ComponentFixture<PoolDetailsComponent>;

  // Needed because of ChangeDetectionStrategy.OnPush
  // https://github.com/angular/angular/issues/12313#issuecomment-444623173
  let changeDetector: ChangeDetectorRef;
  const detectChanges = () => {
    poolDetailsComponent.ngOnChanges();
    changeDetector.detectChanges(); // won't call ngOnChanges on it's own but updates fixture
  };

  const updatePoolSelection = (selection: any) => {
    poolDetailsComponent.selection = selection;
    detectChanges();
  };

  const currentPoolUpdate = () => {
    updatePoolSelection(poolDetailsComponent.selection);
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      NgbNavModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule
    ],
    declarations: [PoolDetailsComponent, RbdConfigurationListComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolDetailsComponent);
    // Needed because of ChangeDetectionStrategy.OnPush
    // https://github.com/angular/angular/issues/12313#issuecomment-444623173
    changeDetector = fixture.componentRef.injector.get(ChangeDetectorRef);
    poolDetailsComponent = fixture.componentInstance;
    poolDetailsComponent.selection = undefined;
    poolDetailsComponent.permissions = new Permissions({
      grafana: ['read']
    });
    updatePoolSelection({ tiers: [0], pool: 0, pool_name: 'micro_pool' });
  });

  it('should create', () => {
    expect(poolDetailsComponent).toBeTruthy();
  });

  describe('Pool details tabset', () => {
    it('should recognize a tabset child', () => {
      detectChanges();
      const ngbNav = TabHelper.getNgbNav(fixture);
      expect(ngbNav).toBeDefined();
    });

    it('should not change the tabs active status when selection is the same as before', () => {
      const tabs = TabHelper.getNgbNavItems(fixture);
      expect(tabs[0].active).toBeTruthy();
      currentPoolUpdate();
      expect(tabs[0].active).toBeTruthy();

      const ngbNav = TabHelper.getNgbNav(fixture);
      ngbNav.select(tabs[1].id);
      expect(tabs[1].active).toBeTruthy();
      currentPoolUpdate();
      expect(tabs[1].active).toBeTruthy();
    });

    it('should filter out cdExecuting, cdIsBinary and all stats', () => {
      updatePoolSelection({
        prop1: 1,
        cdIsBinary: true,
        prop2: 2,
        cdExecuting: true,
        prop3: 3,
        stats: { anyStat: 3, otherStat: [1, 2, 3] }
      });
      const expectedPool = { prop1: 1, prop2: 2, prop3: 3 };
      expect(poolDetailsComponent.poolDetails).toEqual(expectedPool);
    });

    describe('Updates of shown data', () => {
      const expectedChange = (
        expected: {
          selectedPoolConfiguration?: object;
          poolDetails?: object;
        },
        newSelection: object,
        doesNotEqualOld = true
      ) => {
        const getData = () => {
          const data = {};
          Object.keys(expected).forEach((key) => (data[key] = poolDetailsComponent[key]));
          return data;
        };
        const oldData = getData();
        updatePoolSelection(newSelection);
        const newData = getData();
        if (doesNotEqualOld) {
          expect(expected).not.toEqual(oldData);
        } else {
          expect(expected).toEqual(oldData);
        }
        expect(expected).toEqual(newData);
      };

      it('should update shown data on change', () => {
        expectedChange(
          {
            poolDetails: {
              application_metadata: ['rbd'],
              pg_num: 256,
              pg_num_target: 256,
              pg_placement_num: 256,
              pg_placement_num_target: 256,
              pool: 2,
              pool_name: 'somePool',
              type: 'replicated',
              size: 3
            }
          },
          Mocks.getPool('somePool', 2)
        );
      });

      it('should not update shown data if no detail has changed on pool refresh', () => {
        expectedChange(
          {
            poolDetails: {
              pool: 0,
              pool_name: 'micro_pool',
              tiers: [0]
            }
          },
          poolDetailsComponent.selection,
          false
        );
      });

      it('should show "Cache Tiers Details" tab if selected pool has "tiers"', () => {
        const tabsItem = TabHelper.getNgbNavItems(fixture);
        const tabsText = TabHelper.getTextContents(fixture);
        expect(poolDetailsComponent.selection['tiers'].length).toBe(1);
        expect(tabsItem.length).toBe(3);
        expect(tabsText[2]).toBe('Cache Tiers Details');
        expect(tabsItem[0].active).toBeTruthy();
      });

      it('should not show "Cache Tiers Details" tab if selected pool has no "tiers"', () => {
        updatePoolSelection({ tiers: [] });
        const tabs = TabHelper.getNgbNavItems(fixture);
        expect(tabs.length).toEqual(2);
        expect(tabs[0].active).toBeTruthy();
      });
    });
  });
});
