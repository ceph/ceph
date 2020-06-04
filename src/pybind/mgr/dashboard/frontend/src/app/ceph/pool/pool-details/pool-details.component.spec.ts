import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, i18nProviders, TabHelper } from '../../../../testing/unit-test-helper';
import { Permissions } from '../../../shared/models/permissions';
import { SharedModule } from '../../../shared/shared.module';
import { RbdConfigurationListComponent } from '../../block/rbd-configuration-list/rbd-configuration-list.component';
import { PoolDetailsComponent } from './pool-details.component';

describe('PoolDetailsComponent', () => {
  let poolDetailsComponent: PoolDetailsComponent;
  let fixture: ComponentFixture<PoolDetailsComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      NgbNavModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule
    ],
    declarations: [PoolDetailsComponent, RbdConfigurationListComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolDetailsComponent);
    poolDetailsComponent = fixture.componentInstance;
    poolDetailsComponent.selection = undefined;
    poolDetailsComponent.permissions = new Permissions({
      grafana: ['read']
    });
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(poolDetailsComponent).toBeTruthy();
  });

  describe('Pool details tabset', () => {
    beforeEach(() => {
      poolDetailsComponent.selection = {
        tiers: [0],
        pool: 0
      };
    });

    it('should recognize a tabset child', () => {
      fixture.detectChanges();
      const ngbNav = TabHelper.getNgbNav(fixture);
      expect(ngbNav).toBeDefined();
    });

    it('should show "Cache Tiers Details" tab if selected pool has "tiers"', () => {
      fixture.detectChanges();
      const tabsItem = TabHelper.getNgbNavItems(fixture);
      const tabsText = TabHelper.getTextContents(fixture);
      expect(tabsItem.length).toBe(3);
      expect(tabsText[2]).toBe('Cache Tiers Details');
      expect(tabsItem[0].active).toBeTruthy();
    });

    it('should not show "Cache Tiers Details" tab if selected pool has no "tiers"', () => {
      poolDetailsComponent.selection = {
        tiers: []
      };
      fixture.detectChanges();
      const tabs = TabHelper.getNgbNavItems(fixture);
      expect(tabs.length).toEqual(2);
      expect(tabs[0].active).toBeTruthy();
    });

    it('current active status of tabs should not change when selection is the same as previous selection', () => {
      fixture.detectChanges();
      const tabs = TabHelper.getNgbNavItems(fixture);
      expect(tabs[0].active).toBeTruthy();

      const ngbNav = TabHelper.getNgbNav(fixture);
      ngbNav.select(tabs[1].id);

      fixture.detectChanges();
      expect(tabs[1].active).toBeTruthy();
    });

    it('returns pool details correctly', () => {
      const pool = { prop1: 1, cdIsBinary: true, prop2: 2, cdExecuting: true, prop3: 3 };
      const expectedPool = { prop1: 1, prop2: 2, prop3: 3 };

      expect(poolDetailsComponent.filterNonPoolData(pool)).toEqual(expectedPool);
    });

    it('pool data filtering is called', () => {
      const filterNonPoolDataSpy = spyOn(
        poolDetailsComponent,
        'filterNonPoolData'
      ).and.callThrough();

      fixture.detectChanges();

      expect(filterNonPoolDataSpy).toHaveBeenCalled();
    });
  });
});
