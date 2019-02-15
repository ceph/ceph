import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsetComponent, TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { AppModule } from '../../../app.module';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permissions } from '../../../shared/models/permissions';
import { PoolDetailsComponent } from './pool-details.component';

describe('PoolDetailsComponent', () => {
  let poolDetailsComponent: PoolDetailsComponent;
  let fixture: ComponentFixture<PoolDetailsComponent>;

  configureTestBed({
    imports: [TabsModule.forRoot(), AppModule],
    decalarations: [PoolDetailsComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolDetailsComponent);
    poolDetailsComponent = fixture.componentInstance;
    poolDetailsComponent.selection = new CdTableSelection();
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
      poolDetailsComponent.selection.selected = [
        {
          tiers: [0],
          pool: 0
        }
      ];
      poolDetailsComponent.selection.update();
    });

    it('should recognize a tabset child', () => {
      fixture.detectChanges();
      const tabsetChild: TabsetComponent = poolDetailsComponent.tabsetChild;
      expect(tabsetChild).toBeDefined();
    });

    it('should show "Cache Tiers Details" tab if selected pool has "tiers"', () => {
      fixture.detectChanges();
      const tabs = poolDetailsComponent.tabsetChild.tabs;
      expect(tabs.length).toBe(3);
      expect(tabs[2].heading).toBe('Cache Tiers Details');
      expect(tabs[0].active).toBeTruthy();
    });

    it('should not show "Cache Tiers Details" tab if selected pool has no "tiers"', () => {
      poolDetailsComponent.selection.selected = [
        {
          tiers: []
        }
      ];
      poolDetailsComponent.selection.update();
      fixture.detectChanges();
      const tabs = poolDetailsComponent.tabsetChild.tabs;
      expect(tabs.length).toEqual(2);
      expect(tabs[0].active).toBeTruthy();
    });

    it('current active status of tabs should not change when selection is same with previour selection', () => {
      fixture.detectChanges();
      const tabs = poolDetailsComponent.tabsetChild.tabs;
      expect(tabs[0].active).toBeTruthy();

      tabs[1].active = true;
      fixture.detectChanges();
      expect(tabs[1].active).toBeTruthy();
    });
  });
});
