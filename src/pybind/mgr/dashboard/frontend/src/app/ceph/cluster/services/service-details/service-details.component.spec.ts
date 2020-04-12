import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { CoreModule } from '../../../../core/core.module';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../../shared/shared.module';
import { CephModule } from '../../../ceph.module';
import { ServiceDetailsComponent } from './service-details.component';

describe('ServiceDetailsComponent', () => {
  let component: ServiceDetailsComponent;
  let fixture: ComponentFixture<ServiceDetailsComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, CephModule, CoreModule, SharedModule],
    declarations: [],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServiceDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Service details tabset', () => {
    beforeEach(() => {
      component.selection.selected = [{ serviceName: 'osd' }];
      fixture.detectChanges();
    });

    it('should recognize a tabset child', () => {
      const tabsetChild = component.tabsetChild;
      expect(tabsetChild).toBeDefined();
    });

    it('should show tabs', () => {
      expect(component.tabsetChild.tabs.map((t) => t.heading)).toEqual(['Daemons']);
    });
  });
});
