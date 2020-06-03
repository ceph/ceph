import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { SummaryService } from '../../../../shared/services/summary.service';
import { SharedModule } from '../../../../shared/shared.module';
import { ServiceDaemonListComponent } from '../service-daemon-list/service-daemon-list.component';
import { ServiceDetailsComponent } from './service-details.component';

describe('ServiceDetailsComponent', () => {
  let component: ServiceDetailsComponent;
  let fixture: ComponentFixture<ServiceDetailsComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, TabsModule.forRoot(), SharedModule],
    declarations: [ServiceDetailsComponent, ServiceDaemonListComponent],
    providers: [
      i18nProviders,
      {
        provide: SummaryService,
        useValue: {
          subscribeOnce: jest.fn()
        }
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServiceDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
  });

  it('should create', () => {
    fixture.detectChanges();
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
