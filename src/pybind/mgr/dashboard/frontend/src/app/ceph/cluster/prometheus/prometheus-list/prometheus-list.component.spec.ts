import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { PrometheusTabsComponent } from '../prometheus-tabs/prometheus-tabs.component';
import { PrometheusListComponent } from './prometheus-list.component';

describe('PrometheusListComponent', () => {
  let component: PrometheusListComponent;
  let fixture: ComponentFixture<PrometheusListComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      TabsModule.forRoot(),
      RouterTestingModule,
      ToastModule.forRoot(),
      SharedModule
    ],
    declarations: [PrometheusListComponent, PrometheusTabsComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PrometheusListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
  it('should show the actions to add a silence, edit the currently used silence and expire the silence', () => {});
  it('should be possible to see silence details in a details tab', () => {});
});
