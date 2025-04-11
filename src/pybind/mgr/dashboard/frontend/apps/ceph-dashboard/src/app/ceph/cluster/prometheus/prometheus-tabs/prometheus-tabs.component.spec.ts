import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { PrometheusTabsComponent } from './prometheus-tabs.component';

describe('PrometheusTabsComponent', () => {
  let component: PrometheusTabsComponent;
  let fixture: ComponentFixture<PrometheusTabsComponent>;

  configureTestBed({
    imports: [RouterTestingModule, NgbNavModule],
    declarations: [PrometheusTabsComponent],
    providers: [{ provide: PrometheusAlertService, useValue: { alerts: [] } }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PrometheusTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
