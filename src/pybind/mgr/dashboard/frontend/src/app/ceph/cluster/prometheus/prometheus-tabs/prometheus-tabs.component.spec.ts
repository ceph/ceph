import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permission } from '~/app/shared/models/permissions';
import { configureTestBed } from '~/testing/unit-test-helper';
import { PrometheusTabsComponent } from './prometheus-tabs.component';
import { By } from '@angular/platform-browser';

describe('PrometheusTabsComponent', () => {
  let component: PrometheusTabsComponent;
  let fixture: ComponentFixture<PrometheusTabsComponent>;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [PrometheusTabsComponent],
    providers: [
      { provide: PrometheusAlertService, useValue: { alerts: [] } },
      {
        provide: AuthStorageService,
        useValue: {
          getPermissions: () => ({
            prometheus: new Permission(['read'])
          })
        }
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PrometheusTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display three tabs when user has read permission', () => {
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.length).toBe(3);
  });
});
