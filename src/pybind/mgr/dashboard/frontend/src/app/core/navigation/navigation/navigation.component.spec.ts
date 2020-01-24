import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { AppModule } from '../../../app.module';
import { PrometheusService } from '../../../shared/api/prometheus.service';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NavigationComponent } from './navigation.component';

describe('NavigationComponent', () => {
  let component: NavigationComponent;
  let fixture: ComponentFixture<NavigationComponent>;
  let ifAlertmanagerConfiguredSpy: jasmine.Spy;
  let ifPrometheusConfigured: jasmine.Spy;

  configureTestBed({
    imports: [AppModule],
    providers: i18nProviders
  });

  beforeEach(() => {
    ifAlertmanagerConfiguredSpy = spyOn(
      TestBed.get(PrometheusService),
      'ifAlertmanagerConfigured'
    ).and.stub();
    ifPrometheusConfigured = spyOn(
      TestBed.get(PrometheusService),
      'ifPrometheusConfigured'
    ).and.stub();
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create and PrometheusService methods should not have been called', () => {
    expect(component).toBeTruthy();
    expect(ifAlertmanagerConfiguredSpy).toHaveBeenCalledTimes(0);
    expect(ifPrometheusConfigured).toHaveBeenCalledTimes(0);
  });

  it('PrometheusService methods should have been called', () => {
    const authStorageServiceSpy = spyOn(
      TestBed.get(AuthStorageService),
      'getPermissions'
    ).and.returnValue(new Permissions({ 'config-opt': ['read'] }));
    TestBed.overrideProvider(AuthStorageService, { useValue: authStorageServiceSpy });
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    expect(ifAlertmanagerConfiguredSpy).toHaveBeenCalledTimes(1);
    expect(ifPrometheusConfigured).toHaveBeenCalledTimes(1);
  });
});
