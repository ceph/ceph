import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';

import { ToastModule } from 'ng2-toastr';
import { PopoverModule } from 'ngx-bootstrap/popover';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { PrometheusService } from '../../../shared/api/prometheus.service';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { PrometheusAlertService } from '../../../shared/services/prometheus-alert.service';
import { PrometheusNotificationService } from '../../../shared/services/prometheus-notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { NotificationsComponent } from './notifications.component';

describe('NotificationsComponent', () => {
  let component: NotificationsComponent;
  let fixture: ComponentFixture<NotificationsComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      PopoverModule.forRoot(),
      SharedModule,
      ToastModule.forRoot()
    ],
    declarations: [NotificationsComponent],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationsComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('prometheus alert handling', () => {
    let prometheusAlertService: PrometheusAlertService;
    let prometheusNotificationService: PrometheusNotificationService;
    let prometheusAccessAllowed: boolean;

    const expectPrometheusServicesToBeCalledTimes = (n: number) => {
      expect(prometheusNotificationService.refresh).toHaveBeenCalledTimes(n);
      expect(prometheusAlertService.refresh).toHaveBeenCalledTimes(n);
    };

    beforeEach(() => {
      prometheusAccessAllowed = true;
      spyOn(TestBed.get(AuthStorageService), 'getPermissions').and.callFake(() => ({
        prometheus: { read: prometheusAccessAllowed }
      }));

      spyOn(TestBed.get(PrometheusService), 'ifAlertmanagerConfigured').and.callFake((fn) => fn());

      prometheusAlertService = TestBed.get(PrometheusAlertService);
      spyOn(prometheusAlertService, 'refresh').and.stub();

      prometheusNotificationService = TestBed.get(PrometheusNotificationService);
      spyOn(prometheusNotificationService, 'refresh').and.stub();
    });

    it('should not refresh prometheus services if not allowed', () => {
      prometheusAccessAllowed = false;
      fixture.detectChanges();

      expectPrometheusServicesToBeCalledTimes(0);
    });
    it('should first refresh prometheus notifications and alerts during init', () => {
      fixture.detectChanges();

      expect(prometheusAlertService.refresh).toHaveBeenCalledTimes(1);
      expectPrometheusServicesToBeCalledTimes(1);
    });

    it('should refresh prometheus services every 5s', fakeAsync(() => {
      fixture.detectChanges();

      expectPrometheusServicesToBeCalledTimes(1);
      tick(5000);
      expectPrometheusServicesToBeCalledTimes(2);
      tick(15000);
      expectPrometheusServicesToBeCalledTimes(5);
      component.ngOnDestroy();
    }));
  });
});
