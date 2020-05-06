import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import * as _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { MgrModuleService } from '../../../shared/api/mgr-module.service';
import { TelemetryService } from '../../../shared/api/telemetry.service';

import { TextToDownloadService } from '../../../shared/services/text-to-download.service';
import { SharedModule } from '../../../shared/shared.module';
import { TelemetryComponent } from './telemetry.component';

describe('TelemetryComponent', () => {
  let component: TelemetryComponent;
  let fixture: ComponentFixture<TelemetryComponent>;
  let mgrModuleService: MgrModuleService;
  let telemetryService: TelemetryService;
  let options: any;
  let configs: any;
  let httpTesting: HttpTestingController;
  let router: Router;

  const optionsNames = [
    'channel_basic',
    'channel_crash',
    'channel_device',
    'channel_ident',
    'contact',
    'description',
    'device_url',
    'enabled',
    'interval',
    'last_opt_revision',
    'leaderboard',
    'log_level',
    'log_to_cluster',
    'log_to_cluster_level',
    'log_to_file',
    'organization',
    'proxy',
    'url'
  ];

  configureTestBed({
    declarations: [TelemetryComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ],
    providers: i18nProviders
  });

  describe('configForm', () => {
    beforeEach(() => {
      fixture = TestBed.createComponent(TelemetryComponent);
      component = fixture.componentInstance;
      mgrModuleService = TestBed.inject(MgrModuleService);
      options = {};
      configs = {};
      optionsNames.forEach((name) => (options[name] = { name }));
      optionsNames.forEach((name) => (configs[name] = true));
      spyOn(mgrModuleService, 'getOptions').and.callFake(() => observableOf(options));
      spyOn(mgrModuleService, 'getConfig').and.callFake(() => observableOf(configs));
      fixture.detectChanges();
      httpTesting = TestBed.inject(HttpTestingController);
      router = TestBed.inject(Router);
      spyOn(router, 'navigate');
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should set module enability to true correctly', () => {
      expect(component.moduleEnabled).toBeTruthy();
    });

    it('should set module enability to false correctly', () => {
      configs['enabled'] = false;
      component.ngOnInit();
      expect(component.moduleEnabled).toBeFalsy();
    });

    it('should filter options list correctly', () => {
      _.forEach(Object.keys(component.options), (option) => {
        expect(component.requiredFields).toContain(option);
      });
    });

    it('should update the Telemetry configuration', () => {
      component.updateConfig();
      const req = httpTesting.expectOne('api/mgr/module/telemetry');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        config: {}
      });
      req.flush({});
    });

    it('should disable the Telemetry module', () => {
      const message = 'Module disabled message.';
      const followUpFunc = function () {
        return 'followUp';
      };
      component.disableModule(message, followUpFunc);
      const req = httpTesting.expectOne('api/telemetry');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        enable: false
      });
      req.flush({});
    });

    it('should disable the Telemetry module with default parameters', () => {
      component.disableModule();
      const req = httpTesting.expectOne('api/telemetry');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        enable: false
      });
      req.flush({});
      expect(router.navigate).toHaveBeenCalledWith(['']);
    });
  });

  describe('previewForm', () => {
    const reportText = {
      testA: 'testA',
      testB: 'testB'
    };

    beforeEach(() => {
      fixture = TestBed.createComponent(TelemetryComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();
      telemetryService = TestBed.inject(TelemetryService);
      httpTesting = TestBed.inject(HttpTestingController);
      router = TestBed.inject(Router);
      spyOn(router, 'navigate');
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should call TextToDownloadService download function', () => {
      spyOn(telemetryService, 'getReport').and.returnValue(observableOf(reportText));
      component.ngOnInit();

      const downloadSpy = spyOn(TestBed.inject(TextToDownloadService), 'download');
      const filename = 'reportText.json';
      component.download(reportText, filename);
      expect(downloadSpy).toHaveBeenCalledWith(JSON.stringify(reportText, null, 2), filename);
    });

    it('should submit', () => {
      component.onSubmit();
      const req = httpTesting.expectOne('api/telemetry');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        enable: true,
        license_name: 'sharing-1-0'
      });
      req.flush({});
      expect(router.navigate).toHaveBeenCalledWith(['']);
    });
  });
});
