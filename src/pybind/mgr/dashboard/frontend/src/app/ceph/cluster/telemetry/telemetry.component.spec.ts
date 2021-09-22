import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { DownloadButtonComponent } from '~/app/shared/components/download-button/download-button.component';
import { LoadingPanelComponent } from '~/app/shared/components/loading-panel/loading-panel.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TelemetryComponent } from './telemetry.component';

describe('TelemetryComponent', () => {
  let component: TelemetryComponent;
  let fixture: ComponentFixture<TelemetryComponent>;
  let mgrModuleService: MgrModuleService;
  let options: any;
  let configs: any;
  let httpTesting: HttpTestingController;
  let router: Router;

  const optionsNames = [
    'channel_basic',
    'channel_crash',
    'channel_device',
    'channel_ident',
    'channel_perf',
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

  configureTestBed(
    {
      declarations: [TelemetryComponent],
      imports: [
        HttpClientTestingModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        ToastrModule.forRoot()
      ]
    },
    [LoadingPanelComponent, DownloadButtonComponent]
  );

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

    it('should show/hide ident fields on checking/unchecking', () => {
      const getContactField = () =>
        fixture.debugElement.nativeElement.querySelector('input[id=contact]');
      const getDescriptionField = () =>
        fixture.debugElement.nativeElement.querySelector('input[id=description]');

      // Initially hidden.
      expect(getContactField()).toBeFalsy();
      expect(getDescriptionField()).toBeFalsy();

      // Show fields.
      component.toggleIdent();
      fixture.detectChanges();
      expect(getContactField()).toBeTruthy();
      expect(getDescriptionField()).toBeTruthy();

      // Hide fields.
      component.toggleIdent();
      fixture.detectChanges();
      expect(getContactField()).toBeFalsy();
      expect(getDescriptionField()).toBeFalsy();
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
    beforeEach(() => {
      fixture = TestBed.createComponent(TelemetryComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();
      httpTesting = TestBed.inject(HttpTestingController);
      router = TestBed.inject(Router);
      spyOn(router, 'navigate');
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should only replace the ranges and values of a JSON object', () => {
      let report = component.replacerTest({
        ranges: [
          [null, -1],
          [0, 511],
          [512, 1023]
        ],
        values: [
          [0, 0, 0],
          [0, 0, 0],
          [0, 0, 0]
        ],
        other: [
          [0, 0, 0],
          [0, 0, 0],
          [0, 0, 0]
        ]
      });
      report = JSON.parse(report);

      // Ensure that the outer arrays have remained untouched by replacer
      expect(
        Array.isArray(report['ranges']) &&
          Array.isArray(report['values']) &&
          Array.isArray(report['other'])
      ).toBeTruthy();

      if (
        Array.isArray(report['ranges']) &&
        Array.isArray(report['values']) &&
        Array.isArray(report['other'])
      ) {
        let idx;

        // Check that each range in 'ranges' was replaced by a string
        for (idx = 0; idx < report['ranges'].length; idx++) {
          expect(typeof report['ranges'][idx] === 'string').toBeTruthy();
        }

        // Check that each value in 'values' was replaced by a string
        for (idx = 0; idx < report['values'].length; idx++) {
          expect(typeof report['values'][idx] === 'string').toBeTruthy();
        }

        // Check that each value in 'other' has remained untouched, as it is not a value or range
        for (idx = 0; idx < report['other'].length; idx++) {
          expect(Array.isArray(report['other'][idx])).toBeTruthy();
        }
      }
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
