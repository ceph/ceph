import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
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

  configureTestBed({
    declarations: [TelemetryComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ]
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

    it('should show/hide ident fields on checking/unchecking', () => {
      const getContactField = () =>
        fixture.debugElement.nativeElement.querySelector('input[id=contact]');
      const getDescriptionField = () =>
        fixture.debugElement.nativeElement.querySelector('input[id=description]');
      const checkVisibility = () => {
        if (component.showContactInfo) {
          expect(getContactField()).toBeTruthy();
          expect(getDescriptionField()).toBeTruthy();
        } else {
          expect(getContactField()).toBeFalsy();
          expect(getDescriptionField()).toBeFalsy();
        }
      };

      // Initial check.
      checkVisibility();

      // toggle fields.
      component.toggleIdent();
      fixture.detectChanges();
      checkVisibility();

      // toggle fields again.
      component.toggleIdent();
      fixture.detectChanges();
      checkVisibility();
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
      expect(
        JSON.parse(
          component.replacerTest({
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
          })
        )
      ).toStrictEqual({
        ranges: ['[null,-1]', '[0,511]', '[512,1023]'],
        values: ['[0,0,0]', '[0,0,0]', '[0,0,0]'],
        other: [
          [0, 0, 0],
          [0, 0, 0],
          [0, 0, 0]
        ]
      });

      expect(
        JSON.parse(
          component.replacerTest({
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
            other: true
          })
        )
      ).toStrictEqual({
        ranges: ['[null,-1]', '[0,511]', '[512,1023]'],
        values: ['[0,0,0]', '[0,0,0]', '[0,0,0]'],
        other: true
      });

      expect(
        JSON.parse(
          component.replacerTest({
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
            other: 1
          })
        )
      ).toStrictEqual({
        ranges: ['[null,-1]', '[0,511]', '[512,1023]'],
        values: ['[0,0,0]', '[0,0,0]', '[0,0,0]'],
        other: 1
      });

      expect(
        JSON.parse(
          component.replacerTest({
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
            other: { value: 0 }
          })
        )
      ).toStrictEqual({
        ranges: ['[null,-1]', '[0,511]', '[512,1023]'],
        values: ['[0,0,0]', '[0,0,0]', '[0,0,0]'],
        other: { value: 0 }
      });
    });

    it('should remove perf channel fields from a report', () => {
      expect(
        JSON.parse(
          component.formatReportTest({
            perf_counters: {},
            stats_per_pool: {},
            stats_per_pg: {},
            io_rate: {},
            osd_perf_histograms: {},
            mempool: {},
            heap_stats: {},
            rocksdb_stats: {}
          })
        )
      ).toStrictEqual({});

      expect(
        JSON.parse(
          component.formatReportTest({
            perf_counters: {},
            stats_per_pool: {},
            stats_per_pg: {},
            io_rate: {},
            osd_perf_histograms: {},
            mempool: {},
            heap_stats: {},
            rocksdb_stats: {},
            other: {}
          })
        )
      ).toStrictEqual({
        other: {}
      });
    });

    it('should submit', () => {
      component.onSubmit();
      const req1 = httpTesting.expectOne('api/telemetry');
      expect(req1.request.method).toBe('PUT');
      expect(req1.request.body).toEqual({
        enable: true,
        license_name: 'sharing-1-0'
      });
      req1.flush({});
      const req2 = httpTesting.expectOne({
        url: 'api/mgr/module/telemetry',
        method: 'PUT'
      });
      expect(req2.request.body).toEqual({
        config: {}
      });
      req2.flush({});
      expect(router.url).toBe('/');
    });
  });
});
