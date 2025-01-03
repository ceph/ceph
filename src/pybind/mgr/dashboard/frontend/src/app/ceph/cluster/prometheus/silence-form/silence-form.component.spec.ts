import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute, Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbPopoverModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import moment from 'moment';
import { ToastrModule } from 'ngx-toastr';
import { of, throwError } from 'rxjs';

import { DashboardNotFoundError } from '~/app/core/error/error';
import { ErrorComponent } from '~/app/core/error/error.component';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import {
  AlertmanagerSilence,
  AlertmanagerSilenceMatcher
} from '~/app/shared/models/alertmanager-silence';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  PrometheusHelper
} from '~/testing/unit-test-helper';
import { SilenceFormComponent } from './silence-form.component';

describe('SilenceFormComponent', () => {
  // SilenceFormComponent specific
  let component: SilenceFormComponent;
  let fixture: ComponentFixture<SilenceFormComponent>;
  let form: CdFormGroup;
  // Spied on
  let prometheusService: PrometheusService;
  let authStorageService: AuthStorageService;
  let notificationService: NotificationService;
  let router: Router;
  // Spies
  let rulesSpy: jasmine.Spy;
  let ifPrometheusSpy: jasmine.Spy;
  // Helper
  let prometheus: PrometheusHelper;
  let formHelper: FormHelper;
  let fixtureH: FixtureHelper;
  let params: Record<string, any>;
  // Date mocking related
  const baseTime = '2022-02-22 00:00';
  const beginningDate = '2022-02-22T00:00:12.35';
  let prometheusPermissions: Permission;

  const routes: Routes = [{ path: '404', component: ErrorComponent }];
  configureTestBed({
    declarations: [ErrorComponent, SilenceFormComponent],
    imports: [
      HttpClientTestingModule,
      RouterTestingModule.withRoutes(routes),
      SharedModule,
      ToastrModule.forRoot(),
      NgbTooltipModule,
      NgbPopoverModule,
      ReactiveFormsModule
    ],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: { params: { subscribe: (fn: Function) => fn(params) } }
      }
    ]
  });

  const createMatcher = (name: string, value: any, isRegex: boolean) => ({ name, value, isRegex });

  const addMatcher = (name: string, value: any, isRegex: boolean) =>
    component['setMatcher'](createMatcher(name, value, isRegex));

  const callInit = () =>
    fixture.ngZone.run(() => {
      component['init']();
    });

  const changeAction = (action: string) => {
    const modes = {
      add: '/monitoring/silences/add',
      alertAdd: '/monitoring/silences/add/alert0',
      recreate: '/monitoring/silences/recreate/someExpiredId',
      edit: '/monitoring/silences/edit/someNotExpiredId'
    };
    Object.defineProperty(router, 'url', { value: modes[action] });
    callInit();
  };

  beforeEach(() => {
    params = {};
    spyOn(Date, 'now').and.returnValue(new Date(beginningDate));

    prometheus = new PrometheusHelper();
    prometheusService = TestBed.inject(PrometheusService);
    spyOn(prometheusService, 'getAlerts').and.callFake(() => {
      const name = _.split(router.url, '/').pop();
      return of([prometheus.createAlert(name)]);
    });
    ifPrometheusSpy = spyOn(prometheusService, 'ifPrometheusConfigured').and.callFake((fn) => fn());
    rulesSpy = spyOn(prometheusService, 'getRules').and.callFake(() =>
      of({
        groups: [
          {
            file: '',
            interval: 0,
            name: '',
            rules: [
              prometheus.createRule('alert0', 'someSeverity', [prometheus.createAlert('alert0')]),
              prometheus.createRule('alert1', 'someSeverity', []),
              prometheus.createRule('alert2', 'someOtherSeverity', [
                prometheus.createAlert('alert2')
              ])
            ]
          }
        ]
      })
    );

    router = TestBed.inject(Router);

    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show').and.stub();

    authStorageService = TestBed.inject(AuthStorageService);
    spyOn(authStorageService, 'getUsername').and.returnValue('someUser');

    spyOn(authStorageService, 'getPermissions').and.callFake(() => ({
      prometheus: prometheusPermissions
    }));
    prometheusPermissions = new Permission(['update', 'delete', 'read', 'create']);
    fixture = TestBed.createComponent(SilenceFormComponent);
    fixtureH = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    form = component.form;
    formHelper = new FormHelper(form);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(_.isArray(component.rules)).toBeTruthy();
  });

  it('should have set the logged in user name as creator', () => {
    expect(component.form.getValue('createdBy')).toBe('someUser');
  });

  it('should call disablePrometheusConfig on error calling getRules', () => {
    spyOn(prometheusService, 'disablePrometheusConfig');
    rulesSpy.and.callFake(() => throwError({}));
    callInit();
    expect(component.rules).toEqual([]);
    expect(prometheusService.disablePrometheusConfig).toHaveBeenCalled();
  });

  it('should remind user if prometheus is not set when it is not configured', () => {
    ifPrometheusSpy.and.callFake((_x: any, fn: Function) => fn());
    callInit();
    expect(component.rules).toEqual([]);
    expect(notificationService.show).toHaveBeenCalledWith(
      NotificationType.info,
      'Please add your Prometheus host to the dashboard configuration and refresh the page',
      undefined,
      undefined,
      'Prometheus'
    );
  });

  describe('throw error for not allowed users', () => {
    let navigateSpy: jasmine.Spy;

    const expectError = (action: string, redirected: boolean) => {
      Object.defineProperty(router, 'url', { value: action });
      if (redirected) {
        expect(() => callInit()).toThrowError(DashboardNotFoundError);
      } else {
        expect(() => callInit()).not.toThrowError();
      }
      navigateSpy.calls.reset();
    };

    beforeEach(() => {
      navigateSpy = spyOn(router, 'navigate').and.stub();
    });

    it('should throw error if not allowed', () => {
      prometheusPermissions = new Permission(['delete', 'read']);
      expectError('add', true);
      expectError('alertAdd', true);
    });

    it('should throw error if user does not have minimum permissions to create silences', () => {
      prometheusPermissions = new Permission(['update', 'delete', 'read']);
      expectError('add', true);
      prometheusPermissions = new Permission(['update', 'delete', 'create']);
      expectError('recreate', true);
    });

    it('should throw error if user does not have minimum permissions to update silences', () => {
      prometheusPermissions = new Permission(['delete', 'read']);
      expectError('edit', true);
      prometheusPermissions = new Permission(['create', 'delete', 'update']);
      expectError('edit', true);
    });

    it('does not throw error if user has minimum permissions to create silences', () => {
      prometheusPermissions = new Permission(['create', 'read']);
      expectError('add', false);
      expectError('alertAdd', false);
      expectError('recreate', false);
    });

    it('does not throw error if user has minimum permissions to update silences', () => {
      prometheusPermissions = new Permission(['read', 'create']);
      expectError('edit', false);
    });
  });

  describe('choose the right action', () => {
    const expectMode = (routerMode: string, edit: boolean, recreate: boolean, action: string) => {
      changeAction(routerMode);
      expect(component.recreate).toBe(recreate);
      expect(component.edit).toBe(edit);
      expect(component.action).toBe(action);
    };

    beforeEach(() => {
      spyOn(prometheusService, 'getSilences').and.callFake(() => {
        const id = _.split(router.url, '/').pop();
        return of([prometheus.createSilence(id)]);
      });
    });

    it('should have no special action activate by default', () => {
      expectMode('add', false, false, 'Create');
      expect(prometheusService.getSilences).not.toHaveBeenCalled();
      expect(component.form.value).toEqual({
        comment: null,
        createdBy: 'someUser',
        duration: '2h',
        startsAt: baseTime,
        endsAt: '2022-02-22 02:00'
      });
    });

    it('should be in edit action if route includes edit', () => {
      params = { id: 'someNotExpiredId' };
      expectMode('edit', true, false, 'Edit');
      expect(prometheusService.getSilences).toHaveBeenCalled();
      expect(component.form.value).toEqual({
        comment: `A comment for ${params.id}`,
        createdBy: `Creator of ${params.id}`,
        duration: '1d',
        startsAt: '2022-02-22 22:22',
        endsAt: '2022-02-23 22:22'
      });
      expect(component.matchers).toEqual([createMatcher('job', 'someJob', true)]);
    });

    it('should be in recreation action if route includes recreate', () => {
      params = { id: 'someExpiredId' };
      expectMode('recreate', false, true, 'Recreate');
      expect(prometheusService.getSilences).toHaveBeenCalled();
      expect(component.form.value).toEqual({
        comment: `A comment for ${params.id}`,
        createdBy: `Creator of ${params.id}`,
        duration: '2h',
        startsAt: baseTime,
        endsAt: '2022-02-22 02:00'
      });
      expect(component.matchers).toEqual([createMatcher('job', 'someJob', true)]);
    });

    it('adds matchers based on the label object of the alert with the given id', () => {
      params = { id: 'alert0' };
      expectMode('alertAdd', false, false, 'Create');
      expect(prometheusService.getSilences).not.toHaveBeenCalled();
      expect(prometheusService.getAlerts).toHaveBeenCalled();
      expect(component.matchers).toEqual([createMatcher('alertname', 'alert0', false)]);
      expect(component.matcherMatch).toEqual({
        cssClass: 'has-success',
        status: 'Matches 1 rule with 1 active alert.'
      });
    });
  });

  describe('time', () => {
    const changeEndDate = (text: string) => component.form.patchValue({ endsAt: text });
    const changeStartDate = (text: string) => component.form.patchValue({ startsAt: text });

    it('have all dates set at beginning', () => {
      expect(form.getValue('startsAt')).toEqual(baseTime);
      expect(form.getValue('duration')).toBe('2h');
      expect(form.getValue('endsAt')).toEqual('2022-02-22 02:00');
    });

    describe('on start date change', () => {
      it('changes end date on start date change if it exceeds it', fakeAsync(() => {
        changeStartDate('2022-02-28 04:05');
        expect(form.getValue('duration')).toEqual('2h');
        expect(form.getValue('endsAt')).toEqual('2022-02-28 06:05');

        changeStartDate('2022-12-31 22:00');
        expect(form.getValue('duration')).toEqual('2h');
        expect(form.getValue('endsAt')).toEqual('2023-01-01 00:00');
      }));

      it('changes duration if start date does not exceed end date ', fakeAsync(() => {
        changeStartDate('2022-02-22 00:45');
        expect(form.getValue('duration')).toEqual('1h 15m');
        expect(form.getValue('endsAt')).toEqual('2022-02-22 02:00');
      }));

      it('should raise invalid start date error', fakeAsync(() => {
        changeStartDate('No valid date');
        formHelper.expectError('startsAt', 'format');
        expect(form.getValue('startsAt').toString()).toBe('No valid date');
        expect(form.getValue('endsAt')).toEqual('2022-02-22 02:00');
      }));
    });

    describe('on duration change', () => {
      it('changes end date if duration is changed', () => {
        formHelper.setValue('duration', '15m');
        expect(form.getValue('endsAt')).toEqual('2022-02-22 00:15');
        formHelper.setValue('duration', '5d 23h');
        expect(form.getValue('endsAt')).toEqual('2022-02-27 23:00');
      });
    });

    describe('on end date change', () => {
      it('changes duration on end date change if it exceeds start date', fakeAsync(() => {
        changeEndDate('2022-02-28 04:05');
        expect(form.getValue('duration')).toEqual('6d 4h 5m');
        expect(form.getValue('startsAt')).toEqual(baseTime);
      }));

      it('changes start date if end date happens before it', fakeAsync(() => {
        changeEndDate('2022-02-21 02:00');
        expect(form.getValue('duration')).toEqual('2h');
        expect(form.getValue('startsAt')).toEqual('2022-02-21 00:00');
      }));

      it('should raise invalid end date error', fakeAsync(() => {
        changeEndDate('No valid date');
        formHelper.expectError('endsAt', 'format');
        expect(form.getValue('endsAt').toString()).toBe('No valid date');
        expect(form.getValue('startsAt')).toEqual(baseTime);
      }));
    });
  });

  it('should have a creator field', () => {
    formHelper.expectValid('createdBy');
    formHelper.expectErrorChange('createdBy', '', 'required');
    formHelper.expectValidChange('createdBy', 'Mighty FSM');
  });

  it('should have a comment field', () => {
    formHelper.expectError('comment', 'required');
    formHelper.expectValidChange('comment', 'A pretty long comment');
  });

  it('should be a valid form if all inputs are filled and at least one matcher was added', () => {
    expect(form.valid).toBeFalsy();
    formHelper.expectValidChange('createdBy', 'Mighty FSM');
    formHelper.expectValidChange('comment', 'A pretty long comment');
    addMatcher('job', 'someJob', false);
    expect(form.valid).toBeTruthy();
  });

  describe('matchers', () => {
    const expectMatch = (helpText: string) => {
      expect(fixtureH.getText('#match-state')).toBe(helpText);
    };

    it('should show the add matcher button', () => {
      fixtureH.expectElementVisible('#add-matcher', true);
      fixtureH.expectIdElementsVisible(
        [
          'matcher-name-0',
          'matcher-value-0',
          'matcher-isRegex-0',
          'matcher-edit-0',
          'matcher-delete-0'
        ],
        false
      );
      expectMatch(null);
    });

    it('should show added matcher', () => {
      addMatcher('job', 'someJob', true);
      fixtureH.expectIdElementsVisible(
        ['matcher-name-0', 'matcher-value-0', 'matcher-edit-0', 'matcher-delete-0'],
        true
      );
      expectMatch(null);
    });

    it('should show multiple matchers', () => {
      addMatcher('severity', 'someSeverity', false);
      addMatcher('alertname', 'alert0', false);
      fixtureH.expectIdElementsVisible(
        [
          'matcher-name-0',
          'matcher-value-0',
          'matcher-edit-0',
          'matcher-delete-0',
          'matcher-name-1',
          'matcher-value-1',
          'matcher-edit-1',
          'matcher-delete-1'
        ],
        true
      );
      expectMatch('Matches 1 rule with 1 active alert.');
    });

    it('should show the right matcher values', () => {
      addMatcher('alertname', 'alert.*', true);
      addMatcher('job', 'someJob', false);
      fixture.detectChanges();
      fixtureH.expectFormFieldToBe('#matcher-name-0', 'alertname');
      fixtureH.expectFormFieldToBe('#matcher-value-0', 'alert.*');
      expectMatch(null);
    });

    it('should be able to edit a matcher', () => {
      addMatcher('alertname', 'alert.*', true);
      expectMatch(null);

      const modalService = TestBed.inject(ModalService);
      spyOn(modalService, 'show').and.callFake(() => {
        return {
          componentInstance: {
            preFillControls: (matcher: any) => {
              expect(matcher).toBe(component.matchers[0]);
            },
            submitAction: of({ name: 'alertname', value: 'alert0', isRegex: false })
          }
        };
      });
      fixtureH.clickElement('#matcher-edit-0');

      fixtureH.expectFormFieldToBe('#matcher-name-0', 'alertname');
      fixtureH.expectFormFieldToBe('#matcher-value-0', 'alert0');
      expectMatch('Matches 1 rule with 1 active alert.');
    });

    it('should be able to remove a matcher', () => {
      addMatcher('alertname', 'alert0', false);
      expectMatch('Matches 1 rule with 1 active alert.');
      fixtureH.clickElement('#matcher-delete-0');
      expect(component.matchers).toEqual([]);
      fixtureH.expectIdElementsVisible(
        ['matcher-name-0', 'matcher-value-0', 'matcher-isRegex-0'],
        false
      );
      expectMatch(null);
    });

    it('should be able to remove a matcher and update the matcher text', () => {
      addMatcher('alertname', 'alert0', false);
      addMatcher('alertname', 'alert1', false);
      expectMatch('Your matcher seems to match no currently defined rule or active alert.');
      fixtureH.clickElement('#matcher-delete-1');
      expectMatch('Matches 1 rule with 1 active alert.');
    });

    it('should show form as invalid if no matcher is set', () => {
      expect(form.errors).toEqual({ matcherRequired: true });
    });

    it('should show form as valid if matcher was added', () => {
      addMatcher('some name', 'some value', true);
      expect(form.errors).toEqual(null);
    });
  });

  describe('submit tests', () => {
    const endsAt = '2022-02-22 02:00';
    let silence: AlertmanagerSilence;
    const silenceId = '50M3-10N6-1D';

    const expectSuccessNotification = (
      titleStartsWith: string,
      matchers: AlertmanagerSilenceMatcher[]
    ) => {
      let msg = '';
      for (const matcher of matchers) {
        msg = msg.concat(` ${matcher.name} - ${matcher.value},`);
      }
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `${titleStartsWith} silence for ${msg.slice(0, -1)}`,
        undefined,
        undefined,
        'Prometheus'
      );
    };

    const fillAndSubmit = () => {
      ['createdBy', 'comment'].forEach((attr) => {
        formHelper.setValue(attr, silence[attr]);
      });
      silence.matchers.forEach((matcher) =>
        addMatcher(matcher.name, matcher.value, matcher.isRegex)
      );
      component.submit();
    };

    beforeEach(() => {
      spyOn(prometheusService, 'setSilence').and.callFake(() => of({ body: { silenceId } }));
      spyOn(router, 'navigate').and.stub();
      silence = {
        createdBy: 'some creator',
        comment: 'some comment',
        startsAt: moment(baseTime).toISOString(),
        endsAt: moment(endsAt).toISOString(),
        matchers: [
          {
            name: 'some attribute name',
            value: 'some value',
            isRegex: false
          },
          {
            name: 'job',
            value: 'node-exporter',
            isRegex: false
          },
          {
            name: 'instance',
            value: 'localhost:9100',
            isRegex: false
          },
          {
            name: 'alertname',
            value: 'load_0',
            isRegex: false
          }
        ]
      };
    });

    // it('should not create a silence if the form is invalid', () => {
    //   component.submit();
    //   expect(notificationService.show).not.toHaveBeenCalled();
    //   expect(form.valid).toBeFalsy();
    //   expect(prometheusService.setSilence).not.toHaveBeenCalledWith(silence);
    //   expect(router.navigate).not.toHaveBeenCalled();
    // });

    // it('should route back to previous tab on success', () => {
    //   fillAndSubmit();
    //   expect(form.valid).toBeTruthy();
    //   expect(router.navigate).toHaveBeenCalledWith(['/monitoring'], { fragment: 'silences' });
    // });

    it('should create a silence', () => {
      fillAndSubmit();
      expect(prometheusService.setSilence).toHaveBeenCalledWith(silence);
      expectSuccessNotification('Created', silence.matchers);
    });

    it('should recreate a silence', () => {
      component.recreate = true;
      component.id = 'recreateId';
      fillAndSubmit();
      expect(prometheusService.setSilence).toHaveBeenCalledWith(silence);
      expectSuccessNotification('Recreated', silence.matchers);
    });

    it('should edit a silence', () => {
      component.edit = true;
      component.id = 'editId';
      silence.id = component.id;
      fillAndSubmit();
      expect(prometheusService.setSilence).toHaveBeenCalledWith(silence);
      expectSuccessNotification('Edited', silence.matchers);
    });
  });
});
