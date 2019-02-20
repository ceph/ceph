import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDatepickerDirective, BsDatepickerModule } from 'ngx-bootstrap/datepicker';

import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  i18nProviders
} from '../../../../../testing/unit-test-helper';
import { NotFoundComponent } from '../../../../core/not-found/not-found.component';
import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { Permission } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { SharedModule } from '../../../../shared/shared.module';
import { ClusterModule } from '../../cluster.module';
import { PrometheusFormComponent } from './prometheus-form.component';

describe('PrometheusFormComponent', () => {
  let component: PrometheusFormComponent;
  let fixture: ComponentFixture<PrometheusFormComponent>;
  let router: Router;
  let formH: FormHelper;
  let fixtureH: FixtureHelper;
  let form: CdFormGroup;
  let originalDate;
  const baseTime = new Date('2022-02-22T00:00:00');
  const beginningDate = new Date('2022-02-22T00:00:12.35');

  const routes: Routes = [{ path: '404', component: NotFoundComponent }];
  configureTestBed({
    declarations: [NotFoundComponent],
    imports: [
      HttpClientTestingModule,
      RouterTestingModule.withRoutes(routes),
      BsDatepickerModule.forRoot(),
      SharedModule,
      ClusterModule
    ],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    originalDate = Date;
    spyOn(global, 'Date').and.callFake((sth) => (sth ? new originalDate(sth) : beginningDate));
    router = TestBed.get(Router);
    fixture = TestBed.createComponent(PrometheusFormComponent);
    fixtureH = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    form = component.form;
    formH = new FormHelper(form);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('redirect not allowed users', () => {
    let prometheusPermissions: Permission;
    let authStorageService: AuthStorageService;
    let navigateSpy;

    const testForRedirect = (redirected: boolean) => {
      component.authenticate();
      expect(router.navigate).toHaveBeenCalledTimes(redirected ? 1 : 0);
      navigateSpy.calls.reset();
    };

    beforeEach(() => {
      navigateSpy = spyOn(router, 'navigate').and.stub();
      authStorageService = TestBed.get(AuthStorageService);
      spyOn(authStorageService, 'getPermissions').and.callFake(() => ({
        prometheus: prometheusPermissions
      }));
    });

    it('navigates to 404 if not allowed', () => {
      prometheusPermissions = new Permission(['delete', 'read']);
      component.authenticate();
      expect(router.navigate).toHaveBeenCalledWith(['/404']);
    });

    it('navigates if user does not have minimum permissions to create silences', () => {
      prometheusPermissions = new Permission(['update', 'delete', 'read']);
      testForRedirect(true);
      prometheusPermissions = new Permission(['update', 'delete', 'create']);
      component.recreate = true;
      testForRedirect(true);
    });

    it('navigates if user does not have minimum permissions to update silences', () => {
      prometheusPermissions = new Permission(['create', 'delete', 'read']);
      component.edit = true;
      testForRedirect(true);
      prometheusPermissions = new Permission(['create', 'delete', 'update']);
      testForRedirect(true);
    });

    it('does not navigate if user has minimum permissions to create silences', () => {
      prometheusPermissions = new Permission(['create', 'read']);
      testForRedirect(false);
      component.recreate = true;
      testForRedirect(false);
    });

    it('does not navigate if user has minimum permissions to update silences', () => {
      prometheusPermissions = new Permission(['update', 'read']);
      component.edit = true;
      testForRedirect(false);
    });
  });

  describe('choose the right mode', () => {
    const testChooseMode = (url: string, edit: boolean, recreate: boolean) => {
      Object.defineProperty(router, 'url', { value: url });
      component.chooseMode();
      expect(component.recreate).toBe(recreate);
      expect(component.edit).toBe(edit);
    };

    it('should have no special mode activate by default', () => {
      testChooseMode('/silence/add', false, false);
    });

    it('should be in recreation mode if route includes recreate', () => {
      testChooseMode('/silence/edit/someNotExpiredId', true, false);
    });

    it('should be in recreation mode if route includes recreate', () => {
      testChooseMode('/silence/recreate/someExpiredId', false, true);
    });
  });

  describe('time', () => {
    // Can't be used to set accurate UTC dates as it uses timezones,
    // this means the UTC time changes depending on the timezone you are in
    const changeDatePicker = (el, text) => {
      el.triggerEventHandler('change', { target: { value: text } });
    };
    const getDatePicker = (i) =>
      fixture.debugElement.queryAll(By.directive(BsDatepickerDirective))[i];
    const changeEndDate = (text) => changeDatePicker(getDatePicker(1), text);
    const changeStartDate = (text) => changeDatePicker(getDatePicker(0), text);

    it('have all dates set at beginning', () => {
      expect(form.getValue('startsAt')).toEqual(baseTime);
      expect(form.getValue('duration')).toBe('2h');
      expect(form.getValue('endsAt')).toEqual(new Date('2022-02-22T02:00:00'));
    });

    describe('on start date change', () => {
      it('changes end date on start date change if it exceeds it', fakeAsync(() => {
        changeStartDate('2022-02-28T 04:05');
        expect(form.getValue('duration')).toEqual('2h');
        expect(form.getValue('endsAt')).toEqual(new Date('2022-02-28T06:05:00'));
      }));

      it('changes duration if start date does not exceed end date ', fakeAsync(() => {
        changeStartDate('2022-02-22T 00:45');
        expect(form.getValue('duration')).toEqual('1h 15m');
        expect(form.getValue('endsAt')).toEqual(new Date('2022-02-22T02:00:00'));
      }));

      it('changes end date if start date exceeds it by duration', fakeAsync(() => {
        changeStartDate('2022-12-31T 22:00');
        expect(form.getValue('duration')).toEqual('2h');
        expect(form.getValue('endsAt')).toEqual(new Date('2023-01-01T00:00:00'));
      }));

      it('should raise invalid start date error', fakeAsync(() => {
        changeStartDate('No valid date');
        formH.expectError('startsAt', 'bsDate');
        expect(form.getValue('startsAt').toString()).toBe('Invalid Date');
        expect(form.getValue('endsAt')).toEqual(new Date('2022-02-22T02:00:00'));
      }));
    });

    describe('on duration change', () => {
      it('changes end date if duration is changed', () => {
        formH.setValue('duration', '15m');
        expect(form.getValue('endsAt')).toEqual(new Date('2022-02-22T00:15'));
        formH.setValue('duration', '5d 23h');
        expect(form.getValue('endsAt')).toEqual(new Date('2022-02-27T23:00'));
      });

      describe('duration methods', () => {
        const minutes = 60 * 1000;
        const hours = 60 * minutes;
        const days = 24 * hours;

        it('should allow different writings', () => {
          expect(component.getDurationMs('2h')).toBe(2 * hours);
          expect(component.getDurationMs('4 Days')).toBe(4 * days);
          expect(component.getDurationMs('3 minutes')).toBe(3 * minutes);
          expect(component.getDurationMs('4 Days 2h 3 minutes')).toBe(
            4 * days + 2 * hours + 3 * minutes
          );
          expect(component.getDurationMs('5d3h120m')).toBe(5 * days + 5 * hours);
        });

        it('should create duration string from ms', () => {
          expect(component.makeDuration(2 * hours)).toBe('2h');
          expect(component.makeDuration(4 * days)).toBe('4d');
          expect(component.makeDuration(3 * minutes)).toBe('3m');
          expect(component.makeDuration(4 * days + 2 * hours + 3 * minutes)).toBe('4d 2h 3m');
          expect(component.makeDuration(component.getDurationMs('5d3h120m'))).toBe('5d 5h');
        });
      });
    });

    describe('on end date change', () => {
      it('changes duration on end date change if it exceeds start date', fakeAsync(() => {
        changeEndDate('2022-02-28T 04:05');
        expect(form.getValue('duration')).toEqual('6d 4h 5m');
        expect(form.getValue('startsAt')).toEqual(baseTime);
      }));

      it('changes start date if end date happens before it', fakeAsync(() => {
        changeEndDate('2022-02-21T 02:00');
        expect(form.getValue('duration')).toEqual('2h');
        expect(form.getValue('startsAt')).toEqual(new Date('2022-02-21T00:00:00'));
      }));

      it('should raise invalid end date error', fakeAsync(() => {
        changeEndDate('No valid date');
        formH.expectError('endsAt', 'bsDate');
        expect(form.getValue('endsAt').toString()).toBe('Invalid Date');
        expect(form.getValue('startsAt')).toEqual(baseTime);
      }));
    });
  });

  it('should have a creator field', () => {
    formH.expectError('createdBy', 'required');
    formH.expectValidChange('createdBy', 'Mighty FSM');
  });

  it('should have a comment field', () => {
    formH.expectError('comment', 'required');
    formH.expectValidChange('comment', 'A pretty long comment');
  });

  it('should require a minimum -> start/end date + at least 1 matcher + creator + comment', () => {
    expect(form.valid).toBeFalsy();
    formH.expectValidChange('createdBy', 'Mighty FSM');
    formH.expectValidChange('comment', 'A pretty long comment');
    // Use a matcher here
    // expect(form.valid).toBeTruthy();
  });

  describe('matchers', () => {
    const addMatcher = (name, value, isRegex) => component.setMatcher({ name, value, isRegex });

    it('should show add a matcher button', () => {
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
    });

    it('should show added matcher', () => {
      addMatcher('some name', 'some value', true);
      fixtureH.expectIdElementsVisible(
        [
          'matcher-name-0',
          'matcher-value-0',
          'matcher-isRegex-0',
          'matcher-edit-0',
          'matcher-delete-0'
        ],
        true
      );
    });

    it('should show multiple matchers', () => {
      addMatcher('some name', 'some value', true);
      addMatcher('some other name', 'some other value', false);
      fixtureH.expectIdElementsVisible(
        [
          'matcher-name-0',
          'matcher-value-0',
          'matcher-isRegex-0',
          'matcher-edit-0',
          'matcher-delete-0',
          'matcher-name-1',
          'matcher-value-1',
          'matcher-isRegex-1',
          'matcher-edit-1',
          'matcher-delete-1'
        ],
        true
      );
    });

    it('should show the right matcher values', () => {
      addMatcher('some name', 'some value', true);
      addMatcher('some other name', 'some other value', false);
      fixture.detectChanges();
      fixtureH.expectFormFieldToBe('#matcher-name-0', 'some name');
      fixtureH.expectFormFieldToBe('#matcher-value-0', 'some value');
      fixtureH.expectFormFieldToBe('#matcher-isRegex-0', 'true');
      fixtureH.expectFormFieldToBe('#matcher-isRegex-1', 'false');
    });

    it('should be able to edit a matcher', () => {
      addMatcher('some name', 'some value', true);
    });

    it('should be able to remove a matcher', () => {
      addMatcher('some name', 'some value', true);
      fixtureH.clickElement('#matcher-delete-0');
      expect(component.matchers).toEqual([]);
      fixtureH.expectIdElementsVisible(
        ['matcher-name-0', 'matcher-value-0', 'matcher-isRegex-0'],
        false
      );
    });

    it('should automatically add matchers based on selected alert', () => {});

    it('should show form as invalid if not matcher is set', () => {
      expect(form.errors).toEqual({ matcherRequired: true });
    });

    it('should show form as valid if matcher was added', () => {
      addMatcher('some name', 'some value', true);
      expect(form.errors).toEqual(null);
    });
  });

  describe('submit tests', () => {
    let prometheusService: PrometheusService;
    beforeEach(() => {
      prometheusService = TestBed.get(PrometheusService);
      spyOn(prometheusService, 'setSilence');
    });

    it('should create a json on submit', () => {
      const endsAt = new Date('2022-02-22T02:00:00');
      const silence = {
        createdBy: 'some creator',
        comment: 'some comment',
        startsAt: beginningDate.toISOString(),
        endsAt: endsAt.toISOString(),
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
      ['createdBy', 'comment'].forEach((attr) => {
        formH.setValue(attr, silence[attr]);
      });
      silence.matchers.forEach((matcher) => component.setMatcher(matcher));
      fixtureH.clickElement('#submit');
      expect(form.valid).toBeTruthy();
      expect(prometheusService.setSilence).toHaveBeenCalledWith(silence);
    });
  });
});
