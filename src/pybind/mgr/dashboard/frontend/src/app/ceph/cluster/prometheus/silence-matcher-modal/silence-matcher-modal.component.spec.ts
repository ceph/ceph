import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  i18nProviders,
  PrometheusHelper
} from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { SilenceMatcherModalComponent } from './silence-matcher-modal.component';

describe('SilenceMatcherModalComponent', () => {
  let component: SilenceMatcherModalComponent;
  let fixture: ComponentFixture<SilenceMatcherModalComponent>;

  let formH: FormHelper;
  let fixtureH: FixtureHelper;
  let prometheus: PrometheusHelper;

  configureTestBed({
    declarations: [SilenceMatcherModalComponent],
    imports: [
      HttpClientTestingModule,
      SharedModule,
      NgbTypeaheadModule,
      RouterTestingModule,
      ReactiveFormsModule
    ],
    providers: [NgbActiveModal, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SilenceMatcherModalComponent);
    component = fixture.componentInstance;

    fixtureH = new FixtureHelper(fixture);
    formH = new FormHelper(component.form);
    prometheus = new PrometheusHelper();

    component.rules = [
      prometheus.createRule('alert0', 'someSeverity', [prometheus.createAlert('alert0')]),
      prometheus.createRule('alert1', 'someSeverity', [])
    ];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a name field', () => {
    formH.expectError('name', 'required');
    formH.expectValidChange('name', 'alertname');
  });

  it('should only allow a specific set of name attributes', () => {
    expect(component.nameAttributes).toEqual(['alertname', 'instance', 'job', 'severity']);
  });

  it('should autocomplete a list based on the set name', () => {
    const expectations = {
      alertname: ['alert0', 'alert1'],
      instance: ['someInstance'],
      job: ['someJob'],
      severity: ['someSeverity']
    };
    Object.keys(expectations).forEach((key) => {
      formH.setValue('name', key);
      expect(component.possibleValues).toEqual(expectations[key]);
    });
  });

  describe('test rule matching', () => {
    const expectMatch = (name: string, value: string, helpText: string) => {
      component.preFillControls({
        name: name,
        value: value,
        isRegex: false
      });
      expect(fixtureH.getText('#match-state')).toBe(helpText);
    };

    it('should match no rule and no alert', () => {
      expectMatch(
        'alertname',
        'alert',
        'Your matcher seems to match no currently defined rule or active alert.'
      );
    });

    it('should match a rule with no alert', () => {
      expectMatch('alertname', 'alert1', 'Matches 1 rule with no active alerts.');
    });

    it('should match a rule and an alert', () => {
      expectMatch('alertname', 'alert0', 'Matches 1 rule with 1 active alert.');
    });

    it('should match multiple rules and an alert', () => {
      expectMatch('severity', 'someSeverity', 'Matches 2 rules with 1 active alert.');
    });

    it('should match multiple rules and multiple alerts', () => {
      component.rules[1].alerts.push(null);
      expectMatch('severity', 'someSeverity', 'Matches 2 rules with 2 active alerts.');
    });

    it('should not show match-state if regex is checked', () => {
      fixtureH.expectElementVisible('#match-state', false);
      formH.setValue('name', 'severity');
      formH.setValue('value', 'someSeverity');
      fixtureH.expectElementVisible('#match-state', true);
      formH.setValue('isRegex', true);
      fixtureH.expectElementVisible('#match-state', false);
    });
  });

  it('should only enable value field if name was set', () => {
    const value = component.form.get('value');
    expect(value.disabled).toBeTruthy();
    formH.setValue('name', component.nameAttributes[0]);
    expect(value.enabled).toBeTruthy();
    formH.setValue('name', null);
    expect(value.disabled).toBeTruthy();
  });

  it('should have a value field', () => {
    formH.setValue('name', component.nameAttributes[0]);
    formH.expectError('value', 'required');
    formH.expectValidChange('value', 'alert0');
  });

  it('should test preFillControls', () => {
    const controlValues = {
      name: 'alertname',
      value: 'alert0',
      isRegex: false
    };
    component.preFillControls(controlValues);
    expect(component.form.value).toEqual(controlValues);
  });

  it('should test submit', (done) => {
    const controlValues = {
      name: 'alertname',
      value: 'alert0',
      isRegex: false
    };
    component.preFillControls(controlValues);
    component.submitAction.subscribe((resp: object) => {
      expect(resp).toEqual(controlValues);
      done();
    });
    component.onSubmit();
  });
});
