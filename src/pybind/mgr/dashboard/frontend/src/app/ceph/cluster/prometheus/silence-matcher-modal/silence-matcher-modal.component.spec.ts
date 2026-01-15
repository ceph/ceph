import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  PrometheusHelper
} from '~/testing/unit-test-helper';
import {
  CheckboxModule,
  ComboBoxModule,
  InputModule,
  ModalModule,
  SelectModule
} from 'carbon-components-angular';
import { SilenceMatcherModalComponent } from './silence-matcher-modal.component';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { of } from 'rxjs';

describe('SilenceMatcherModalComponent', () => {
  let component: SilenceMatcherModalComponent;
  let fixture: ComponentFixture<SilenceMatcherModalComponent>;
  let fixtureHelper: FixtureHelper;
  let formHelper: FormHelper;
  let prometheus: PrometheusHelper;

  configureTestBed({
    imports: [
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ToastrModule.forRoot(),
      InputModule,
      SelectModule,
      ComboBoxModule,
      ModalModule,
      CheckboxModule
    ],
    declarations: [SilenceMatcherModalComponent],
    schemas: [CUSTOM_ELEMENTS_SCHEMA],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SilenceMatcherModalComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
    fixtureHelper = new FixtureHelper(fixture);
    formHelper = new FormHelper(component.form);
    prometheus = new PrometheusHelper();

    component.rules = [
      prometheus.createRule('alert0', 'someSeverity', [prometheus.createAlert('alert0')]),
      prometheus.createRule('alert1', 'someSeverity', [])
    ];
    component.possibleValues = ['alert0', 'alert1', 'someInstance', 'someJob', 'someSeverity'];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a name field', () => {
    formHelper.expectError('name', 'required');
    formHelper.expectValidChange('name', 'alertname');
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
      formHelper.setValue('name', key);
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
      fixture.detectChanges();

      const matchState = fixture.nativeElement.querySelector('#match-state');
      expect(matchState?.textContent?.trim()).toBe(helpText);
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
      fixture.detectChanges();
      fixtureHelper.expectElementVisible('#match-state', false);

      formHelper.setValue('name', 'severity');
      formHelper.setValue('value', 'someSeverity');
      fixture.detectChanges();
      fixtureHelper.expectElementVisible('#match-state', true);

      formHelper.setValue('isRegex', true);
      fixture.detectChanges();
      fixtureHelper.expectElementVisible('#match-state', false);
    });
  });

  it('should only enable value field if name was set', () => {
    const value = component.form.get('value');
    expect(value.disabled).toBeTruthy();
    formHelper.setValue('name', component.nameAttributes[0]);
    expect(value.enabled).toBeTruthy();
    formHelper.setValue('name', null);
    expect(value.disabled).toBeTruthy();
  });

  it('should have a value field', () => {
    formHelper.setValue('name', component.nameAttributes[0]);
    formHelper.expectError('value', 'required');
    formHelper.expectValidChange('value', 'alert0');
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

  it('should emit submitAction on valid form submission', fakeAsync(() => {
    jest.spyOn(component.submitAction, 'emit');
    tick();
    fixture.detectChanges();

    formHelper.setValue('name', 'test_name');
    formHelper.setValue('value', 'test_value');
    fixture.detectChanges();
    tick();

    component.onSubmit();
    expect(component.submitAction.emit).toHaveBeenCalledWith({
      name: 'test_name',
      value: 'test_value',
      isRegex: false
    });
  }));

  describe('typeahead', () => {
    const expectations = {
      name: ['alert0', 'alert1'],
      instance: ['someInstance']
    };

    it('should show all values on empty input', fakeAsync(async () => {
      for (const key of Object.keys(expectations) as Array<keyof typeof expectations>) {
        component.possibleValues = expectations[key];

        let result: string[] = [];
        component.search(of('')).subscribe((res) => (result = res));

        tick(200);
        expect(result).toEqual(expectations[key]);
      }
    }));

    it('should search for "some"', fakeAsync(async () => {
      component.possibleValues = expectations['instance'];

      let result: string[] = [];
      component.search(of('some')).subscribe((res) => (result = res));

      tick(200);
      expect(result).toEqual(['someInstance']);
    }));

    it('should search for "er"', fakeAsync(async () => {
      component.possibleValues = expectations['name'];

      let result: string[] = [];
      component.search(of('er')).subscribe((res) => (result = res));

      tick(200);
      expect(result).toEqual(expectations['name'].filter((v) => v.toLowerCase().includes('er')));
    }));
  });
});
