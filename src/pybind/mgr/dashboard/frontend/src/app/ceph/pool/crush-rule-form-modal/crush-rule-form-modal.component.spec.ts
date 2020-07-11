import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  i18nProviders,
  Mocks
} from '../../../../testing/unit-test-helper';
import { CrushRuleService } from '../../../shared/api/crush-rule.service';
import { CrushNode } from '../../../shared/models/crush-node';
import { CrushRuleConfig } from '../../../shared/models/crush-rule';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { PoolModule } from '../pool.module';
import { CrushRuleFormModalComponent } from './crush-rule-form-modal.component';

describe('CrushRuleFormComponent', () => {
  let component: CrushRuleFormModalComponent;
  let crushRuleService: CrushRuleService;
  let fixture: ComponentFixture<CrushRuleFormModalComponent>;
  let formHelper: FormHelper;
  let fixtureHelper: FixtureHelper;
  let data: { names: string[]; nodes: CrushNode[] };

  // Object contains functions to get something
  const get = {
    nodeByName: (name: string): CrushNode => data.nodes.find((node) => node.name === name),
    nodesByNames: (names: string[]): CrushNode[] => names.map(get.nodeByName)
  };

  // Expects that are used frequently
  const assert = {
    failureDomains: (nodes: CrushNode[], types: string[]) => {
      const expectation = {};
      types.forEach((type) => (expectation[type] = nodes.filter((node) => node.type === type)));
      const keys = component.failureDomainKeys;
      expect(keys).toEqual(types);
      keys.forEach((key) => {
        expect(component.failureDomains[key].length).toBe(expectation[key].length);
      });
    },
    formFieldValues: (root: CrushNode, failureDomain: string, device: string) => {
      expect(component.form.value).toEqual({
        name: '',
        root,
        failure_domain: failureDomain,
        device_class: device
      });
    },
    valuesOnRootChange: (
      rootName: string,
      expectedFailureDomain: string,
      expectedDevice: string
    ) => {
      const node = get.nodeByName(rootName);
      formHelper.setValue('root', node);
      assert.formFieldValues(node, expectedFailureDomain, expectedDevice);
    },
    creation: (rule: CrushRuleConfig) => {
      formHelper.setValue('name', rule.name);
      fixture.detectChanges();
      component.onSubmit();
      expect(crushRuleService.create).toHaveBeenCalledWith(rule);
    }
  };

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      PoolModule,
      NgBootstrapFormValidationModule.forRoot()
    ],
    providers: [CrushRuleService, NgbActiveModal, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CrushRuleFormModalComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    formHelper = new FormHelper(component.form);
    crushRuleService = TestBed.inject(CrushRuleService);
    data = {
      names: ['rule1', 'rule2'],
      /**
       * Create the following test crush map:
       * > default
       * --> ssd-host
       * ----> 3x osd with ssd
       * --> mix-host
       * ----> hdd-rack
       * ------> 2x osd-rack with hdd
       * ----> ssd-rack
       * ------> 2x osd-rack with ssd
       */
      nodes: Mocks.getCrushMap()
    };
    spyOn(crushRuleService, 'getInfo').and.callFake(() => of(data));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('calls listing to get rules on ngInit', () => {
    expect(crushRuleService.getInfo).toHaveBeenCalled();
    expect(component.names.length).toBe(2);
    expect(component.buckets.length).toBe(5);
  });

  describe('lists', () => {
    afterEach(() => {
      // The available buckets should not change
      expect(component.buckets).toEqual(
        get.nodesByNames(['default', 'hdd-rack', 'mix-host', 'ssd-host', 'ssd-rack'])
      );
    });

    it('has the following lists after init', () => {
      assert.failureDomains(data.nodes, ['host', 'osd', 'osd-rack', 'rack']); // Not root as root only exist once
      expect(component.devices).toEqual(['hdd', 'ssd']);
    });

    it('has the following lists after selection of ssd-host', () => {
      formHelper.setValue('root', get.nodeByName('ssd-host'));
      assert.failureDomains(get.nodesByNames(['osd.0', 'osd.1', 'osd.2']), ['osd']); // Not host as it only exist once
      expect(component.devices).toEqual(['ssd']);
    });

    it('has the following lists after selection of mix-host', () => {
      formHelper.setValue('root', get.nodeByName('mix-host'));
      expect(component.devices).toEqual(['hdd', 'ssd']);
      assert.failureDomains(
        get.nodesByNames(['hdd-rack', 'ssd-rack', 'osd2.0', 'osd2.1', 'osd2.0', 'osd2.1']),
        ['osd-rack', 'rack']
      );
    });
  });

  describe('selection', () => {
    it('selects the first root after init automatically', () => {
      assert.formFieldValues(get.nodeByName('default'), 'osd-rack', '');
    });

    it('should select all values automatically by selecting "ssd-host" as root', () => {
      assert.valuesOnRootChange('ssd-host', 'osd', 'ssd');
    });

    it('selects automatically the most common failure domain', () => {
      // Select mix-host as mix-host has multiple failure domains (osd-rack and rack)
      assert.valuesOnRootChange('mix-host', 'osd-rack', '');
    });

    it('should override automatic selections', () => {
      assert.formFieldValues(get.nodeByName('default'), 'osd-rack', '');
      assert.valuesOnRootChange('ssd-host', 'osd', 'ssd');
      assert.valuesOnRootChange('mix-host', 'osd-rack', '');
    });

    it('should not override manual selections if possible', () => {
      formHelper.setValue('failure_domain', 'rack', true);
      formHelper.setValue('device_class', 'ssd', true);
      assert.valuesOnRootChange('mix-host', 'rack', 'ssd');
    });

    it('should preselect device by domain selection', () => {
      formHelper.setValue('failure_domain', 'osd', true);
      assert.formFieldValues(get.nodeByName('default'), 'osd', 'ssd');
    });
  });

  describe('form validation', () => {
    it(`isn't valid if name is not set`, () => {
      expect(component.form.invalid).toBeTruthy();
      formHelper.setValue('name', 'someProfileName');
      expect(component.form.valid).toBeTruthy();
    });

    it('sets name invalid', () => {
      component.names = ['awesomeProfileName'];
      formHelper.expectErrorChange('name', 'awesomeProfileName', 'uniqueName');
      formHelper.expectErrorChange('name', 'some invalid text', 'pattern');
      formHelper.expectErrorChange('name', null, 'required');
    });

    it(`should show all default form controls`, () => {
      // name
      // root (preselected(first root))
      // failure_domain (preselected=type that is most common)
      // device_class (preselected=any if multiple or some type if only one device type)
      fixtureHelper.expectIdElementsVisible(
        ['name', 'root', 'failure_domain', 'device_class'],
        true
      );
    });
  });

  describe('submission', () => {
    beforeEach(() => {
      const taskWrapper = TestBed.inject(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
      spyOn(crushRuleService, 'create').and.stub();
    });

    it('creates a rule with only required fields', () => {
      assert.creation(Mocks.getCrushRuleConfig('default-rule', 'default', 'osd-rack'));
    });

    it('creates a rule with all fields', () => {
      assert.valuesOnRootChange('ssd-host', 'osd', 'ssd');
      assert.creation(Mocks.getCrushRuleConfig('ssd-host-rule', 'ssd-host', 'osd', 'ssd'));
    });
  });
});
