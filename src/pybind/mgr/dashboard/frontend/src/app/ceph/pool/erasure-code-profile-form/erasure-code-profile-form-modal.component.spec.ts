import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  i18nProviders
} from '../../../../testing/unit-test-helper';
import { ErasureCodeProfileService } from '../../../shared/api/erasure-code-profile.service';
import { CrushNode } from '../../../shared/models/crush-node';
import { ErasureCodeProfile } from '../../../shared/models/erasure-code-profile';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { PoolModule } from '../pool.module';
import { ErasureCodeProfileFormModalComponent } from './erasure-code-profile-form-modal.component';

describe('ErasureCodeProfileFormModalComponent', () => {
  let component: ErasureCodeProfileFormModalComponent;
  let ecpService: ErasureCodeProfileService;
  let fixture: ComponentFixture<ErasureCodeProfileFormModalComponent>;
  let formHelper: FormHelper;
  let fixtureHelper: FixtureHelper;
  let data: {};

  // Object contains mock functions
  const mock = {
    node: (
      name: string,
      id: number,
      type: string,
      type_id: number,
      children?: number[],
      device_class?: string
    ): CrushNode => {
      return { name, type, type_id, id, children, device_class };
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
    providers: [ErasureCodeProfileService, BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ErasureCodeProfileFormModalComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    formHelper = new FormHelper(component.form);
    ecpService = TestBed.get(ErasureCodeProfileService);
    data = {
      plugins: ['isa', 'jerasure', 'shec', 'lrc'],
      names: ['ecp1', 'ecp2'],
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
      nodes: [
        // Root node
        mock.node('default', -1, 'root', 11, [-2, -3]),
        // SSD host
        mock.node('ssd-host', -2, 'host', 1, [1, 0, 2]),
        mock.node('osd.0', 0, 'osd', 0, undefined, 'ssd'),
        mock.node('osd.1', 1, 'osd', 0, undefined, 'ssd'),
        mock.node('osd.2', 2, 'osd', 0, undefined, 'ssd'),
        // SSD and HDD mixed devices host
        mock.node('mix-host', -3, 'host', 1, [-4, -5]),
        // HDD rack
        mock.node('hdd-rack', -4, 'rack', 3, [3, 4, 5, 6, 7]),
        mock.node('osd2.0', 3, 'osd-rack', 0, undefined, 'hdd'),
        mock.node('osd2.1', 4, 'osd-rack', 0, undefined, 'hdd'),
        mock.node('osd2.2', 5, 'osd-rack', 0, undefined, 'hdd'),
        mock.node('osd2.3', 6, 'osd-rack', 0, undefined, 'hdd'),
        mock.node('osd2.4', 7, 'osd-rack', 0, undefined, 'hdd'),
        // SSD rack
        mock.node('ssd-rack', -5, 'rack', 3, [8, 9, 10, 11, 12]),
        mock.node('osd3.0', 8, 'osd-rack', 0, undefined, 'ssd'),
        mock.node('osd3.1', 9, 'osd-rack', 0, undefined, 'ssd'),
        mock.node('osd3.2', 10, 'osd-rack', 0, undefined, 'ssd'),
        mock.node('osd3.3', 11, 'osd-rack', 0, undefined, 'ssd'),
        mock.node('osd3.4', 12, 'osd-rack', 0, undefined, 'ssd')
      ]
    };
    spyOn(ecpService, 'getInfo').and.callFake(() => of(data));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('calls listing to get ecps on ngInit', () => {
    expect(ecpService.getInfo).toHaveBeenCalled();
    expect(component.names.length).toBe(2);
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

    it('sets k to min error', () => {
      formHelper.expectErrorChange('k', 1, 'min');
    });

    it('sets m to min error', () => {
      formHelper.expectErrorChange('m', 0, 'min');
    });

    it(`should show all default form controls`, () => {
      const showDefaults = (plugin: string) => {
        formHelper.setValue('plugin', plugin);
        fixtureHelper.expectIdElementsVisible(
          [
            'name',
            'plugin',
            'k',
            'm',
            'crushFailureDomain',
            'crushRoot',
            'crushDeviceClass',
            'directory'
          ],
          true
        );
      };
      showDefaults('jerasure');
      showDefaults('shec');
      showDefaults('lrc');
      showDefaults('isa');
    });

    describe(`for 'jerasure' plugin (default)`, () => {
      it(`requires 'm' and 'k'`, () => {
        formHelper.expectErrorChange('k', null, 'required');
        formHelper.expectErrorChange('m', null, 'required');
      });

      it(`should show 'packetSize' and 'technique'`, () => {
        fixtureHelper.expectIdElementsVisible(['packetSize', 'technique'], true);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(['c', 'l', 'crushLocality'], false);
      });

      it('should not allow "k" to be changed more than possible', () => {
        formHelper.expectErrorChange('k', 10, 'max');
      });

      it('should not allow "m" to be changed more than possible', () => {
        formHelper.expectErrorChange('m', 10, 'max');
      });
    });

    describe(`for 'isa' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'isa');
      });

      it(`does require 'm' and 'k'`, () => {
        formHelper.expectErrorChange('k', null, 'required');
        formHelper.expectErrorChange('m', null, 'required');
      });

      it(`should show 'technique'`, () => {
        fixtureHelper.expectIdElementsVisible(['technique'], true);
        expect(fixture.debugElement.query(By.css('#technique'))).toBeTruthy();
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(['c', 'l', 'crushLocality', 'packetSize'], false);
      });

      it('should not allow "k" to be changed more than possible', () => {
        formHelper.expectErrorChange('k', 10, 'max');
      });

      it('should not allow "m" to be changed more than possible', () => {
        formHelper.expectErrorChange('m', 10, 'max');
      });
    });

    describe(`for 'lrc' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'lrc');
        formHelper.expectValid('k');
        formHelper.expectValid('l');
        formHelper.expectValid('m');
      });

      it(`requires 'm', 'l' and 'k'`, () => {
        formHelper.expectErrorChange('k', null, 'required');
        formHelper.expectErrorChange('m', null, 'required');
        formHelper.expectErrorChange('l', null, 'required');
      });

      it(`should show 'l' and 'crushLocality'`, () => {
        fixtureHelper.expectIdElementsVisible(['l', 'crushLocality'], true);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(['c', 'packetSize', 'technique'], false);
      });

      it('should not allow "k" to be changed more than possible', () => {
        formHelper.expectErrorChange('k', 10, 'max');
      });

      it('should not allow "m" to be changed more than possible', () => {
        formHelper.expectErrorChange('m', 10, 'max');
      });

      it('should not allow "l" to be changed so that (k+m) is not a multiple of "l"', () => {
        formHelper.expectErrorChange('l', 4, 'unequal');
      });

      it('should update validity of k and l on m change', () => {
        formHelper.expectValidChange('m', 3);
        formHelper.expectError('k', 'unequal');
        formHelper.expectError('l', 'unequal');
      });

      describe('lrc calculation', () => {
        const expectCorrectCalculation = (
          k: number,
          m: number,
          l: number,
          failedControl: string[] = []
        ) => {
          formHelper.setValue('k', k);
          formHelper.setValue('m', m);
          formHelper.setValue('l', l);
          ['k', 'l'].forEach((name) => {
            if (failedControl.includes(name)) {
              formHelper.expectError(name, 'unequal');
            } else {
              formHelper.expectValid(name);
            }
          });
        };

        const tests = {
          kFails: [
            [2, 1, 1],
            [2, 2, 1],
            [3, 1, 1],
            [3, 2, 1],
            [3, 1, 2],
            [3, 3, 1],
            [3, 3, 3],
            [4, 1, 1],
            [4, 2, 1],
            [4, 2, 2],
            [4, 3, 1],
            [4, 4, 1]
          ],
          lFails: [
            [2, 1, 2],
            [3, 2, 2],
            [3, 1, 3],
            [3, 2, 3],
            [4, 1, 2],
            [4, 3, 2],
            [4, 3, 3],
            [4, 1, 3],
            [4, 4, 3],
            [4, 1, 4],
            [4, 2, 4],
            [4, 3, 4]
          ],
          success: [
            [2, 2, 2],
            [2, 2, 4],
            [3, 3, 2],
            [3, 3, 6],
            [4, 2, 3],
            [4, 2, 6],
            [4, 4, 2],
            [4, 4, 8],
            [4, 4, 4]
          ]
        };

        it('tests all cases where k fails', () => {
          tests.kFails.forEach((testCase) => {
            expectCorrectCalculation(testCase[0], testCase[1], testCase[2], ['k']);
          });
        });

        it('tests all cases where l fails', () => {
          tests.lFails.forEach((testCase) => {
            expectCorrectCalculation(testCase[0], testCase[1], testCase[2], ['k', 'l']);
          });
        });

        it('tests all cases where everything is valid', () => {
          tests.success.forEach((testCase) => {
            expectCorrectCalculation(testCase[0], testCase[1], testCase[2]);
          });
        });
      });
    });

    describe(`for 'shec' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'shec');
        formHelper.expectValid('c');
        formHelper.expectValid('m');
        formHelper.expectValid('k');
      });

      it(`does require 'm', 'c' and 'k'`, () => {
        formHelper.expectErrorChange('k', null, 'required');
        formHelper.expectErrorChange('m', null, 'required');
        formHelper.expectErrorChange('c', null, 'required');
      });

      it(`should show 'c'`, () => {
        fixtureHelper.expectIdElementsVisible(['c'], true);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(
          ['l', 'crushLocality', 'packetSize', 'technique'],
          false
        );
      });

      it('should make sure that k has to be equal or greater than m', () => {
        formHelper.expectValidChange('k', 3);
        formHelper.expectErrorChange('k', 2, 'kLowerM');
      });

      it('should make sure that c has to be equal or less than m', () => {
        formHelper.expectValidChange('c', 3);
        formHelper.expectErrorChange('c', 4, 'cGreaterM');
      });

      it('should update validity of k and c on m change', () => {
        formHelper.expectValidChange('m', 5);
        formHelper.expectError('k', 'kLowerM');
        formHelper.expectValid('c');

        formHelper.expectValidChange('m', 1);
        formHelper.expectError('c', 'cGreaterM');
        formHelper.expectValid('k');
      });
    });
  });

  describe('submission', () => {
    let ecp: ErasureCodeProfile;
    let submittedEcp: ErasureCodeProfile;

    const testCreation = () => {
      fixture.detectChanges();
      component.onSubmit();
      expect(ecpService.create).toHaveBeenCalledWith(submittedEcp);
    };

    const ecpChange = (attribute: string, value: string | number) => {
      ecp[attribute] = value;
      submittedEcp[attribute] = value;
    };

    beforeEach(() => {
      ecp = new ErasureCodeProfile();
      submittedEcp = new ErasureCodeProfile();
      submittedEcp['crush-root'] = 'default';
      submittedEcp['crush-failure-domain'] = 'osd-rack';
      submittedEcp['packetsize'] = 2048;
      submittedEcp['technique'] = 'reed_sol_van';

      const taskWrapper = TestBed.get(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
      spyOn(ecpService, 'create').and.stub();
    });

    describe(`'jerasure' usage`, () => {
      beforeEach(() => {
        submittedEcp['plugin'] = 'jerasure';
        ecpChange('name', 'jerasureProfile');
        submittedEcp.k = 4;
        submittedEcp.m = 2;
      });

      it('should be able to create a profile with only required fields', () => {
        formHelper.setMultipleValues(ecp, true);
        testCreation();
      });

      it(`does not create with missing 'k' or invalid form`, () => {
        ecpChange('k', 0);
        formHelper.setMultipleValues(ecp, true);
        component.onSubmit();
        expect(ecpService.create).not.toHaveBeenCalled();
      });

      it('should be able to create a profile with m, k, name, directory and packetSize', () => {
        ecpChange('m', 3);
        ecpChange('directory', '/different/ecp/path');
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('packetSize', 8192, true);
        ecpChange('packetsize', 8192);
        testCreation();
      });

      it('should not send the profile with unsupported fields', () => {
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('crushLocality', 'osd', true);
        testCreation();
      });
    });

    describe(`'isa' usage`, () => {
      beforeEach(() => {
        ecpChange('name', 'isaProfile');
        ecpChange('plugin', 'isa');
        submittedEcp.k = 7;
        submittedEcp.m = 3;
        delete submittedEcp.packetsize;
      });

      it('should be able to create a profile with only plugin and name', () => {
        formHelper.setMultipleValues(ecp, true);
        testCreation();
      });

      it('should send profile with plugin, name, failure domain and technique only', () => {
        ecpChange('technique', 'cauchy');
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('crushFailureDomain', 'osd', true);
        submittedEcp['crush-failure-domain'] = 'osd';
        submittedEcp['crush-device-class'] = 'ssd';
        testCreation();
      });

      it('should not send the profile with unsupported fields', () => {
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('packetSize', 'osd', true);
        testCreation();
      });
    });

    describe(`'lrc' usage`, () => {
      beforeEach(() => {
        ecpChange('name', 'lrcProfile');
        ecpChange('plugin', 'lrc');
        submittedEcp.k = 4;
        submittedEcp.m = 2;
        submittedEcp.l = 3;
        delete submittedEcp.packetsize;
        delete submittedEcp.technique;
      });

      it('should be able to create a profile with only required fields', () => {
        formHelper.setMultipleValues(ecp, true);
        testCreation();
      });

      it('should send profile with all required fields and crush root and locality', () => {
        ecpChange('l', '6');
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('crushRoot', component.buckets[2], true);
        submittedEcp['crush-root'] = 'mix-host';
        formHelper.setValue('crushLocality', 'osd-rack', true);
        submittedEcp['crush-locality'] = 'osd-rack';
        testCreation();
      });

      it('should not send the profile with unsupported fields', () => {
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('c', 4, true);
        testCreation();
      });
    });

    describe(`'shec' usage`, () => {
      beforeEach(() => {
        ecpChange('name', 'shecProfile');
        ecpChange('plugin', 'shec');
        submittedEcp.k = 4;
        submittedEcp.m = 3;
        submittedEcp.c = 2;
        delete submittedEcp.packetsize;
        delete submittedEcp.technique;
      });

      it('should be able to create a profile with only plugin and name', () => {
        formHelper.setMultipleValues(ecp, true);
        testCreation();
      });

      it('should send profile with plugin, name, c and crush device class only', () => {
        ecpChange('c', '3');
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('crushDeviceClass', 'ssd', true);
        submittedEcp['crush-device-class'] = 'ssd';
        testCreation();
      });

      it('should not send the profile with unsupported fields', () => {
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('l', 8, true);
        testCreation();
      });
    });
  });
});
