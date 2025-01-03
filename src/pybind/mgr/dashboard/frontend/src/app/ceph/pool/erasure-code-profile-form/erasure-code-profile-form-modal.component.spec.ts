import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { CrushNode } from '~/app/shared/models/crush-node';
import { ErasureCodeProfile } from '~/app/shared/models/erasure-code-profile';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { configureTestBed, FixtureHelper, FormHelper, Mocks } from '~/testing/unit-test-helper';
import { PoolModule } from '../pool.module';
import { ErasureCodeProfileFormModalComponent } from './erasure-code-profile-form-modal.component';

describe('ErasureCodeProfileFormModalComponent', () => {
  let component: ErasureCodeProfileFormModalComponent;
  let ecpService: ErasureCodeProfileService;
  let fixture: ComponentFixture<ErasureCodeProfileFormModalComponent>;
  let formHelper: FormHelper;
  let fixtureHelper: FixtureHelper;
  let data: { plugins: string[]; names: string[]; nodes: CrushNode[] };

  const expectTechnique = (current: string) =>
    expect(component.form.getValue('technique')).toBe(current);

  const expectTechniques = (techniques: string[], current: string) => {
    expect(component.techniques).toEqual(techniques);
    expectTechnique(current);
  };

  const expectRequiredControls = (controlNames: string[]) => {
    controlNames.forEach((name) => {
      const value = component.form.getValue(name);
      formHelper.expectValid(name);
      formHelper.expectErrorChange(name, null, 'required');
      // This way other fields won't fail through getting invalid.
      formHelper.expectValidChange(name, value);
    });
    fixtureHelper.expectIdElementsVisible(controlNames, true);
  };

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, ToastrModule.forRoot(), PoolModule],
    providers: [ErasureCodeProfileService, NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ErasureCodeProfileFormModalComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    formHelper = new FormHelper(component.form);
    ecpService = TestBed.inject(ErasureCodeProfileService);
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
       * ------> 5x osd-rack with hdd
       * ----> ssd-rack
       * ------> 5x osd-rack with ssd
       */
      nodes: [
        // Root node
        Mocks.getCrushNode('default', -1, 'root', 11, [-2, -3]),
        // SSD host
        Mocks.getCrushNode('ssd-host', -2, 'host', 1, [1, 0, 2]),
        Mocks.getCrushNode('osd.0', 0, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd.1', 1, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd.2', 2, 'osd', 0, undefined, 'ssd'),
        // SSD and HDD mixed devices host
        Mocks.getCrushNode('mix-host', -3, 'host', 1, [-4, -5]),
        // HDD rack
        Mocks.getCrushNode('hdd-rack', -4, 'rack', 3, [3, 4, 5, 6, 7]),
        Mocks.getCrushNode('osd2.0', 3, 'osd-rack', 0, undefined, 'hdd'),
        Mocks.getCrushNode('osd2.1', 4, 'osd-rack', 0, undefined, 'hdd'),
        Mocks.getCrushNode('osd2.2', 5, 'osd-rack', 0, undefined, 'hdd'),
        Mocks.getCrushNode('osd2.3', 6, 'osd-rack', 0, undefined, 'hdd'),
        Mocks.getCrushNode('osd2.4', 7, 'osd-rack', 0, undefined, 'hdd'),
        // SSD rack
        Mocks.getCrushNode('ssd-rack', -5, 'rack', 3, [8, 9, 10, 11, 12]),
        Mocks.getCrushNode('osd3.0', 8, 'osd-rack', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd3.1', 9, 'osd-rack', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd3.2', 10, 'osd-rack', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd3.3', 11, 'osd-rack', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd3.4', 12, 'osd-rack', 0, undefined, 'ssd')
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

    it('should change technique to default if not available in other plugin', () => {
      expectTechnique('reed_sol_van');
      formHelper.setValue('technique', 'blaum_roth');
      expectTechnique('blaum_roth');
      formHelper.setValue('plugin', 'isa');
      expectTechnique('reed_sol_van');
      formHelper.setValue('plugin', 'clay');
      formHelper.expectValidChange('scalar_mds', 'shec');
      expectTechnique('single');
    });

    describe(`for 'jerasure' plugin (default)`, () => {
      it(`requires 'm' and 'k'`, () => {
        expectRequiredControls(['k', 'm']);
      });

      it(`should show 'packetSize' and 'technique'`, () => {
        fixtureHelper.expectIdElementsVisible(['packetSize', 'technique'], true);
      });

      it('should show available techniques', () => {
        expectTechniques(
          [
            'reed_sol_van',
            'reed_sol_r6_op',
            'cauchy_orig',
            'cauchy_good',
            'liberation',
            'blaum_roth',
            'liber8tion'
          ],
          'reed_sol_van'
        );
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(
          ['c', 'l', 'crushLocality', 'd', 'scalar_mds'],
          false
        );
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
        expectRequiredControls(['k', 'm']);
      });

      it(`should show 'technique'`, () => {
        fixtureHelper.expectIdElementsVisible(['technique'], true);
      });

      it('should show available techniques', () => {
        expectTechniques(['reed_sol_van', 'cauchy'], 'reed_sol_van');
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(
          ['c', 'l', 'crushLocality', 'packetSize', 'd', 'scalar_mds'],
          false
        );
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
        expectRequiredControls(['k', 'm', 'l']);
      });

      it(`should show 'l' and 'crushLocality'`, () => {
        fixtureHelper.expectIdElementsVisible(['l', 'crushLocality'], true);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(
          ['c', 'packetSize', 'technique', 'd', 'scalar_mds'],
          false
        );
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
        expectRequiredControls(['k', 'm', 'c']);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(
          ['l', 'crushLocality', 'packetSize', 'technique', 'd', 'scalar_mds'],
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

    describe(`for 'clay' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'clay');
        // Through this change d has a valid range from 4 to 7
        formHelper.expectValidChange('k', 3);
        formHelper.expectValidChange('m', 5);
      });

      it(`does require 'm', 'c', 'd', 'scalar_mds' and 'k'`, () => {
        fixtureHelper.clickElement('#d-calc-btn');
        expectRequiredControls(['k', 'm', 'd', 'scalar_mds']);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(['l', 'crushLocality', 'packetSize', 'c'], false);
      });

      it('should show default values for d and scalar_mds', () => {
        expect(component.form.getValue('d')).toBe(7); // (k+m-1)
        expect(component.form.getValue('scalar_mds')).toBe('jerasure');
      });

      it('should auto change d if auto calculation is enabled (default)', () => {
        formHelper.expectValidChange('k', 4);
        expect(component.form.getValue('d')).toBe(8);
      });

      it('should have specific techniques for scalar_mds jerasure', () => {
        expectTechniques(
          ['reed_sol_van', 'reed_sol_r6_op', 'cauchy_orig', 'cauchy_good', 'liber8tion'],
          'reed_sol_van'
        );
      });

      it('should have specific techniques for scalar_mds isa', () => {
        formHelper.expectValidChange('scalar_mds', 'isa');
        expectTechniques(['reed_sol_van', 'cauchy'], 'reed_sol_van');
      });

      it('should have specific techniques for scalar_mds shec', () => {
        formHelper.expectValidChange('scalar_mds', 'shec');
        expectTechniques(['single', 'multiple'], 'single');
      });

      describe('Validity of d', () => {
        beforeEach(() => {
          // Don't automatically change d - the only way to get d invalid
          fixtureHelper.clickElement('#d-calc-btn');
        });

        it('should not automatically change d if k or m have been changed', () => {
          formHelper.expectValidChange('m', 4);
          formHelper.expectValidChange('k', 5);
          expect(component.form.getValue('d')).toBe(7);
        });

        it('should trigger dMin through change of d', () => {
          formHelper.expectErrorChange('d', 3, 'dMin');
        });

        it('should trigger dMax through change of d', () => {
          formHelper.expectErrorChange('d', 8, 'dMax');
        });

        it('should trigger dMin through change of k and m', () => {
          formHelper.expectValidChange('m', 2);
          formHelper.expectValidChange('k', 7);
          formHelper.expectError('d', 'dMin');
        });

        it('should trigger dMax through change of m', () => {
          formHelper.expectValidChange('m', 3);
          formHelper.expectError('d', 'dMax');
        });

        it('should remove dMax through change of k', () => {
          formHelper.expectValidChange('m', 3);
          formHelper.expectError('d', 'dMax');
          formHelper.expectValidChange('k', 5);
          formHelper.expectValid('d');
        });
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

      const taskWrapper = TestBed.inject(TaskWrapperService);
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
        formHelper.setValue('crushDeviceClass', 'ssd', true);
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

    describe(`'clay' usage`, () => {
      beforeEach(() => {
        ecpChange('name', 'clayProfile');
        ecpChange('plugin', 'clay');
        // Setting expectations
        submittedEcp.k = 4;
        submittedEcp.m = 2;
        submittedEcp.d = 5;
        submittedEcp.scalar_mds = 'jerasure';
        delete submittedEcp.packetsize;
      });

      it('should be able to create a profile with only plugin and name', () => {
        formHelper.setMultipleValues(ecp, true);
        testCreation();
      });

      it('should send profile with a changed d', () => {
        formHelper.setMultipleValues(ecp, true);
        ecpChange('d', '5');
        submittedEcp.d = 5;
        testCreation();
      });

      it('should send profile with a changed k which automatically changes d', () => {
        ecpChange('k', 5);
        formHelper.setMultipleValues(ecp, true);
        submittedEcp.d = 6;
        testCreation();
      });

      it('should send profile with a changed sclara_mds', () => {
        ecpChange('scalar_mds', 'shec');
        formHelper.setMultipleValues(ecp, true);
        submittedEcp.scalar_mds = 'shec';
        submittedEcp.technique = 'single';
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
