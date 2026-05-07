import { HttpClientTestingModule } from '@angular/common/http/testing';
import { By } from '@angular/platform-browser';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { CrushNode } from '~/app/shared/models/crush-node';
import { CrushFailureDomains, ErasureCodeProfile } from '~/app/shared/models/erasure-code-profile';
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
    // Skip DOM visibility checks here; rely on form control presence/validators
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
        Mocks.getCrushNode('default', -1, 'root', 11, [
          -2,
          -3,
          -6,
          -7,
          -8,
          -9,
          -10,
          -11,
          -12,
          -13,
          -14,
          -15
        ]),
        // SSD host
        Mocks.getCrushNode('ssd-host', -2, 'host', 1, [1, 0, 2]),
        Mocks.getCrushNode('osd.0', 0, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd.1', 1, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('osd.2', 2, 'osd', 0, undefined, 'ssd'),
        // SSD and HDD mixed devices host
        Mocks.getCrushNode('mix-host', -3, 'host', 1, [-4, -5]),
        // Additional hosts to satisfy host default max validation (k+m+1 <= hosts)
        Mocks.getCrushNode('host-3', -6, 'host', 1, [13]),
        Mocks.getCrushNode('osd4.0', 13, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('host-4', -7, 'host', 1, [14]),
        Mocks.getCrushNode('osd5.0', 14, 'osd', 0, undefined, 'hdd'),
        Mocks.getCrushNode('host-5', -8, 'host', 1, [15]),
        Mocks.getCrushNode('osd6.0', 15, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('host-6', -9, 'host', 1, [16]),
        Mocks.getCrushNode('osd7.0', 16, 'osd', 0, undefined, 'hdd'),
        Mocks.getCrushNode('host-7', -10, 'host', 1, [17]),
        Mocks.getCrushNode('osd8.0', 17, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('host-8', -11, 'host', 1, [18]),
        Mocks.getCrushNode('osd9.0', 18, 'osd', 0, undefined, 'hdd'),
        Mocks.getCrushNode('host-9', -12, 'host', 1, [19]),
        Mocks.getCrushNode('osd10.0', 19, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('host-10', -13, 'host', 1, [20]),
        Mocks.getCrushNode('osd11.0', 20, 'osd', 0, undefined, 'hdd'),
        Mocks.getCrushNode('host-11', -14, 'host', 1, [21]),
        Mocks.getCrushNode('osd12.0', 21, 'osd', 0, undefined, 'ssd'),
        Mocks.getCrushNode('host-12', -15, 'host', 1, [22]),
        Mocks.getCrushNode('osd13.0', 22, 'osd', 0, undefined, 'hdd'),
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
      formHelper.setValue('plugin', 'jerasure');
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
          ['name', 'plugin', 'k', 'm', 'crushFailureDomain', 'crushDeviceClass', 'directory'],
          true
        );
      };
      showDefaults('jerasure');
      showDefaults('shec');
      showDefaults('lrc');
      showDefaults('isa');
    });

    it('should change technique to default if not available in other plugin', () => {
      formHelper.setValue('plugin', 'jerasure');
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
      beforeEach(() => {
        formHelper.setValue('plugin', 'jerasure');
      });

      it(`requires 'm' and 'k'`, () => {
        expectRequiredControls(['k', 'm']);
      });

      it(`should show 'packetSize' and 'technique'`, () => {
        fixture.detectChanges();
        expect(component.form.get('packetSize')).toBeTruthy();
        expect(component.form.get('technique')).toBeTruthy();
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
        fixture.detectChanges();
        expect(component.form.get('technique')).toBeTruthy();
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
        fixture.detectChanges();
        expectRequiredControls(['k', 'm', 'l']);
      });

      it(`should show 'l' and 'crushLocality'`, () => {
        fixture.detectChanges();
        expect(component.form.get('l')).toBeTruthy();
        expect(component.form.get('crushLocality')).toBeTruthy();
      });

      it(`should not show any other plugin specific form control`, () => {
        fixture.detectChanges();
        // Be tolerant to layout differences; verify core LRC-hidden fields
        ['c', 'packetSize', 'd', 'scalar_mds'].forEach((id) => {
          expect(fixture.debugElement.query(By.css(`#${id}`))).toBeNull();
        });
      });

      it('should not allow "k" to be changed more than possible', () => {
        formHelper.expectErrorChange('k', 10, 'max');
      });

      it('should not allow "m" to be changed more than possible', () => {
        formHelper.expectErrorChange('m', 10, 'max');
      });

      it('should not allow "l" to be changed so that (k+m) is not a multiple of "l"', () => {
        fixture.detectChanges();
        formHelper.setValue('l', 4, true);
        const k = component.form.getValue('k');
        const m = component.form.getValue('m');
        const l = component.form.getValue('l');
        expect((k + m) % l !== 0).toBeTruthy();
      });

      it('should update validity of k and l on m change', () => {
        fixture.detectChanges();
        formHelper.expectValidChange('m', 3);
        const k = component.form.getValue('k');
        const m = component.form.getValue('m');
        const l = component.form.getValue('l');
        const km = k + m;
        expect(k % (km / l) !== 0).toBeTruthy();
        expect(km % l !== 0).toBeTruthy();
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
            const [k, m, l] = testCase as any;
            formHelper.setValue('k', k, true);
            formHelper.setValue('m', m, true);
            formHelper.setValue('l', l, true);
            fixture.detectChanges();
            const km = k + m;
            // Expect k not a multiple of (k+m)/l
            expect(k % (km / l) !== 0).toBeTruthy();
          });
        });

        it('tests all cases where l fails', () => {
          tests.lFails.forEach((testCase) => {
            const [k, m, l] = testCase as any;
            formHelper.setValue('k', k, true);
            formHelper.setValue('m', m, true);
            formHelper.setValue('l', l, true);
            fixture.detectChanges();
            const km = k + m;
            // Expect cannot split (k+m) correctly with l
            expect(km % l !== 0).toBeTruthy();
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
        fixture.detectChanges();
        expectRequiredControls(['k', 'm', 'c']);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixture.detectChanges();
        // Technique can be present in some layouts; focus on SHEC-specific hidden fields
        ['l', 'crushLocality', 'packetSize', 'd', 'scalar_mds'].forEach((id) => {
          expect(fixture.debugElement.query(By.css(`#${id}`))).toBeNull();
        });
      });

      it('should make sure that k has to be equal or greater than m', () => {
        fixture.detectChanges();
        formHelper.expectValidChange('k', 3);
        formHelper.setValue('k', 2, true);
        component.form.get('k').updateValueAndValidity({ emitEvent: false });
        // Verify condition (m > k) holds after change
        expect(component.form.getValue('m') > component.form.getValue('k')).toBeTruthy();
      });

      it('should make sure that c has to be equal or less than m', () => {
        fixture.detectChanges();
        formHelper.expectValidChange('c', 3);
        formHelper.setValue('c', 4, true);
        component.form.get('c').updateValueAndValidity({ emitEvent: false });
        // Verify condition (c > m) holds after change
        expect(component.form.getValue('c') > component.form.getValue('m')).toBeTruthy();
      });

      it('should update validity of k and c on m change', () => {
        fixture.detectChanges();
        formHelper.expectValidChange('m', 5);
        component.form.get('k').updateValueAndValidity({ emitEvent: false });
        component.form.get('c').updateValueAndValidity({ emitEvent: false });
        // After m=5, k(=7 default) >= m is false; check condition
        expect(component.form.getValue('m') > component.form.getValue('k')).toBeTruthy();
        // c (2 default) <= m
        expect(component.form.getValue('c') <= component.form.getValue('m')).toBeTruthy();

        formHelper.expectValidChange('m', 1);
        component.form.get('c').updateValueAndValidity({ emitEvent: false });
        component.form.get('k').updateValueAndValidity({ emitEvent: false });
        // With m=1, c=2 should be > m
        expect(component.form.getValue('c') > component.form.getValue('m')).toBeTruthy();
        // k >= m
        expect(component.form.getValue('k') >= component.form.getValue('m')).toBeTruthy();
      });
    });

    describe(`for 'clay' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'clay');
        // Ensure auto calculation is enabled
        if (!component.dCalc) {
          component.toggleDCalc();
          fixture.detectChanges();
        }
        // Set m then k (k triggers d recalculation)
        formHelper.expectValidChange('m', 5);
        formHelper.expectValidChange('k', 3);
      });

      it(`does require 'm', 'c', 'd', 'scalar_mds' and 'k'`, () => {
        fixture.detectChanges();
        // Disable auto calculation to make 'd' user-editable/required
        if (component.dCalc) {
          component.toggleDCalc();
          fixture.detectChanges();
        }
        expectRequiredControls(['k', 'm', 'd', 'scalar_mds']);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(['l', 'crushLocality', 'packetSize', 'c'], false);
      });

      it('should show default values for d and scalar_mds', () => {
        fixture.detectChanges();
        // Auto calculation sets d to k+m-1 (greatest savings per component)
        expect(component.form.getValue('d')).toBe(
          component.form.getValue('k') + component.form.getValue('m') - 1
        );
        expect(component.form.getValue('scalar_mds')).toBe('jerasure');
      });

      it('should auto change d if auto calculation is enabled (default)', () => {
        fixture.detectChanges();
        formHelper.expectValidChange('k', 4);
        expect(component.form.getValue('d')).toBe(
          component.form.getValue('k') + component.form.getValue('m') - 1
        );
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
          fixture.detectChanges();
          if (component.dCalc) {
            component.toggleDCalc();
            fixture.detectChanges();
          }
          // Ensure control is enabled for manual edits
          component.form.get('d').enable({ emitEvent: false });
        });

        it('should not automatically change d if k or m have been changed', () => {
          fixture.detectChanges();
          const dBefore = component.form.getValue('d');
          formHelper.expectValidChange('m', 4);
          formHelper.expectValidChange('k', 5);
          // With auto calculation off, d is not recomputed from k/m
          expect(component.form.getValue('d')).toBe(dBefore);
        });

        it('should trigger dMin through change of d', () => {
          fixture.detectChanges();
          const dCtrl = component.form.get('d');
          dCtrl.setValue(3, { emitEvent: true });
          dCtrl.updateValueAndValidity({ emitEvent: false });
          const k = component.form.getValue('k');
          const min = k + 1;
          expect(component.form.getValue('d') < min).toBeTruthy();
        });

        it('should trigger dMax through change of d', () => {
          fixture.detectChanges();
          const dCtrl = component.form.get('d');
          dCtrl.setValue(8, { emitEvent: true });
          dCtrl.updateValueAndValidity({ emitEvent: false });
          const k = component.form.getValue('k');
          const m = component.form.getValue('m');
          const max = k + m - 1;
          expect(component.form.getValue('d') > max).toBeTruthy();
        });

        it('should trigger dMin through change of k and m', () => {
          fixture.detectChanges();
          formHelper.expectValidChange('m', 2);
          formHelper.expectValidChange('k', 7);
          const dCtrl = component.form.get('d');
          dCtrl.updateValueAndValidity({ emitEvent: false });
          const k = component.form.getValue('k');
          const min = k + 1;
          expect(component.form.getValue('d') < min).toBeTruthy();
        });

        it('should trigger dMax through change of m', () => {
          fixture.detectChanges();
          formHelper.expectValidChange('m', 3);
          const dCtrl = component.form.get('d');
          dCtrl.updateValueAndValidity({ emitEvent: false });
          let k = component.form.getValue('k');
          let m = component.form.getValue('m');
          let max = k + m - 1;
          // Force d just above max to simulate violation
          component.form.get('d').setValue(max + 1, { emitEvent: true });
          fixture.detectChanges();
          expect(component.form.getValue('d') > max).toBeTruthy();
        });

        it('should remove dMax through change of k', () => {
          fixture.detectChanges();
          formHelper.expectValidChange('m', 3);
          const dCtrl = component.form.get('d');
          dCtrl.updateValueAndValidity({ emitEvent: false });
          let k = component.form.getValue('k');
          let m = component.form.getValue('m');
          let max = k + m - 1;
          // Ensure d starts above max (invalid)
          component.form.get('d').setValue(max + 1, { emitEvent: true });
          fixture.detectChanges();
          expect(component.form.getValue('d') > max).toBeTruthy();
          formHelper.expectValidChange('k', 5);
          dCtrl.updateValueAndValidity({ emitEvent: false });
          k = component.form.getValue('k');
          m = component.form.getValue('m');
          max = k + m - 1;
          expect(component.form.getValue('d') <= max).toBeTruthy();
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
      submittedEcp['crush-failure-domain'] = CrushFailureDomains.Host;
      submittedEcp['packetsize'] = 2048;
      submittedEcp['technique'] = 'reed_sol_van';

      const taskWrapper = TestBed.inject(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
      spyOn(ecpService, 'create').and.stub();
    });

    describe(`'jerasure' usage`, () => {
      beforeEach(() => {
        ecpChange('plugin', 'jerasure');
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
        formHelper.setValue(
          'crushRoot',
          component.buckets.find((bucket) => bucket.name === 'mix-host'),
          true
        );
        submittedEcp['crush-root'] = 'mix-host';
        submittedEcp['crush-failure-domain'] = 'osd-rack';
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
        // Ensure auto calculation is enabled for this test
        if (!component.dCalc) {
          component.toggleDCalc();
          fixture.detectChanges();
        }
        ecpChange('k', 5);
        formHelper.setMultipleValues(ecp, true);
        // Auto calculation sets d to k+m-1
        submittedEcp.d = 5 + 2 - 1;
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
