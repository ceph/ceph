import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { of } from 'rxjs';

import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  i18nProviders
} from '../../../../testing/unit-test-helper';
import { ErasureCodeProfileService } from '../../../shared/api/erasure-code-profile.service';
import { ErasureCodeProfile } from '../../../shared/models/erasure-code-profile';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { PoolModule } from '../pool.module';
import { ErasureCodeProfileFormComponent } from './erasure-code-profile-form.component';

describe('ErasureCodeProfileFormComponent', () => {
  let component: ErasureCodeProfileFormComponent;
  let ecpService: ErasureCodeProfileService;
  let fixture: ComponentFixture<ErasureCodeProfileFormComponent>;
  let formHelper: FormHelper;
  let fixtureHelper: FixtureHelper;
  let data: {};

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, ToastModule.forRoot(), PoolModule],
    providers: [ErasureCodeProfileService, BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ErasureCodeProfileFormComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    formHelper = new FormHelper(component.form);
    ecpService = TestBed.get(ErasureCodeProfileService);
    data = {
      failure_domains: ['host', 'osd'],
      plugins: ['isa', 'jerasure', 'shec', 'lrc'],
      names: ['ecp1', 'ecp2'],
      devices: ['ssd', 'hdd']
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
      formHelper.expectErrorChange('k', 0, 'min');
    });

    it('sets m to min error', () => {
      formHelper.expectErrorChange('m', 0, 'min');
    });

    it(`should show all default form controls`, () => {
      const showDefaults = (plugin) => {
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
    });

    describe(`for 'isa' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'isa');
      });

      it(`does not require 'm' and 'k'`, () => {
        formHelper.setValue('k', null);
        formHelper.expectValidChange('k', null);
        formHelper.expectValidChange('m', null);
      });

      it(`should show 'technique'`, () => {
        fixtureHelper.expectIdElementsVisible(['technique'], true);
        expect(fixture.debugElement.query(By.css('#technique'))).toBeTruthy();
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(['c', 'l', 'crushLocality', 'packetSize'], false);
      });
    });

    describe(`for 'lrc' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'lrc');
      });

      it(`requires 'm', 'l' and 'k'`, () => {
        formHelper.expectErrorChange('k', null, 'required');
        formHelper.expectErrorChange('m', null, 'required');
      });

      it(`should show 'l' and 'crushLocality'`, () => {
        fixtureHelper.expectIdElementsVisible(['l', 'crushLocality'], true);
      });

      it(`should not show any other plugin specific form control`, () => {
        fixtureHelper.expectIdElementsVisible(['c', 'packetSize', 'technique'], false);
      });
    });

    describe(`for 'shec' plugin`, () => {
      beforeEach(() => {
        formHelper.setValue('plugin', 'shec');
      });

      it(`does not require 'm' and 'k'`, () => {
        formHelper.expectValidChange('k', null);
        formHelper.expectValidChange('m', null);
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
    });
  });

  describe('submission', () => {
    let ecp: ErasureCodeProfile;

    const testCreation = () => {
      fixture.detectChanges();
      component.onSubmit();
      expect(ecpService.create).toHaveBeenCalledWith(ecp);
    };

    beforeEach(() => {
      ecp = new ErasureCodeProfile();
      const taskWrapper = TestBed.get(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
      spyOn(ecpService, 'create').and.stub();
    });

    describe(`'jerasure' usage`, () => {
      beforeEach(() => {
        ecp.name = 'jerasureProfile';
      });

      it('should be able to create a profile with only required fields', () => {
        formHelper.setMultipleValues(ecp, true);
        ecp.k = 4;
        ecp.m = 2;
        testCreation();
      });

      it(`does not create with missing 'k' or invalid form`, () => {
        ecp.k = 0;
        formHelper.setMultipleValues(ecp, true);
        component.onSubmit();
        expect(ecpService.create).not.toHaveBeenCalled();
      });

      it('should be able to create a profile with m, k, name, directory and packetSize', () => {
        ecp.m = 3;
        ecp.directory = '/different/ecp/path';
        formHelper.setMultipleValues(ecp, true);
        ecp.k = 4;
        formHelper.setValue('packetSize', 8192, true);
        ecp.packetsize = 8192;
        testCreation();
      });

      it('should not send the profile with unsupported fields', () => {
        formHelper.setMultipleValues(ecp, true);
        ecp.k = 4;
        ecp.m = 2;
        formHelper.setValue('crushLocality', 'osd', true);
        testCreation();
      });
    });

    describe(`'isa' usage`, () => {
      beforeEach(() => {
        ecp.name = 'isaProfile';
        ecp.plugin = 'isa';
      });

      it('should be able to create a profile with only plugin and name', () => {
        formHelper.setMultipleValues(ecp, true);
        testCreation();
      });

      it('should send profile with plugin, name, failure domain and technique only', () => {
        ecp.technique = 'cauchy';
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('crushFailureDomain', 'osd', true);
        ecp['crush-failure-domain'] = 'osd';
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
        ecp.name = 'lreProfile';
        ecp.plugin = 'lrc';
      });

      it('should be able to create a profile with only required fields', () => {
        formHelper.setMultipleValues(ecp, true);
        ecp.k = 4;
        ecp.m = 2;
        ecp.l = 3;
        testCreation();
      });

      it('should send profile with all required fields and crush root and locality', () => {
        ecp.l = 8;
        formHelper.setMultipleValues(ecp, true);
        ecp.k = 4;
        ecp.m = 2;
        formHelper.setValue('crushLocality', 'osd', true);
        formHelper.setValue('crushRoot', 'rack', true);
        ecp['crush-locality'] = 'osd';
        ecp['crush-root'] = 'rack';
        testCreation();
      });

      it('should not send the profile with unsupported fields', () => {
        formHelper.setMultipleValues(ecp, true);
        ecp.k = 4;
        ecp.m = 2;
        ecp.l = 3;
        formHelper.setValue('c', 4, true);
        testCreation();
      });
    });

    describe(`'shec' usage`, () => {
      beforeEach(() => {
        ecp.name = 'shecProfile';
        ecp.plugin = 'shec';
      });

      it('should be able to create a profile with only plugin and name', () => {
        formHelper.setMultipleValues(ecp, true);
        testCreation();
      });

      it('should send profile with plugin, name, c and crush device class only', () => {
        ecp.c = 4;
        formHelper.setMultipleValues(ecp, true);
        formHelper.setValue('crushDeviceClass', 'ssd', true);
        ecp['crush-device-class'] = 'ssd';
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
