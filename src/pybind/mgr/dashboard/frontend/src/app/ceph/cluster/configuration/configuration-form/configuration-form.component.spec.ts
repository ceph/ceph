import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule, UntypedFormGroup } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ConfigFormModel } from '~/app/shared/components/config-option/config-option.model';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ConfigurationFormComponent } from './configuration-form.component';

describe('ConfigurationFormComponent', () => {
  let component: ConfigurationFormComponent;
  let fixture: ComponentFixture<ConfigurationFormComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, ReactiveFormsModule, RouterTestingModule, SharedModule],
    declarations: [ConfigurationFormComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationFormComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('getValidators', () => {
    it('should return a validator for types float, addr and uuid', () => {
      const types = ['float', 'addr', 'uuid'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = component.getValidators(configOption);
        expect(ret).toBeTruthy();
        expect(ret.length).toBe(1);
      });
    });

    it('should not return a validator for types str and bool', () => {
      const types = ['str', 'bool'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = component.getValidators(configOption);
        expect(ret).toBeUndefined();
      });
    });

    it('should return a pattern and a min validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int';
      configOption.min = 2;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(2);
      expect(component.minValue).toBe(2);
      expect(component.maxValue).toBeUndefined();
    });

    it('should return a pattern and a max validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int';
      configOption.max = 5;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(2);
      expect(component.minValue).toBeUndefined();
      expect(component.maxValue).toBe(5);
    });

    it('should return multiple validators', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'float';
      configOption.max = 5.2;
      configOption.min = 1.5;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(3);
      expect(component.minValue).toBe(1.5);
      expect(component.maxValue).toBe(5.2);
    });
  });

  describe('Client Configuration', () => {
    beforeEach(() => {
      component.response = new ConfigFormModel();
      component.response.name = 'test_config';
      component.response.type = 'str';
    });

    describe('initializeClientEntries', () => {
      it('should initialize with no entries when no client values exist', () => {
        component.response.value = [];
        component.initializeClientEntries();

        expect(component.clientEntries.length).toBe(0);
      });

      it('should load existing client and client.* configurations', () => {
        component.response.type = 'bool';
        component.response.value = [
          { section: 'client', value: 'false' },
          { section: 'client.rgw.my-daemon', value: 'true' },
          { section: 'client.admin', value: '100' }
        ];
        component.initializeClientEntries();

        expect(component.clientEntries.length).toBe(3);

        const firstEntry = component.clientEntries.at(0) as UntypedFormGroup;
        expect(firstEntry.get('clientEntity')?.value).toBe('client');
        expect(firstEntry.get('value')?.value).toBe(false);

        const secondEntry = component.clientEntries.at(1) as UntypedFormGroup;
        expect(secondEntry.get('clientEntity')?.value).toBe('client.rgw.my-daemon');
        expect(secondEntry.get('value')?.value).toBe(true);

        const thirdEntry = component.clientEntries.at(2) as UntypedFormGroup;
        expect(thirdEntry.get('clientEntity')?.value).toBe('client.admin');
        expect(thirdEntry.get('value')?.value).toBe('100');
      });

      it('should filter duplicate client sections', () => {
        component.response.value = [
          { section: 'client.rgw.daemon1', value: 'true' },
          { section: 'client.rgw.daemon1', value: 'false' }
        ];
        component.initializeClientEntries();

        expect(component.clientEntries.length).toBe(1);
      });
    });

    describe('addClientEntry', () => {
      it('should add a new empty client entry', () => {
        component.response.value = [];
        component.initializeClientEntries();
        expect(component.clientEntries.length).toBe(0);
        component.addClientEntry();
        expect(component.clientEntries.length).toBe(1);
        const newEntry = component.clientEntries.at(0) as UntypedFormGroup;
        expect(newEntry.get('clientEntity')?.value).toBe('');
        expect(newEntry.get('value')?.value).toBeNull();
      });
    });

    describe('removeClientEntry', () => {
      it('should remove a client entry at specified index', () => {
        component.response.value = [
          { section: 'client.rgw.daemon1', value: 'true' },
          { section: 'client.admin', value: '100' }
        ];
        component.initializeClientEntries();
        expect(component.clientEntries.length).toBe(2);

        component.removeClientEntry(0);

        expect(component.clientEntries.length).toBe(1);
        const remainingEntry = component.clientEntries.at(0) as UntypedFormGroup;
        expect(remainingEntry.get('clientEntity')?.value).toBe('client.admin');
      });
    });

    describe('clientPrefixValidator', () => {
      beforeEach(() => {
        component.response.value = [];
        component.createForm();
        component.initializeClientEntries();
        component.addClientEntry();
      });

      it('should accept values starting with "client."', () => {
        const entry = component.clientEntries.at(0) as UntypedFormGroup;
        const control = entry.get('clientEntity');

        control?.setValue('client.rgw.my-daemon');
        expect(control?.hasError('clientPrefix')).toBe(false);

        control?.setValue('client.admin');
        expect(control?.hasError('clientPrefix')).toBe(false);
      });

      it('should reject values not starting with "client"', () => {
        const entry = component.clientEntries.at(0) as UntypedFormGroup;
        const control = entry.get('clientEntity');

        control?.setValue('rgw.my-daemon');
        expect(control?.hasError('clientPrefix')).toBe(true);

        control?.setValue('admin');
        expect(control?.hasError('clientPrefix')).toBe(true);
      });

      it('should allow empty values (handled by required validator)', () => {
        const entry = component.clientEntries.at(0) as UntypedFormGroup;
        const control = entry.get('clientEntity');

        control?.setValue('');
        expect(control?.hasError('clientPrefix')).toBe(false);
        expect(control?.hasError('required')).toBe(true);
      });
    });

    describe('clientEntityUniquenessValidator', () => {
      beforeEach(() => {
        component.response.value = [];
        component.createForm();
        component.initializeClientEntries();
      });

      it('should reject duplicate client entities', () => {
        // Add two entries with the same client entity
        component.addClientEntry();
        component.addClientEntry();

        const firstEntry = component.clientEntries.at(0) as UntypedFormGroup;
        const secondEntry = component.clientEntries.at(1) as UntypedFormGroup;

        firstEntry.get('clientEntity')?.setValue('client.rgw.daemon1');
        secondEntry.get('clientEntity')?.setValue('client.rgw.daemon1');

        // Both should have the duplicate error
        expect(firstEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(true);
        expect(secondEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(true);
      });

      it('should allow unique client entities', () => {
        component.addClientEntry();
        component.addClientEntry();

        const firstEntry = component.clientEntries.at(0) as UntypedFormGroup;
        const secondEntry = component.clientEntries.at(1) as UntypedFormGroup;

        firstEntry.get('clientEntity')?.setValue('client.rgw.daemon1');
        secondEntry.get('clientEntity')?.setValue('client.admin');

        // Neither should have the duplicate error
        expect(firstEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(false);
        expect(secondEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(false);
      });

      it('should revalidate sibling entries when an entity changes', () => {
        component.addClientEntry();
        component.addClientEntry();

        const firstEntry = component.clientEntries.at(0) as UntypedFormGroup;
        const secondEntry = component.clientEntries.at(1) as UntypedFormGroup;

        // Set both to the same value
        firstEntry.get('clientEntity')?.setValue('client.rgw.daemon1');
        secondEntry.get('clientEntity')?.setValue('client.rgw.daemon1');

        // Both should have errors
        expect(firstEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(true);
        expect(secondEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(true);

        // Change the second one to a different value
        secondEntry.get('clientEntity')?.setValue('client.admin');

        // Now neither should have errors
        expect(firstEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(false);
        expect(secondEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(false);
      });

      it('should revalidate remaining entries after removal', () => {
        component.addClientEntry();
        component.addClientEntry();
        component.addClientEntry();

        const firstEntry = component.clientEntries.at(0) as UntypedFormGroup;
        const secondEntry = component.clientEntries.at(1) as UntypedFormGroup;
        const thirdEntry = component.clientEntries.at(2) as UntypedFormGroup;

        // Set first and second to the same value
        firstEntry.get('clientEntity')?.setValue('client.rgw.daemon1');
        secondEntry.get('clientEntity')?.setValue('client.rgw.daemon1');
        thirdEntry.get('clientEntity')?.setValue('client.admin');

        // First and second should have errors
        expect(firstEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(true);
        expect(secondEntry.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(true);

        // Remove the second entry
        component.removeClientEntry(1);

        // Now the first entry should not have an error (no more duplicates)
        const remainingFirst = component.clientEntries.at(0) as UntypedFormGroup;
        expect(remainingFirst.get('clientEntity')?.hasError('clientEntityDuplicate')).toBe(false);
      });
    });

    describe('prepareClientEntityOptions', () => {
      it('should filter and map client entities starting with "client."', () => {
        component.clientEntities = [
          { entity: 'client.rgw.daemon1', caps: {}, key: 'key1' },
          { entity: 'client.admin', caps: {}, key: 'key2' },
          { entity: 'osd.0', caps: {}, key: 'key3' },
          { entity: 'mgr.x', caps: {}, key: 'key4' }
        ];

        component.prepareClientEntityOptions();

        expect(component.clientEntityOptions.length).toBe(2);
        expect(component.clientEntityOptions[0].content).toBe('client.rgw.daemon1');
        expect(component.clientEntityOptions[1].content).toBe('client.admin');
      });

      it('should handle empty client entities list', () => {
        component.clientEntities = [];

        component.prepareClientEntityOptions();

        expect(component.clientEntityOptions.length).toBe(0);
      });
    });

    describe('parseConfigValue', () => {
      it('should parse boolean strings only for bool type', () => {
        component.response.type = 'bool';
        expect(component['parseConfigValue']('true')).toBe(true);
        expect(component['parseConfigValue']('false')).toBe(false);
      });

      it('should not parse boolean strings for non-bool types', () => {
        component.response.type = 'str';
        expect(component['parseConfigValue']('true')).toBe('true');
        expect(component['parseConfigValue']('false')).toBe('false');
      });

      it('should parse numeric strings only for numeric types', () => {
        component.response.type = 'int';
        expect(component['parseConfigValue']('123')).toBe(123);
        expect(component['parseConfigValue']('  89  ')).toBe(89);

        component.response.type = 'float';
        expect(component['parseConfigValue']('45.67')).toBe(45.67);

        component.response.type = 'uint';
        expect(component['parseConfigValue']('100')).toBe(100);
      });

      it('should preserve string values with leading zeros for str type', () => {
        component.response.type = 'str';
        expect(component['parseConfigValue']('00123')).toBe('00123');
        expect(component['parseConfigValue']('007')).toBe('007');
      });

      it('should return string for non-numeric types', () => {
        component.response.type = 'str';
        expect(component['parseConfigValue']('some_string')).toBe('some_string');
        expect(component['parseConfigValue']('192.168.1.1')).toBe('192.168.1.1');
        expect(component['parseConfigValue']('123')).toBe('123');
      });

      it('should handle non-string inputs', () => {
        component.response.type = 'str';
        expect(component['parseConfigValue'](123 as any)).toBe(123);
        expect(component['parseConfigValue'](true as any)).toBe(true);
      });
    });

    describe('setResponse', () => {
      beforeEach(() => {
        component.createForm();
      });

      it('should add daemon-specific sections dynamically', () => {
        const response = new ConfigFormModel();
        response.name = 'test_config';
        response.type = 'str';
        response.value = [
          { section: 'mgr.ceph-node-00.vfbbqn', value: 'mgr_value' },
          { section: 'osd.0', value: 'osd_value' }
        ];

        component.setResponse(response);

        expect(component.availSections).toContain('mgr.ceph-node-00.vfbbqn');
        expect(component.availSections).toContain('osd.0');
      });
    });

    describe('createRequest', () => {
      beforeEach(() => {
        component.response = new ConfigFormModel();
        component.response.name = 'test_config';
        component.response.type = 'str';
        component.response.value = [];
        component.createForm();
      });

      it('should include client entity configurations in request', () => {
        component.response.value = [];
        component.initializeClientEntries();
        component.addClientEntry();

        const entry = component.clientEntries.at(0) as UntypedFormGroup;
        entry.get('clientEntity')?.setValue('client.rgw.my-daemon');
        entry.get('value')?.setValue('true');

        const request = component.createRequest();

        expect(request).toBeTruthy();
        expect(request?.value).toContainEqual({ section: 'client.rgw.my-daemon', value: 'true' });
      });

      it('should remove deleted client configurations', () => {
        component.response.type = 'int';
        component.response.value = [
          { section: 'client.rgw.daemon1', value: 'true' },
          { section: 'client.admin', value: '100' }
        ];
        component.initializeClientEntries();

        component.removeClientEntry(0);

        const request = component.createRequest();

        expect(request).toBeTruthy();
        expect(request?.value).toContainEqual({ section: 'client.rgw.daemon1', value: '' });
        expect(request?.value).toContainEqual({ section: 'client.admin', value: 100 });
      });

      it('should not include empty client entries', () => {
        component.response.value = [];
        component.initializeClientEntries();
        component.addClientEntry();

        const entry = component.clientEntries.at(0) as UntypedFormGroup;
        entry.get('clientEntity')?.setValue('');
        entry.get('value')?.setValue('');

        const request = component.createRequest();

        expect(request).toBeNull();
      });
    });
  });
});
