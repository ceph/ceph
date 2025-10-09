import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';

import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NfsFormClientComponent } from './nfs-form-client.component';

describe('NfsFormClientComponent', () => {
  let component: NfsFormClientComponent;
  let fixture: ComponentFixture<NfsFormClientComponent>;

  configureTestBed({
    declarations: [NfsFormClientComponent],
    imports: [ReactiveFormsModule, SharedModule, HttpClientTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsFormClientComponent);
    const formBuilder = TestBed.inject(CdFormBuilder);
    component = fixture.componentInstance;

    component.form = new CdFormGroup({
      access_type: new FormControl(''),
      clients: formBuilder.array([]),
      squash: new FormControl('')
    });

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should add a client', () => {
    expect(component.form.getValue('clients')).toEqual([]);
    component.addClient();
    expect(component.form.getValue('clients')).toEqual([
      { access_type: '', addresses: '', squash: '' }
    ]);
  });

  it('should return form access_type', () => {
    expect(component.getNoAccessTypeDescr()).toBe('-- Select the access type --');

    component.form.patchValue({ access_type: 'RW' });
    expect(component.getNoAccessTypeDescr()).toBe('RW (inherited from global config)');
  });

  it('should return form squash', () => {
    expect(component.getNoSquashDescr()).toBe(
      '-- Select what kind of user id squashing is performed --'
    );

    component.form.patchValue({ squash: 'root_id_squash' });
    expect(component.getNoSquashDescr()).toBe('root_id_squash (inherited from global config)');
  });

  it('should remove client', () => {
    component.addClient();
    expect(component.form.getValue('clients')).toEqual([
      { access_type: '', addresses: '', squash: '' }
    ]);

    component.removeClient(0);
    expect(component.form.getValue('clients')).toEqual([]);
  });

  describe(`test 'isValidClientAddress'`, () => {
    it('should return false for empty value', () => {
      expect(component.isValidClientAddress('')).toBeFalsy();
      expect(component.isValidClientAddress(null)).toBeFalsy();
    });

    it('should return false for valid single IPv4 address', () => {
      expect(component.isValidClientAddress('192.168.1.1')).toBeFalsy();
    });

    it('should return false for valid IPv6 address', () => {
      expect(component.isValidClientAddress('2001:db8::1')).toBeFalsy();
    });

    it('should return false for valid FQDN', () => {
      expect(component.isValidClientAddress('nfs.example.com')).toBeFalsy();
    });

    it('should return false for valid IP CIDR range', () => {
      expect(component.isValidClientAddress('192.168.0.0/24')).toBeFalsy();
      expect(component.isValidClientAddress('2001:db8::/64')).toBeFalsy();
    });

    it('should return false for multiple valid addresses separated by comma', () => {
      const input = '192.168.1.1, 2001:db8::1, nfs.example.com, 10.0.0.0/8';
      expect(component.isValidClientAddress(input)).toBeFalsy();
    });

    it('should return true for invalid single address', () => {
      expect(component.isValidClientAddress('invalid-address')).toBeTruthy();
    });

    it('should return true for mixed valid and invalid addresses', () => {
      const input = '192.168.1.1, invalid-address';
      expect(component.isValidClientAddress(input)).toBeTruthy();
    });

    it('should return true for URLs with protocols', () => {
      expect(component.isValidClientAddress('http://nfs.example.com')).toBeTruthy();
    });
  });
});
