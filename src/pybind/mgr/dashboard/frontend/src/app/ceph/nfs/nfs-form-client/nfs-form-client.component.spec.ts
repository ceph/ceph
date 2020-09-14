import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { SharedModule } from '../../../shared/shared.module';
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
});
