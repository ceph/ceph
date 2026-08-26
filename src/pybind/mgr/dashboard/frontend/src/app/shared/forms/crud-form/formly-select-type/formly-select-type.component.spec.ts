import { Component, NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormGroup, ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { FormlyFieldConfig, FormlyModule } from '@ngx-formly/core';
import { SelectModule } from 'carbon-components-angular';

import { FormlySelectTypeComponent } from './formly-select-type.component';
import { DirectivesModule } from '~/app/shared/directives/directives.module';
import { configureTestBed } from '~/testing/unit-test-helper';

@Component({
  template: ` <form [formGroup]="form">
    <formly-form
      [model]="model"
      [fields]="fields"
      [form]="form"
    ></formly-form>
  </form>`,
  standalone: false
})
class MockFormComponent {
  form = new FormGroup({});
  model: { key_type?: string } = { key_type: 'aes' };
  fields: FormlyFieldConfig[] = [
    {
      key: 'key_type',
      type: 'enum',
      props: {
        label: 'Key type',
        options: [
          { label: 'aes', value: 'aes' },
          { label: 'aes256k', value: 'aes256k' }
        ]
      }
    }
  ];
}

describe('FormlySelectTypeComponent', () => {
  let component: MockFormComponent;
  let fixture: ComponentFixture<MockFormComponent>;

  configureTestBed({
    declarations: [MockFormComponent, FormlySelectTypeComponent],
    imports: [
      ReactiveFormsModule,
      SelectModule,
      DirectivesModule,
      FormlyModule.forRoot({
        types: [{ name: 'enum', component: FormlySelectTypeComponent }]
      })
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MockFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  function getSelect(): HTMLSelectElement {
    return fixture.nativeElement.querySelector('select');
  }

  function getSelectType(): FormlySelectTypeComponent {
    return fixture.debugElement.query(By.directive(FormlySelectTypeComponent)).componentInstance;
  }

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(getSelectType()).toBeTruthy();
  });

  it('should render aes and aes256k options', () => {
    expect(getSelectType().selectOptions).toEqual([
      { label: 'aes', value: 'aes' },
      { label: 'aes256k', value: 'aes256k' }
    ]);
    const options: HTMLOptionElement[] = Array.from(getSelect().querySelectorAll('option'));
    expect(options.map((opt) => opt.value)).toEqual(['aes', 'aes256k']);
  });

  it('should default to aes', () => {
    expect(component.form.get('key_type')?.value).toBe('aes');
    expect(getSelect().value).toBe('aes');
  });

  it('should select aes', () => {
    const select = getSelect();
    select.value = 'aes';
    select.dispatchEvent(new Event('change'));
    fixture.detectChanges();
    expect(component.form.get('key_type')?.value).toBe('aes');
  });

  it('should select aes256k', () => {
    const select = getSelect();
    select.value = 'aes256k';
    select.dispatchEvent(new Event('change'));
    fixture.detectChanges();
    expect(component.form.get('key_type')?.value).toBe('aes256k');
  });

  it('should map string options to label/value pairs', () => {
    getSelectType().field = {
      ...getSelectType().field,
      props: { label: 'Key type', options: ['aes', 'aes256k'] }
    };
    expect(getSelectType().selectOptions).toEqual([
      { label: 'aes', value: 'aes' },
      { label: 'aes256k', value: 'aes256k' }
    ]);
  });
});
