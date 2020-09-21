import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { CdFormGroup } from '../forms/cd-form-group';
import { TrimDirective } from './trim.directive';

@Component({
  template: `
    <form [formGroup]="trimForm">
      <input type="text" formControlName="trimInput" cdTrim />
    </form>
  `
})
export class TrimComponent {
  trimForm: CdFormGroup;
  constructor() {
    this.trimForm = new CdFormGroup({
      trimInput: new FormControl()
    });
  }
}

describe('TrimDirective', () => {
  configureTestBed({
    imports: [FormsModule, ReactiveFormsModule],
    declarations: [TrimDirective, TrimComponent]
  });

  it('should create an instance', () => {
    const directive = new TrimDirective(null);
    expect(directive).toBeTruthy();
  });

  it('should trim', () => {
    const fixture: ComponentFixture<TrimComponent> = TestBed.createComponent(TrimComponent);
    const component: TrimComponent = fixture.componentInstance;
    const inputElement: HTMLInputElement = fixture.debugElement.query(By.css('input'))
      .nativeElement;
    fixture.detectChanges();

    inputElement.value = ' a b ';
    inputElement.dispatchEvent(new Event('input'));
    const expectedValue = 'a b';
    expect(inputElement.value).toBe(expectedValue);
    expect(component.trimForm.getValue('trimInput')).toBe(expectedValue);
  });
});
