import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { AutofocusDirective } from './autofocus.directive';

@Component({
  template: `
    <form>
      <input id="x" type="text" />
      <input id="y" type="password" autofocus />
    </form>
  `
})
export class PasswordFormComponent {}

@Component({
  template: `
    <form>
      <input id="x" type="checkbox" [autofocus]="edit" />
      <input id="y" type="text" />
    </form>
  `
})
export class CheckboxFormComponent {
  public edit = true;
}

@Component({
  template: `
    <form>
      <input id="x" type="text" [autofocus]="foo" />
    </form>
  `
})
export class TextFormComponent {
  foo() {
    return false;
  }
}

describe('AutofocusDirective', () => {
  configureTestBed({
    declarations: [
      AutofocusDirective,
      CheckboxFormComponent,
      PasswordFormComponent,
      TextFormComponent
    ]
  });

  it('should create an instance', () => {
    const directive = new AutofocusDirective(null);
    expect(directive).toBeTruthy();
  });

  it('should focus the password form field', () => {
    const fixture: ComponentFixture<PasswordFormComponent> = TestBed.createComponent(
      PasswordFormComponent
    );
    fixture.detectChanges();
    const focused = fixture.debugElement.query(By.css(':focus'));
    expect(focused.attributes.id).toBe('y');
    expect(focused.attributes.type).toBe('password');
    const element = document.getElementById('y');
    expect(element === document.activeElement).toBeTruthy();
  });

  it('should focus the checkbox form field', () => {
    const fixture: ComponentFixture<CheckboxFormComponent> = TestBed.createComponent(
      CheckboxFormComponent
    );
    fixture.detectChanges();
    const focused = fixture.debugElement.query(By.css(':focus'));
    expect(focused.attributes.id).toBe('x');
    expect(focused.attributes.type).toBe('checkbox');
    const element = document.getElementById('x');
    expect(element === document.activeElement).toBeTruthy();
  });

  it('should not focus the text form field', () => {
    const fixture: ComponentFixture<TextFormComponent> = TestBed.createComponent(TextFormComponent);
    fixture.detectChanges();
    const focused = fixture.debugElement.query(By.css(':focus'));
    expect(focused).toBeNull();
    const element = document.getElementById('x');
    expect(element !== document.activeElement).toBeTruthy();
  });
});
