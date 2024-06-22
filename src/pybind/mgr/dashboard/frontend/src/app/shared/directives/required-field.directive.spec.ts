import { ElementRef } from '@angular/core';
import { RequiredFieldDirective } from './required-field.directive';

describe('RequiredFieldDirective', () => {
  it('should create an instance', () => {
    const directive = new RequiredFieldDirective(new ElementRef(''), null);
    expect(directive).toBeTruthy();
  });
});
