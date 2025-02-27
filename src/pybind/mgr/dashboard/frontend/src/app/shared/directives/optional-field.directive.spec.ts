import { ElementRef } from '@angular/core';
import { OptionalFieldDirective } from './optional-field.directive';

describe('OptionalFieldDirective', () => {
  it('should create an instance', () => {
    const directive = new OptionalFieldDirective(new ElementRef(''), null);
    expect(directive).toBeTruthy();
  });
});
