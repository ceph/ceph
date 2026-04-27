import { EventEmitter } from '@angular/core';
import { TimePickerComponent } from './time-picker.component';

describe('TimePickerComponent', () => {
  it('should mount', () => {
    cy.mount(TimePickerComponent);
  });

  it('should emit default time range on init (Last 6 hours)', () => {
    const selectedTimeEmitter = new EventEmitter<{ start: number; end: number; step: number }>();
    cy.spy(selectedTimeEmitter, 'emit').as('emitSpy');

    cy.mount(TimePickerComponent, {
      componentProperties: {
        selectedTime: selectedTimeEmitter
      }
    });

    cy.get('@emitSpy').should('have.been.calledOnce');
    cy.get('@emitSpy')
      .invoke('getCall', 0)
      .its('args.0')
      .should((emitted) => {
        expect(emitted.step).to.eq(14);
        const duration = emitted.end - emitted.start;
        expect(duration).to.eq(60 * 60);
      });
  });
});
