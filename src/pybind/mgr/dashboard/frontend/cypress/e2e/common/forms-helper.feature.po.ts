import { And, Then } from 'cypress-cucumber-preprocessor/steps';

/**
 * Fills in the given field using the value provided
 * @param field ID of the field that needs to be filled out.
 * @param value Value that should be filled in the field.
 */
And('enter {string} {string}', (field: string, value: string) => {
  cy.get('.cd-col-form').within(() => {
    cy.get(`input[id=${field}]`).clear().type(value);
  });
});

/**
 * Fills in the given field using the value provided
 * @param field ID of the field that needs to be filled out.
 * @param value Value that should be filled in the field.
 */
And('enter {string} {string} in the modal', (field: string, value: string) => {
  cy.get('cd-modal').within(() => {
    cy.get(`input[id=${field}]`).clear().type(value);
  });
});

And('select options {string}', (labels: string) => {
  if (labels) {
    cy.get('a[data-testid=select-menu-edit]').click();
    for (const label of labels.split(', ')) {
      cy.get('.popover-body div.select-menu-item-content').contains(label).click();
    }
  }
});

And('{string} option {string}', (action: string, labels: string) => {
  if (labels) {
    if (action === 'add') {
      cy.get('cd-modal').find('.select-menu-edit').click();
      for (const label of labels.split(', ')) {
        cy.get('.popover-body input').type(`${label}{enter}`);
      }
    } else {
      for (const label of labels.split(', ')) {
        cy.contains('cd-modal .badge', new RegExp(`^${label}$`))
          .find('.badge-remove')
          .click();
      }
    }
  }
});

And('I click on submit button', () => {
  cy.get('[data-cy=submitBtn]').click();
});

/**
 * Some modals have an additional confirmation to be provided
 * by ticking the 'Are you sure?' box.
 */
Then('I check the tick box in modal', () => {
  cy.get('cd-modal input#confirmation').click();
});

And('I confirm to {string}', (action: string) => {
  cy.contains('cd-modal button', action).click();
  cy.get('cd-modal').should('not.exist');
});

Then('I should see an error in {string} field', (field: string) => {
  cy.get('cd-modal').within(() => {
    cy.get(`input[id=${field}]`).should('have.class', 'ng-invalid');
  });
});

And('select {string} {string}', (selectionName: string, option: string) => {
  cy.get(`select[name=${selectionName}]`).select(option);
  cy.get(`select[name=${selectionName}] option:checked`).contains(option);
});
