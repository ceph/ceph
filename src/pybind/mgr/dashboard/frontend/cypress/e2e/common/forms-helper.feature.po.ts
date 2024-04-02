import { And, Then } from 'cypress-cucumber-preprocessor/steps';

/**
 * Fills in the given field using the value provided
 * @param field ID of the field that needs to be filled out.
 * @param value Value that should be filled in the field.
 */
And('enter {string} {string}', (field: string, value: string) => {
  cy.get(`input[id=${field}]`).clear().type(value);
});

/**
 * Ticks a checkbox in the form
 * @param field name of the field that needs to be filled out.
 */
And('checks {string}', (field: string) => {
  cy.get('cds-checkbox span').contains(field).click();
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

/**
 * Fills in the given field using the value provided in carbon modal
 * @param field ID of the field that needs to be filled out.
 * @param value Value that should be filled in the field.
 */
And('enter {string} {string} in the carbon modal', (field: string, value: string) => {
  cy.get('cds-modal').within(() => {
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
    cy.get('cds-modal').find('input[id=labels]').click();
    if (action === 'add') {
      for (const label of labels.split(', ')) {
        cy.get('input[id=labels]').clear().type(`${label}`);

        cy.get('cds-dropdown-list').find('li').should('have.attr', 'title', label).click();
      }
    } else {
      for (const label of labels.split(', ')) {
        cy.get('input[id=labels]').clear().type(`${label}`);
        cy.get('cds-dropdown-list').find('li').should('have.attr', 'title', label).click();
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

Then('I check the tick box in carbon modal', () => {
  cy.get('cds-modal input#confirmation_input').click({ force: true });
});

Then('I confirm the resource {string}', (name: string) => {
  cy.get('cds-modal input#resource_name').type(name);
});

And('I confirm to {string}', (action: string) => {
  cy.contains('cd-modal button', action).click();
  cy.get('cd-modal').should('not.exist');
});

And('I confirm to {string} on carbon modal', (action: string) => {
  cy.contains('cds-modal button', action).click();
  cy.get('cds-modal').should('not.exist');
});

Then('I should see an error in {string} field', (field: string) => {
  cy.get('cds-modal').within(() => {
    cy.get(`input[id=${field}]`).should('have.class', 'ng-invalid');
  });
});

And('select {string} {string}', (selectionName: string, option: string) => {
  cy.get(`select[id=${selectionName}]`).select(option);
  cy.get(`select[id=${selectionName}] option:checked`).contains(option);
});
