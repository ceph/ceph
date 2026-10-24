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
  cy.get(`cds-modal input[id=${field}]`).filter(':visible').clear().type(value);
});

And('select options {string}', (labels: string) => {
  if (labels) {
    // Open Carbon combo-box dropdown (finds first multi-select combo-box)
    cy.get('cds-combo-box[type="multi"] input.cds--text-input').first().click({ force: true });
    cy.get('.cds--list-box__menu.cds--multi-select').should('be.visible');
    for (const label of labels.split(', ')) {
      cy.get('.cds--list-box__menu.cds--multi-select .cds--checkbox-label')
        .contains('.cds--checkbox-label-text', label, { matchCase: false })
        .parent()
        .click({ force: true });
    }
    // Close the dropdown
    cy.get('body').type('{esc}');
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
  cy.get('[data-testid=submitBtn]').click();
});

/**
 * Clicks a button in the carbon modal and waits for that dialog to finish.
 * The form must be valid before click: submit silently ignores invalid forms.
 * Do not require section.cds--modal.is-visible afterward: the tearsheet overlay
 * is sometimes removed entirely, which timed out Remove host and Edit labels.
 */
And('I submit the carbon modal with {string}', (button: string) => {
  cy.get('cds-modal form').filter(':visible').should('have.class', 'ng-valid');
  cy.get(`cds-modal button[aria-label="${button}"]`)
    .filter(':visible')
    .should('be.enabled')
    .click();
  cy.get(`cds-modal button[aria-label="${button}"]`).should(($btns) => {
    expect(Cypress.$($btns).filter(':visible')).to.have.length(0);
  });
});

And('I cancel the carbon modal', () => {
  // cd-back-button uses aria-label="Back" while the visible label is Cancel.
  cy.get('cds-modal button[aria-label="Back"]').filter(':visible').click({ force: true });
  cy.get('cds-modal button[aria-label="Back"]').should(($btns) => {
    expect(Cypress.$($btns).filter(':visible')).to.have.length(0);
  });
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
  cy.get('cds-modal input#resource_name').filter(':visible').clear().type(name);
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
    // Blur so async validators (e.g. unique hostname) run.
    cy.get(`input[id=${field}]`).blur();
    // Carbon shows validation in .cds--form-requirement, not always ng-invalid on input.
    cy.get(`cds-text-label[for=${field}] .cds--form-requirement`).should('be.visible');
  });
});

And('select {string} {string}', (selectionName: string, option: string) => {
  cy.get('body').then(($body) => {
    if ($body.find(`cds-select[id=${selectionName}]`).length > 0) {
      // Carbon Design System select
      cy.get(`cds-select[id=${selectionName}] select`).select(option, { force: true });
      cy.get(`cds-select[id=${selectionName}] select option:checked`).should(($opt) => {
        expect($opt.text().trim().toLowerCase()).to.include(option.toLowerCase());
      });
    } else if ($body.find(`cds-radio-group[formControlName=${selectionName}]`).length > 0) {
      // Carbon Design System radio group
      cy.get(
        `cds-radio-group[formControlName=${selectionName}] cds-radio input[type="radio"][value="${option}"]`
      ).check({ force: true });
    } else {
      // Native select
      cy.get(`select[id=${selectionName}]`).select(option);
      cy.get(`select[id=${selectionName}] option:checked`).contains(option);
    }
  });
});
