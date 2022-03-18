import { And, Given, Then, When } from 'cypress-cucumber-preprocessor/steps';

import { UrlsCollection } from './urls.po';

const urlsCollection = new UrlsCollection();

Given('I am logged in', () => {
  cy.login();
  Cypress.Cookies.preserveOnce('token');
});

Given('I am on the {string} page', (page: string) => {
  cy.visit(urlsCollection.pages[page].url);
  cy.get(urlsCollection.pages[page].id).should('exist');
});

Then('I should be on the {string} page', (page: string) => {
  cy.get(urlsCollection.pages[page].id).should('exist');
});

And('I should see a button to {string}', (button: string) => {
  cy.get(`[aria-label="${button}"]`).should('be.visible');
});

When('I click on {string} button', (button: string) => {
  cy.get(`[aria-label="${button}"]`).first().click();
});

// When you are clicking on an action in the table actions dropdown button
When('I click on {string} button from the table actions', (button: string) => {
  cy.get('.table-actions button.dropdown-toggle').first().click();
  cy.get(`[aria-label="${button}"]`).first().click();
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

/**
 * Fills in the given field using the value provided
 * @param field ID of the field that needs to be filled out.
 * @param value Value that should be filled in the field.
 */
And('enter {string} {string}', (field: string, value: string) => {
  cy.get('cd-modal').within(() => {
    cy.get(`input[id=${field}]`).type(value);
  });
});

And('I click on submit button', () => {
  cy.get('[data-cy=submitBtn]').click();
});

/**
 * Selects any row on the datatable if it matches the given name
 */
When('I select a row {string}', (row: string) => {
  cy.get('cd-table .search input').first().clear().type(row);
  cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).click();
});

Then('I should see the modal', () => {
  cy.get('cd-modal').should('exist');
});

Then('I should not see the modal', () => {
  cy.get('cd-modal').should('not.exist');
});

/**
 * Some modals have an additional confirmation to be provided
 * by ticking the 'Are you sure?' box.
 */
Then('I check the tick box in modal', () => {
  cy.get('cd-modal .custom-control-label').click();
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

Then('I should see a row with {string}', (row: string) => {
  cy.get('cd-table .search input').first().clear().type(row);
  cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).should(
    'exist'
  );
});

Then('I should not see a row with {string}', (row: string) => {
  cy.get('cd-table .search input').first().clear().type(row);
  cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).should(
    'not.exist'
  );
});

Then('I should see rows with following entries', (entries) => {
  entries.hashes().forEach((entry: any) => {
    cy.get('cd-table .search input').first().clear().type(entry.hostname);
    cy.contains(
      `datatable-body-row datatable-body-cell .datatable-body-cell-label`,
      entry.hostname
    ).should('exist');
  });
});

And('I should see row {string} have {string}', (row: string, options: string) => {
  if (options) {
    cy.get('cd-table .search input').first().clear().type(row);
    for (const option of options.split(',')) {
      cy.contains(
        `datatable-body-row datatable-body-cell .datatable-body-cell-label .badge`,
        option
      ).should('exist');
    }
  }
});

And('I should see row {string} does not have {string}', (row: string, options: string) => {
  if (options) {
    cy.get('cd-table .search input').first().clear().type(row);
    for (const option of options.split(',')) {
      cy.contains(
        `datatable-body-row datatable-body-cell .datatable-body-cell-label .badge`,
        option
      ).should('not.exist');
    }
  }
});
