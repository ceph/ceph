import { And, Then, When } from 'cypress-cucumber-preprocessor/steps';

// When you are clicking on an action in the table actions dropdown button
When('I click on {string} button from the table actions', (button: string) => {
  cy.get('.table-actions button.dropdown-toggle').first().click();
  cy.get(`[aria-label="${button}"]`).first().click();
});

// When you are clicking on an action inside the expanded table row
When('I click on {string} button from the expanded row', (button: string) => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('.table-actions button.dropdown-toggle').first().click();
    cy.get(`[aria-label="${button}"]`).first().click();
  });
});

When('I click on {string} button from the table actions in the expanded row', (button: string) => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('.table-actions button.dropdown-toggle').first().click();
    cy.get(`[aria-label="${button}"]`).first().click();
  });
});

When('I expand the row {string}', (row: string) => {
  cy.contains('.datatable-body-row', row).first().find('.tc_expand-collapse').click();
});

/**
 * Selects any row on the datatable if it matches the given name
 */
When('I select a row {string}', (row: string) => {
  cy.get('cd-table .search input').first().clear().type(row);
  cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).click();
});

When('I select a row {string} in the expanded row', (row: string) => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('cd-table .search input').first().clear().type(row);
    cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).click();
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

Then('I should see a table in the expanded row', () => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('cd-table').should('exist');
    cy.get('datatable-scroller, .empty-row');
  });
});

Then('I should not see a row with {string} in the expanded row', (row: string) => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('cd-table .search input').first().clear().type(row);
    cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).should(
      'not.exist'
    );
  });
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

And('I should see row {string} of the expanded row to have a usage bar', (row: string) => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('cd-table .search input').first().clear().type(row);
    cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).should(
      'exist'
    );
    cy.get('.datatable-body-row .datatable-body-cell .datatable-body-cell-label .progress').should(
      'exist'
    );
  });
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

Then('I should see a row with {string} in the expanded row', (row: string) => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('cd-table .search input').first().clear().type(row);
    cy.contains(`datatable-body-row datatable-body-cell .datatable-body-cell-label`, row).should(
      'exist'
    );
  });
});

And('I should see row {string} have {string} on this tab', (row: string, options: string) => {
  if (options) {
    cy.get('cd-table').should('exist');
    cy.get('datatable-scroller, .empty-row');
    cy.get('.datatable-row-detail').within(() => {
      cy.get('cd-table .search input').first().clear().type(row);
      for (const option of options.split(',')) {
        cy.contains(
          `datatable-body-row datatable-body-cell .datatable-body-cell-label span`,
          option
        ).should('exist');
      }
    });
  }
});

Then('I should see an alert {string} in the expanded row', (alert: string) => {
  cy.get('.datatable-row-detail').within(() => {
    cy.get('.alert-panel-text').contains(alert);
  });
});
