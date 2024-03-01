import { Given, Then } from 'cypress-cucumber-preprocessor/steps';

Given('I am on the {string} section', (page: string) => {
  cy.get('cd-wizard').within(() => {
    cy.get('.nav-link').should('contain.text', page).first().click();
    cy.get('.nav-link.active').should('contain.text', page);
  });
});

Then('I should see a message {string}', () => {
  cy.get('cd-create-cluster').should('contain.text', 'Please expand your cluster first');
});
