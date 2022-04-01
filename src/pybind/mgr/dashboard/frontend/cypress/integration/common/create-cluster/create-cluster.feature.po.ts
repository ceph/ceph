import { Given, Then } from 'cypress-cucumber-preprocessor/steps';

Given('I am on the {string} section', (page: string) => {
  cy.get('cd-wizard').within(() => {
    cy.get('.nav-link').contains(page).click();
    cy.get('.nav-link.active').contains(page);
  });
});

Then('I go to the {string} section', (page: string) => {
  cy.get('cd-wizard').within(() => {
    cy.get('.nav-link').contains(page).click();
    cy.get('.nav-link.active').contains(page);
  });
});

Then('I should see a message {string}', () => {
  cy.get('cd-create-cluster').should('contain.text', 'Please expand your cluster first');
});
