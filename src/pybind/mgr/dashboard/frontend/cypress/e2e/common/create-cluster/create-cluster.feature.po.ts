import { Given, Then } from 'cypress-cucumber-preprocessor/steps';

Given('I am on the {string} section', (page: string) => {
  cy.get('cd-wizard').within(() => {
    cy.get('button').should('have.attr', 'title', page).first().click();
    cy.get('.cds--assistive-text').should('contain.text', 'Current');
  });
});

Then('I should see a message {string}', () => {
  cy.get('cd-create-cluster').should('contain.text', 'Please expand your cluster first');
});
