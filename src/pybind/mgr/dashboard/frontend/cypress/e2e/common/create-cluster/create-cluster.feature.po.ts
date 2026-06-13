import { Given, Then } from 'cypress-cucumber-preprocessor/steps';

Given('I am on the {string} section', (page: string) => {
  cy.get('cd-tearsheet cds-progress-indicator').contains(page).click();
});

Then('I should see a message {string}', () => {
  cy.get('cd-create-cluster').should('contain.text', 'Welcome to Ceph Dashboard');
});
