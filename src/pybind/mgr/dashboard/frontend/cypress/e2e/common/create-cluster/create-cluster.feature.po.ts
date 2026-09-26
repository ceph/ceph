import { Given, Then } from 'cypress-cucumber-preprocessor/steps';

/**
 * Opens the Add Storage wizard reliably across scenarios.
 * If the tearsheet is already open (same SPA session), skip the welcome click.
 */
Given('I open the Add Storage wizard', () => {
  cy.location('hash').then((hash) => {
    if (hash.includes('/add-storage')) {
      return;
    }
    cy.visit('#/dashboard', { failOnStatusCode: false });
    cy.visit('#/add-storage?welcome=true', { failOnStatusCode: false });
  });
  cy.get('cd-create-cluster', { timeout: 120000 }).should('exist');
  cy.get('cd-create-cluster').then(($cluster) => {
    if ($cluster.find('cd-tearsheet').length) {
      return;
    }
    cy.get('[aria-label="Add Storage"]').first().click({ force: true });
  });
  cy.get('cd-tearsheet').should('exist');
});

Given('I am on the {string} section', (page: string) => {
  cy.get('cd-tearsheet cds-progress-indicator').contains(page).click();
});

Then('I should see a message {string}', () => {
  cy.get('cd-create-cluster').should('contain.text', 'Welcome to Ceph Dashboard');
  cy.contains('Welcome to Ceph Dashboard').should('be.visible');
});
