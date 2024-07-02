import { And, Given, Then, When } from 'cypress-cucumber-preprocessor/steps';

import { UrlsCollection } from './urls.po';

const urlsCollection = new UrlsCollection();

Given('I am logged in', () => {
  cy.login();
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

Then('I should see the modal', () => {
  cy.get('cd-modal').should('exist');
});

Then('I should not see the modal', () => {
  cy.get('cd-modal').should('not.exist');
});

And('I go to the {string} tab', (names: string) => {
  for (const name of names.split(', ')) {
    cy.contains('.nav.nav-tabs a', name).click();
  }
});

And('I wait for {string} seconds', (seconds: number) => {
  cy.wait(seconds * 1000);
});
