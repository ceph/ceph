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
  cy.get(`[aria-label="${button}"]`).first().click({ force: true });
});

/**
 * General purpose click on function
 * @param dataTestid data-testid of the element to click on
 */
And('I click on {string}', (dataTestid: string) => {
  cy.get(`[data-testid=${dataTestid}]`).click();
});

/**
 * General purpose click on function in modal
 * @param dataTestid data-testid of the element to click on
 */
And('I click on {string} in the carbon modal', (dataTestid: string) => {
  cy.get('cds-modal').within(() => {
    cy.get(`[data-testid="${dataTestid}"]`).click();
  });
});

Then('I should see the modal', () => {
  cy.get('cd-modal').should('exist');
});

// @TODO: Replace with the existing (above one)
// once carbon migration is completed
Then('I should see the carbon modal', () => {
  cy.get('cds-modal').should('exist');
});

Then('I should not see the modal', () => {
  cy.get('cd-modal').should('not.exist');
});

Then('I should not see the carbon modal', () => {
  cy.get('cds-modal').should('not.exist');
});

And('I go to the {string} tab', (names: string) => {
  for (const name of names.split(', ')) {
    cy.contains('.nav.nav-tabs a', name).click();
  }
});

And('I go to the {string} carbon tab', (name: string) => {
  cy.get('cds-tabs').within(() => {
    cy.contains('button', name).click();
  });
});

And('I wait for {string} seconds', (seconds: number) => {
  cy.wait(seconds * 1000);
});

Given('a volume is available', () => {
  cy.request({
    method: 'POST',
    url: '/api/cephfs',
    body: { name: 'testFs', service_spec: { placement: {} } },
    headers: { Accept: 'application/vnd.ceph.api.v1.0+json' }
  });
});

Given('a subvolume group is available', () => {
  cy.request({
    method: 'POST',
    url: '/api/cephfs/subvolume/group',
    body: { vol_name: 'testFs', group_name: 'testSubvGrp' },
    headers: { Accept: 'application/vnd.ceph.api.v1.0+json' }
  });
});
