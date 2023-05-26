import { NavigationPageHelper } from '../ui/navigation.po';

describe('Navigation accessibility', { retries: 0 }, () => {
  const shared = new NavigationPageHelper();

  beforeEach(() => {
    cy.login();
    shared.navigateTo();
  });

  it('top-nav should have no accessibility violations', () => {
    cy.injectAxe();
    cy.checkAccessibility('.cd-navbar-top');
  });

  it('sidebar should have no accessibility violations', () => {
    cy.injectAxe();
    cy.checkAccessibility('nav[id=sidebar]');
  });
});
