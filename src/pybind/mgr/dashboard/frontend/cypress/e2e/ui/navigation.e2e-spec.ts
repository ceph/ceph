import { NavigationPageHelper } from './navigation.po';

describe('Shared pages', () => {
  const shared = new NavigationPageHelper();

  beforeEach(() => {
    cy.login();
    shared.navigateTo();
  });

  it('should display the vertical menu by default', () => {
    shared.getVerticalMenu().should('not.have.class', 'active');
  });

  it('should hide the vertical menu', () => {
    shared.getMenuToggler().click();
    shared.getVerticalMenu().should('have.class', 'active');
  });

  it('should navigate to the correct page', () => {
    shared.checkNavigations(shared.navigations);
  });
});
