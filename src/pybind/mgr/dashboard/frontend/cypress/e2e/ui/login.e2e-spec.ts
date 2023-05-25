import { LoginPageHelper } from './login.po';

describe('Login page', () => {
  const login = new LoginPageHelper();

  it('should login and navigate to dashboard page', () => {
    login.navigateTo();
    login.doLogin();
  });

  it('should logout when clicking the button', () => {
    login.navigateTo();
    login.doLogin();

    login.doLogout();
  });

  it('should have no accessibility violations', () => {
    login.navigateTo();
    cy.injectAxe();
    cy.checkA11y();
  });
});
