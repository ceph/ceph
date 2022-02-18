import { LoginPageHelper } from '../ui/login.po';

describe('Dashboard Landing Page', () => {
  const login = new LoginPageHelper();

  beforeEach(() => {
    cy.eyesOpen({
      testName: 'Dashboard Component'
    });
  });

  afterEach(() => {
    cy.eyesClose();
  });

  it('should take screenshot of dashboard landing page', () => {
    login.navigateTo();
    login.doLogin();
    cy.get('.card-text').should('be.visible');
    cy.eyesCheckWindow({ tag: 'Dashboard landing page', ignore: { selector: '.card-text' } });
  });
});
