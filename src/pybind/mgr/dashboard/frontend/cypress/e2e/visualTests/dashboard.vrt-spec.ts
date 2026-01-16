import { LoginPageHelper } from '../ui/login.po';

describe.skip('Overview Landing Page', () => {
  const login = new LoginPageHelper();

  beforeEach(() => {
    cy.eyesOpen({
      testName: 'Overview Component'
    });
  });

  afterEach(() => {
    cy.eyesClose();
  });

  it('should take screenshot of overview landing page', () => {
    login.navigateTo();
    login.doLogin();
    cy.get('[aria-label="Status card"]').should('be.visible');
    cy.get('[aria-label="Inventory card"]').should('be.visible');
    cy.get('[aria-label="Cluster utilization card"]').should('be.visible');
    cy.eyesCheckWindow({ tag: 'Overview landing page' });
  });
});
