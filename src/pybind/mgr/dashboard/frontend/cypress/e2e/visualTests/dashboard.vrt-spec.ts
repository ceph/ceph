import { LoginPageHelper } from '../ui/login.po';

describe.skip('Dashboard Landing Page', () => {
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
    cy.get('[aria-label="Status card"]').should('be.visible');
    cy.get('[aria-label="Inventory card"]').should('be.visible');
    cy.get('[aria-label="Cluster utilization card"]').should('be.visible');
    cy.eyesCheckWindow({ tag: 'Dashboard landing page' });
  });
});
