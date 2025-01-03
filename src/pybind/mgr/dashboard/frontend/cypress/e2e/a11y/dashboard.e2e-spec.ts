import { DashboardPageHelper } from '../ui/dashboard.po';

describe('Dashboard Main Page', { retries: 0 }, () => {
  const dashboard = new DashboardPageHelper();

  beforeEach(() => {
    cy.login();
    dashboard.navigateTo();
  });

  describe('Dashboard accessibility', () => {
    it('should have no accessibility violations', () => {
      cy.injectAxe();
      cy.checkAccessibility(
        {
          exclude: [['.cd-navbar-main']]
        },
        {
          rules: {
            'page-has-heading-one': { enabled: false }
          }
        }
      );
    });
  });
});
