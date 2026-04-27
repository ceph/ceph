import { OverviewPagehelper } from '../ui/overview.po';

describe('Overview Page', { retries: 0 }, () => {
  const overview = new OverviewPagehelper();

  beforeEach(() => {
    cy.login();
    overview.navigateTo();
  });

  describe('Overview accessibility', () => {
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
