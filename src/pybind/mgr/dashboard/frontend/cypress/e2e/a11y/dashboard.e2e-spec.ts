import { DashboardV3PageHelper } from '../ui/dashboard-v3.po';

describe('Overview Page', { retries: 0 }, () => {
  const overview = new DashboardV3PageHelper();

  beforeEach(() => {
    cy.intercept('GET', '**/api/prometheus/data*', {
      statusCode: 200,
      body: {
        status: 'success',
        data: {
          resultType: 'matrix',
          result: []
        }
      }
    });
    cy.intercept('GET', '**/api/prometheus/prometheus_query_data*', {
      statusCode: 200,
      body: {
        status: 'success',
        data: {
          resultType: 'vector',
          result: []
        }
      }
    });
    cy.login();
    overview.navigateTo();
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
