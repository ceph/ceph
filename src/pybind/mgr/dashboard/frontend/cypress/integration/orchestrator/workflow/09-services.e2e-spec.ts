import { ServicesPageHelper } from 'cypress/integration/cluster/services.po';

describe('Services page', () => {
  const services = new ServicesPageHelper();
  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    services.navigateTo();
  });

  it('should check if rgw service is created', () => {
    services.checkExist('rgw.foo', true);
  });

  it('should create and delete an mds service', () => {
    services.navigateTo('create');
    services.addService('mds', false);
    services.checkExist('mds.test', true);

    services.clickServiceTab('mds.test', 'Details');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('mds');
    });

    services.deleteService('mds.test');
  });
});
