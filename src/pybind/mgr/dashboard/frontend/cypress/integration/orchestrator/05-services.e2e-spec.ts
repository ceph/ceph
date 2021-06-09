import { ServicesPageHelper } from '../cluster/services.po';

describe('Services page', () => {
  const services = new ServicesPageHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    services.navigateTo();
  });

  describe('when Orchestrator is available', () => {
    it('should create an rgw service', () => {
      services.navigateTo('create');
      services.addService('rgw');

      services.checkExist('rgw.rgw.foo', true);
    });

    it('should create and delete an ingress service', () => {
      services.navigateTo('create');
      services.addService('ingress');

      services.checkExist('ingress.rgw.rgw.foo', true);

      services.deleteService('ingress.rgw.rgw.foo', 5000);
    });
  });
});
