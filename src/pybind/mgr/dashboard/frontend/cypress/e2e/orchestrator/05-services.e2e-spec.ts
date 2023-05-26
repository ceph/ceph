import { ServicesPageHelper } from '../cluster/services.po';

describe('Services page', () => {
  const services = new ServicesPageHelper();
  const serviceName = 'rgw.foo';

  beforeEach(() => {
    cy.login();
    services.navigateTo();
  });

  describe('when Orchestrator is available', () => {
    it('should create an rgw service', () => {
      services.navigateTo('create');
      services.addService('rgw');

      services.checkExist(serviceName, true);
    });

    it('should edit a service', () => {
      const count = '2';
      services.editService(serviceName, count);
      services.expectPlacementCount(serviceName, count);
    });

    it('should create and delete an ingress service', () => {
      services.navigateTo('create');
      services.addService('ingress');

      services.checkExist('ingress.rgw.foo', true);

      services.deleteService('ingress.rgw.foo');
    });
  });
});
