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

    it('should create and delete a smb service', () => {
      services.navigateTo('create');
      services.addService('smb');

      services.checkExist('smb.testsmb', true);

      services.deleteService('smb.testsmb');
    });

    it('should create and delete an oauth2-proxy service', () => {
      services.navigateTo('create');
      services.addService('oauth2-proxy');

      services.checkExist('oauth2-proxy', true);

      services.deleteService('oauth2-proxy');
    });

    it('should create and delete a mgmt-gateway service', () => {
      services.navigateTo('create');
      services.addService('mgmt-gateway');

      services.checkExist('mgmt-gateway', true);

      services.deleteService('mgmt-gateway');
    });
  });
});
