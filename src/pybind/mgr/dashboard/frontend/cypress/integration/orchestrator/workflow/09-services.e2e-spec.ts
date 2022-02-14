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

  it('should create and delete snmp-gateway service with version V2c', () => {
    services.navigateTo('create');
    services.addService('snmp-gateway', false, '1', 'V2c');
    services.checkExist('snmp-gateway', true);

    services.clickServiceTab('snmp-gateway', 'Details');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('snmp-gateway');
    });

    services.deleteService('snmp-gateway');
  });

  it('should create and delete snmp-gateway service with version V3', () => {
    services.navigateTo('create');
    services.addService('snmp-gateway', false, '1', 'V3', true);
    services.checkExist('snmp-gateway', true);

    services.clickServiceTab('snmp-gateway', 'Details');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('snmp-gateway');
    });

    services.deleteService('snmp-gateway');
  });

  it('should create and delete snmp-gateway service with version V3 and w/o privacy protocol', () => {
    services.navigateTo('create');
    services.addService('snmp-gateway', false, '1', 'V3', false);
    services.checkExist('snmp-gateway', true);

    services.clickServiceTab('snmp-gateway', 'Details');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('snmp-gateway');
    });

    services.deleteService('snmp-gateway');
  });
});
