import { ServicesPageHelper } from 'cypress/integration/cluster/services.po';

describe('Services page', () => {
  const services = new ServicesPageHelper();
  const mdsDaemonName = 'mds.test';
  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    services.navigateTo();
  });

  it('should check if rgw service is created', () => {
    services.checkExist('rgw.foo', true);
  });

  it('should create an mds service', () => {
    services.navigateTo('create');
    services.addService('mds', false);
    services.checkExist(mdsDaemonName, true);

    services.clickServiceTab(mdsDaemonName, 'Details');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName);
    });
  });

  it('should stop a daemon', () => {
    services.clickServiceTab(mdsDaemonName, 'Details');
    services.checkServiceStatus(mdsDaemonName);

    services.daemonAction('mds', 'stop');
    services.checkServiceStatus(mdsDaemonName, 'stopped');
  });

  it('should restart a daemon', () => {
    services.checkExist(mdsDaemonName, true);
    services.clickServiceTab(mdsDaemonName, 'Details');
    services.daemonAction('mds', 'restart');
    services.checkServiceStatus(mdsDaemonName, 'running');
  });

  it('should redeploy a daemon', () => {
    services.checkExist(mdsDaemonName, true);
    services.clickServiceTab(mdsDaemonName, 'Details');

    services.daemonAction('mds', 'stop');
    services.checkServiceStatus(mdsDaemonName, 'stopped');
    services.daemonAction('mds', 'redeploy');
    services.checkServiceStatus(mdsDaemonName, 'running');
  });

  it('should start a daemon', () => {
    services.checkExist(mdsDaemonName, true);
    services.clickServiceTab(mdsDaemonName, 'Details');

    services.daemonAction('mds', 'stop');
    services.checkServiceStatus(mdsDaemonName, 'stopped');
    services.daemonAction('mds', 'start');
    services.checkServiceStatus(mdsDaemonName, 'running');
  });

  it('should delete an mds service', () => {
    services.deleteService(mdsDaemonName);
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
