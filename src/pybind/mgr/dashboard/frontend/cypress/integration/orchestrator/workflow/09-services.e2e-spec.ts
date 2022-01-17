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

  it('should create an mds service', () => {
    services.navigateTo('create');
    services.addService('mds', false);
    services.checkExist('mds.test', true);

    services.clickServiceTab('mds.test', 'Details');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('mds');
    });
  });

  it('should stop a daemon', () => {
    services.clickServiceTab('mds.test', 'Details');
    services.checkServiceStatus('mds');

    services.daemonAction('mds', 'stop');
    services.checkServiceStatus('mds', 'stopped');
  });

  it('should restart a daemon', () => {
    services.checkExist('mds.test', true);
    services.clickServiceTab('mds.test', 'Details');
    services.daemonAction('mds', 'restart');
    services.checkServiceStatus('mds', 'running');
  });

  it('should redeploy a daemon', () => {
    services.checkExist('mds.test', true);
    services.clickServiceTab('mds.test', 'Details');

    services.daemonAction('mds', 'stop');
    services.checkServiceStatus('mds', 'stopped');
    services.daemonAction('mds', 'redeploy');
    services.checkServiceStatus('mds', 'running');
  });

  it('should start a daemon', () => {
    services.checkExist('mds.test', true);
    services.clickServiceTab('mds.test', 'Details');

    services.daemonAction('mds', 'stop');
    services.checkServiceStatus('mds', 'stopped');
    services.daemonAction('mds', 'start');
    services.checkServiceStatus('mds', 'running');
  });

  it('should delete an mds service', () => {
    services.deleteService('mds.test');
  });
});
