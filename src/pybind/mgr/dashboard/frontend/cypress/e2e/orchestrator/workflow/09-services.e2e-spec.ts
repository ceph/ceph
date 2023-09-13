/* tslint:disable*/
import { ServicesPageHelper } from '../../cluster/services.po';
/* tslint:enable*/

describe('Services page', () => {
  const services = new ServicesPageHelper();
  const mdsDaemonName = 'mds.test';
  beforeEach(() => {
    cy.login();
    services.navigateTo();
  });

  it('should check if rgw service is created', () => {
    services.checkExist('rgw.foo', true);
  });

  it('should create an mds service', () => {
    services.navigateTo('create');
    services.addService('mds', false);
    services.checkExist(mdsDaemonName, true);

    services.clickServiceTab(mdsDaemonName, 'Daemons');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName);
    });
  });

  it('should stop a daemon', () => {
    services.clickServiceTab(mdsDaemonName, 'Daemons');
    services.checkServiceStatus(mdsDaemonName);

    services.daemonAction('mds', 'stop');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName, 'stopped');
    });
  });

  it('should restart a daemon', () => {
    services.checkExist(mdsDaemonName, true);
    services.clickServiceTab(mdsDaemonName, 'Daemons');
    services.daemonAction('mds', 'restart');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName, 'running');
    });
  });

  it('should redeploy a daemon', () => {
    services.checkExist(mdsDaemonName, true);
    services.clickServiceTab(mdsDaemonName, 'Daemons');

    services.daemonAction('mds', 'stop');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName, 'stopped');
    });
    services.daemonAction('mds', 'redeploy');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName, 'running');
    });
  });

  it('should start a daemon', () => {
    services.checkExist(mdsDaemonName, true);
    services.clickServiceTab(mdsDaemonName, 'Daemons');

    services.daemonAction('mds', 'stop');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName, 'stopped');
    });
    services.daemonAction('mds', 'start');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus(mdsDaemonName, 'running');
    });
  });

  it('should delete an mds service', () => {
    services.deleteService(mdsDaemonName);
  });

  it('should create and delete snmp-gateway service with version V2c', () => {
    services.navigateTo('create');
    services.addService('snmp-gateway', false, 1, 'V2c');
    services.checkExist('snmp-gateway', true);

    services.clickServiceTab('snmp-gateway', 'Daemons');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('snmp-gateway');
    });

    services.deleteService('snmp-gateway');
  });

  it('should create and delete snmp-gateway service with version V3', () => {
    services.navigateTo('create');
    services.addService('snmp-gateway', false, 1, 'V3', true);
    services.checkExist('snmp-gateway', true);

    services.clickServiceTab('snmp-gateway', 'Daemons');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('snmp-gateway');
    });

    services.deleteService('snmp-gateway');
  });

  it('should create and delete snmp-gateway service with version V3 and w/o privacy protocol', () => {
    services.navigateTo('create');
    services.addService('snmp-gateway', false, 1, 'V3', false);
    services.checkExist('snmp-gateway', true);

    services.clickServiceTab('snmp-gateway', 'Daemons');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('snmp-gateway');
    });

    services.deleteService('snmp-gateway');
  });

  it('should create ingress as unmanaged', () => {
    services.navigateTo('create');
    services.addService('ingress', false, undefined, undefined, undefined, true);
    services.checkExist('ingress.rgw.foo', true);
    services.isUnmanaged('ingress.rgw.foo', true);
    services.deleteService('ingress.rgw.foo');
  });

  it('should check if exporter daemons are running', () => {
    services.clickServiceTab('ceph-exporter', 'Daemons');
    cy.get('cd-service-details').within(() => {
      services.checkServiceStatus('ceph-exporter', 'running');
    });
  });
});
