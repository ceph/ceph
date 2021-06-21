import { CreateClusterWelcomePageHelper } from 'cypress/integration/cluster/cluster-welcome-page.po';
import { HostsPageHelper } from 'cypress/integration/cluster/hosts.po';

describe('Create cluster add host page', () => {
  const createCluster = new CreateClusterWelcomePageHelper();
  const hosts = new HostsPageHelper();
  const hostnames = ['ceph-node-00.cephlab.com', 'ceph-node-01.cephlab.com', 'ceph-node-02.cephlab.com'];
  const addHost = (hostname: string, exist?: boolean) => {
    hosts.navigateTo('add');
    hosts.add(hostname, exist, true);
    hosts.checkExist(hostname, true);
  };

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();
  });

  it('should check if nav-link and title contains Add Hosts', () => {
    cy.get('.nav-link').should('contain.text', 'Add Hosts');

    cy.get('.title').should('contain.text', 'Add Hosts');
  });

  it('should check existing host and add new hosts into maintenance mode', () => {
    hosts.checkExist(hostnames[0], true);

    addHost(hostnames[1], false);
    addHost(hostnames[2], false);
  });

  it('should not add an existing host', () => {
    hosts.navigateTo('add');
    hosts.add(hostnames[1], true);
  });

  it('should edit host labels', () => {
    const labels = ['foo', 'bar'];
    hosts.editLabels(hostnames[0], labels, true);
    hosts.editLabels(hostnames[0], labels, false);
  });

  it('should delete a host', () => {
    hosts.delete(hostnames[1]);
  });
});
