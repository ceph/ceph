/* tslint:disable*/
import { Input, ManagerModulesPageHelper } from '../../cluster/mgr-modules.po';
import { CreateClusterWizardHelper } from '../../cluster/create-cluster.po';
import { HostsPageHelper } from '../../cluster/hosts.po';
import { ServicesPageHelper } from '../../cluster/services.po';
/* tslint:enable*/

describe('when cluster creation is completed', () => {
  const createCluster = new CreateClusterWizardHelper();
  const services = new ServicesPageHelper();
  const hosts = new HostsPageHelper();
  const mgrmodules = new ManagerModulesPageHelper();

  const hostnames = ['ceph-node-00', 'ceph-node-01', 'ceph-node-02', 'ceph-node-03'];

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
  });

  it('should redirect to dashboard landing page after cluster creation', () => {
    createCluster.navigateTo();
    createCluster.createCluster();

    cy.get('.nav-link').contains('Review').click();
    cy.get('button[aria-label="Next"]').click();
    cy.get('cd-dashboard').should('exist');
  });

  describe('Hosts page', () => {
    beforeEach(() => {
      hosts.navigateTo();
    });

    it('should check if monitoring stacks are running on the root host', () => {
      const monitoringStack = ['alertmanager', 'grafana', 'node-exporter', 'prometheus'];
      hosts.clickTab('cd-host-details', 'ceph-node-00', 'Daemons');
      for (const daemon of monitoringStack) {
        cy.get('cd-host-details').within(() => {
          services.checkServiceStatus(daemon);
        });
      }
    });

    // avoid creating node-exporter on the newly added host
    // to favour the host draining process
    it('should reduce the count for node-exporter', () => {
      services.editService('node-exporter', '3');
    });

    // grafana ip address is set to the fqdn by default.
    // kcli is not working with that, so setting the IP manually.
    it('should change ip address of grafana', { retries: 2 }, () => {
      const dashboardArr: Input[] = [
        {
          id: 'GRAFANA_API_URL',
          newValue: 'https://192.168.100.100:3000',
          oldValue: ''
        }
      ];
      mgrmodules.editMgrModule('dashboard', dashboardArr);
    });

    it('should add one more host', () => {
      hosts.navigateTo('add');
      hosts.add(hostnames[3]);
      hosts.checkExist(hostnames[3], true);
    });

    it('should have removed "_no_schedule" label', () => {
      for (const hostname of hostnames) {
        hosts.checkLabelExists(hostname, ['_no_schedule'], false);
      }
    });

    it('should display inventory', () => {
      hosts.clickTab('cd-host-details', hostnames[1], 'Physical Disks');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should display daemons', () => {
      hosts.clickTab('cd-host-details', hostnames[1], 'Daemons');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should check if mon daemon is running on all hosts', () => {
      for (const hostname of hostnames) {
        hosts.clickTab('cd-host-details', hostname, 'Daemons');
        cy.get('cd-host-details').within(() => {
          services.checkServiceStatus('mon');
        });
      }
    });
  });
});
