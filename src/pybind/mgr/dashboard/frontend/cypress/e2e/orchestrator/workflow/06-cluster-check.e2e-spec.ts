/* tslint:disable*/
import { OnboardingHelper } from '../../cluster/create-cluster.po';
import { HostsPageHelper } from '../../cluster/hosts.po';
import { ServicesPageHelper } from '../../cluster/services.po';
/* tslint:enable*/

describe('when cluster creation is completed', () => {
  const onboarding = new OnboardingHelper();
  const services = new ServicesPageHelper();
  const hosts = new HostsPageHelper();

  const hostnames = ['ceph-node-00', 'ceph-node-01', 'ceph-node-02', 'ceph-node-03'];

  beforeEach(() => {
    cy.login();
  });

  it('should redirect to overview landing page after adding storage', () => {
    onboarding.navigateTo();
    onboarding.onboarding();

    // Explicitly skip OSD Creation Step so that it prevents from
    // deploying OSDs to the hosts automatically.
    onboarding.selectStep('Create OSDs');
    cy.get('#skipStepBtn').click();

    onboarding.selectStep('Review');
    onboarding.submitStorage();
    cy.get('cd-overview').should('exist');
  });

  describe('Hosts page', () => {
    beforeEach(() => {
      hosts.navigateTo();
    });

    it('should add one more host', () => {
      hosts.navigateTo('add');
      hosts.add(hostnames[3]);
      hosts.checkExist(hostnames[3], true);
    });

    it('should check if monitoring stacks are running on the root host', { retries: 2 }, () => {
      const monitoringStack = ['alertmanager', 'grafana', 'node-exporter', 'prometheus'];
      hosts.clickHostTab('ceph-node-00', 'Service instances');
      for (const daemon of monitoringStack) {
        cy.get('cd-host-resource-page').within(() => {
          services.checkServiceStatus(daemon);
        });
      }
    });

    it('should have removed "_no_schedule" label', () => {
      for (const hostname of hostnames) {
        hosts.checkLabelExists(hostname, ['_no_schedule'], false);
      }
    });

    it('should display inventory', () => {
      hosts.clickHostTab(hostnames[1], 'Storage Devices');
      cy.get('cd-host-resource-page cd-inventory').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should display daemons', () => {
      hosts.clickHostTab(hostnames[1], 'Service instances');
      cy.get('cd-host-resource-page cd-service-daemon-list').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should check if mon daemon is running on all hosts', () => {
      for (const hostname of hostnames) {
        hosts.clickHostTab(hostname, 'Service instances');
        cy.get('cd-host-resource-page').within(() => {
          services.checkServiceStatus('mon');
        });
      }
    });
  });
});
