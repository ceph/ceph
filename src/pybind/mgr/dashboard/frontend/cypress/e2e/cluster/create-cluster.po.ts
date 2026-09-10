import { PageHelper } from '../page-helper.po';
import { NotificationSidebarPageHelper } from '../ui/notification.po';
import { HostsPageHelper } from './hosts.po';
import { ServicesPageHelper } from './services.po';

export class OnboardingHelper extends PageHelper {
  pages = { index: { url: '#/add-storage', id: 'cd-create-cluster' } };

  onboarding() {
    cy.get('cd-create-cluster').then(($cluster) => {
      if ($cluster.find('cd-tearsheet').length) {
        return;
      }
      cy.get('cd-create-cluster').should('contain.text', 'Welcome to Ceph Dashboard');
      cy.get('[aria-label="Add Storage"]').first().click({ force: true });
      cy.get('cd-tearsheet').should('exist');
    });
  }

  selectStep(stepLabel: string) {
    cy.get('cd-tearsheet cds-progress-indicator').contains(stepLabel).click();
  }

  openWizardStep(stepLabel: string) {
    cy.visit('/');
    this.navigateTo();
    this.onboarding();
    this.selectStep(stepLabel);
  }

  clickNext() {
    cy.get('cd-tearsheet').contains('button', 'Next').click();
  }

  submitStorage() {
    cy.get('cd-tearsheet .tearsheet-footer-submit').click();
  }

  doSkip() {
    cy.get('[aria-label="View cluster overview"]').first().click({ force: true });
    cy.contains('cd-modal button', 'Continue').click();

    cy.get('cd-overview').should('exist');
    const notification = new NotificationSidebarPageHelper();
    notification.open();
    notification.getNotifications().should('contain', 'Storage setup skipped by user');
  }
}

export class CreateClusterHostPageHelper extends HostsPageHelper {
  pages = {
    index: { url: '#/add-storage', id: 'cd-create-cluster' },
    add: { url: '', id: 'cd-host-form' }
  };

  columnIndex = {
    hostname: 1,
    labels: 2,
    status: 3,
    services: 0
  };
}

export class CreateClusterServicePageHelper extends ServicesPageHelper {
  pages = {
    index: { url: '#/add-storage', id: 'cd-create-cluster' },
    create: { url: '', id: 'cd-service-form' }
  };

  columnIndex = {
    service_name: 2,
    placement: 3
  };

  checkExist(serviceName: string, exist: boolean) {
    this.existTableCell(serviceName, exist);
  }

  expectPlacementCount(serviceName: string, expectedCount: string) {
    this.getTableRow(serviceName)
      .find(`[cdstabledata]:nth-child(${this.columnIndex.placement})`)
      .should(($cell) => {
        expect($cell.text()).to.include(`count:${expectedCount}`);
      });
  }
}
