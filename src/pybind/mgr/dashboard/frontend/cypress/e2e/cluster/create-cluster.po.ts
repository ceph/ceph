import { PageHelper } from '../page-helper.po';
import { NotificationSidebarPageHelper } from '../ui/notification.po';
import { HostsPageHelper } from './hosts.po';
import { ServicesPageHelper } from './services.po';

export class OnboardingHelper extends PageHelper {
  pages = { index: { url: '#/add-storage?welcome=true', id: 'cd-create-cluster' } };

  navigateTo(name: string = null) {
    name = name || 'index';
    const page = this.pages[name];

    this.stubOrchestratorEndpoints();
    cy.visit(page.url);
    cy.wait(['@orchStatus', '@orchConfig']);
    cy.get(page.id, { timeout: 60000 });
  }

  onboarding() {
    cy.get('cd-create-cluster').should('contain.text', 'Welcome to Ceph Dashboard');
    cy.get('[aria-label="Add Storage"]').first().click({ force: true });
    cy.get('cd-tearsheet').should('exist');
  }

  /**
   * Click a step on the outer create-cluster tearsheet only.
   * Nested forms (e.g. cd-osd-form) also render cds-progress-indicator.
   */
  selectStep(stepLabel: string) {
    cy.get('cd-create-cluster > cd-tearsheet cds-progress-indicator')
      .contains(stepLabel)
      .click({ force: true });
  }

  clickNext() {
    cy.get('cd-create-cluster > cd-tearsheet').contains('button', 'Next').click();
  }

  submitStorage() {
    cy.get('cd-create-cluster > cd-tearsheet .tearsheet-footer-submit').click();
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
    service_name: 1,
    placement: 2,
    running: 3,
    size: 4,
    last_refresh: 5
  };
}
