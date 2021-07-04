import { PageHelper } from '../page-helper.po';
import { NotificationSidebarPageHelper } from '../ui/notification.po';

const pages = {
  index: { url: '#/expand-cluster', id: 'cd-create-cluster' }
};

export class CreateClusterWizardHelper extends PageHelper {
  pages = pages;
  columnIndex = {
    hostname: 1,
    labels: 2,
    status: 3
  };

  createCluster() {
    cy.get('cd-create-cluster').should('contain.text', 'Please expand your cluster first');
    cy.get('[name=expand-cluster]').click();
  }

  doSkip() {
    cy.get('[name=skip-cluster-creation]').click();
    cy.contains('cd-modal button', 'Continue').click();

    cy.get('cd-dashboard').should('exist');
    const notification = new NotificationSidebarPageHelper();
    notification.open();
    notification.getNotifications().should('contain', 'Cluster expansion skipped by user');
  }

  check_for_host() {
    this.getTableCount('total').should('not.be.eq', 0);
  }

  clickHostTab(hostname: string, tabName: string) {
    this.getExpandCollapseElement(hostname).click();
    cy.get('cd-host-details').within(() => {
      this.getTab(tabName).click();
    });
  }

  add(hostname: string, exist?: boolean, maintenance?: boolean) {
    cy.get('.btn.btn-accent').first().click({ force: true });

    cy.get('cd-modal').should('exist');
    cy.get('cd-modal').within(() => {
      cy.get('#hostname').type(hostname);
      if (maintenance) {
        cy.get('label[for=maintenance]').click();
      }
      if (exist) {
        cy.get('#hostname').should('have.class', 'ng-invalid');
      }
      cy.get('cd-submit-button').click();
    });
    // back to host list
    cy.get(`${this.pages.index.id}`);
  }

  checkExist(hostname: string, exist: boolean) {
    this.clearTableSearchInput();
    this.getTableCell(this.columnIndex.hostname, hostname).should(($elements) => {
      const hosts = $elements.map((_, el) => el.textContent).get();
      if (exist) {
        expect(hosts).to.include(hostname);
      } else {
        expect(hosts).to.not.include(hostname);
      }
    });
  }

  delete(hostname: string) {
    super.delete(hostname, this.columnIndex.hostname);
  }

  // Add or remove labels on a host, then verify labels in the table
  editLabels(hostname: string, labels: string[], add: boolean) {
    this.getTableCell(this.columnIndex.hostname, hostname).click();
    this.clickActionButton('edit');

    // add or remove label badges
    if (add) {
      cy.get('cd-modal').find('.select-menu-edit').click();
      for (const label of labels) {
        cy.contains('cd-modal .badge', new RegExp(`^${label}$`)).should('not.exist');
        cy.get('.popover-body input').type(`${label}{enter}`);
      }
    } else {
      for (const label of labels) {
        cy.contains('cd-modal .badge', new RegExp(`^${label}$`))
          .find('.badge-remove')
          .click();
      }
    }
    cy.get('cd-modal cd-submit-button').click();

    // Verify labels are added or removed from Labels column
    // First find row with hostname, then find labels in the row
    this.getTableCell(this.columnIndex.hostname, hostname)
      .parent()
      .find(`datatable-body-cell:nth-child(${this.columnIndex.labels}) .badge`)
      .should(($ele) => {
        const newLabels = $ele.toArray().map((v) => v.innerText);
        for (const label of labels) {
          if (add) {
            expect(newLabels).to.include(label);
          } else {
            expect(newLabels).to.not.include(label);
          }
        }
      });
  }
}
