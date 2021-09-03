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

  serviceColumnIndex = {
    service_name: 1,
    placement: 2
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
    super.delete(hostname, this.columnIndex.hostname, 'hosts');
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
    this.checkLabelExists(hostname, labels, add);
  }

  checkLabelExists(hostname: string, labels: string[], add: boolean) {
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

  createOSD(deviceType: 'hdd' | 'ssd') {
    // Click Primary devices Add button
    cy.get('cd-osd-devices-selection-groups[name="Primary"]').as('primaryGroups');
    cy.get('@primaryGroups').find('button').click();

    // Select all devices with `deviceType`
    cy.get('cd-osd-devices-selection-modal').within(() => {
      cy.get('.modal-footer .tc_submitButton').as('addButton').should('be.disabled');
      this.filterTable('Type', deviceType);
      this.getTableCount('total').should('be.gte', 1);
      cy.get('@addButton').click();
    });
  }

  private selectServiceType(serviceType: string) {
    return this.selectOption('service_type', serviceType);
  }

  addService(serviceType: string) {
    cy.get('.btn.btn-accent').first().click({ force: true });
    cy.get('cd-modal').should('exist');
    cy.get('cd-modal').within(() => {
      this.selectServiceType(serviceType);
      if (serviceType === 'rgw') {
        cy.get('#service_id').type('rgw');
        cy.get('#count').type('1');
      } else if (serviceType === 'ingress') {
        this.selectOption('backend_service', 'rgw.rgw');
        cy.get('#service_id').should('have.value', 'rgw.rgw');
        cy.get('#virtual_ip').type('192.168.20.1/24');
        cy.get('#frontend_port').type('8081');
        cy.get('#monitor_port').type('8082');
      }

      cy.get('cd-submit-button').click();
    });
  }

  checkServiceExist(serviceName: string, exist: boolean) {
    this.getTableCell(this.serviceColumnIndex.service_name, serviceName).should(($elements) => {
      const services = $elements.map((_, el) => el.textContent).get();
      if (exist) {
        expect(services).to.include(serviceName);
      } else {
        expect(services).to.not.include(serviceName);
      }
    });
  }

  deleteService(serviceName: string, wait: number) {
    const getRow = this.getTableCell.bind(this, this.serviceColumnIndex.service_name);
    getRow(serviceName).click();

    // Clicks on table Delete button
    this.clickActionButton('delete');

    // Confirms deletion
    cy.get('cd-modal .custom-control-label').click();
    cy.contains('cd-modal button', 'Delete').click();

    // Wait for modal to close
    cy.get('cd-modal').should('not.exist');

    // wait for delete operation to complete: tearing down the service daemons
    cy.wait(wait);

    this.checkServiceExist(serviceName, false);
  }
}
