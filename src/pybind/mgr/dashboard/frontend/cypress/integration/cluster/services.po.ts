import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/services', id: 'cd-services' },
  create: { url: '#/services/(modal:create)', id: 'cd-service-form' }
};

export class ServicesPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    service_name: 2,
    placement: 3,
    running: 4,
    size: 5,
    last_refresh: 6
  };

  serviceDetailColumnIndex = {
    hostname: 1,
    daemonType: 2,
    status: 8
  };

  check_for_service() {
    this.getTableCount('total').should('not.be.eq', 0);
  }

  private selectServiceType(serviceType: string) {
    return this.selectOption('service_type', serviceType);
  }

  clickServiceTab(serviceName: string, tabName: string) {
    this.getExpandCollapseElement(serviceName).click();
    cy.get('cd-service-details').within(() => {
      this.getTab(tabName).click();
    });
  }

  addService(serviceType: string, exist?: boolean, count = '1') {
    cy.get(`${this.pages.create.id}`).within(() => {
      this.selectServiceType(serviceType);
      switch (serviceType) {
        case 'rgw':
          cy.get('#service_id').type('foo');
          cy.get('#count').type(count);
          break;

        case 'ingress':
          this.selectOption('backend_service', 'rgw.foo');
          cy.get('#service_id').should('have.value', 'rgw.foo');
          cy.get('#virtual_ip').type('192.168.20.1/24');
          cy.get('#frontend_port').type('8081');
          cy.get('#monitor_port').type('8082');
          break;

        case 'nfs':
          cy.get('#service_id').type('testnfs');
          cy.get('#count').type(count);
          break;
      }

      cy.get('cd-submit-button').click();
    });
    if (exist) {
      cy.get('#service_id').should('have.class', 'ng-invalid');
    } else {
      // back to service list
      cy.get(`${this.pages.index.id}`);
    }
  }

  checkServiceStatus(daemon: string) {
    this.getTableCell(this.serviceDetailColumnIndex.daemonType, daemon)
      .parent()
      .find(`datatable-body-cell:nth-child(${this.serviceDetailColumnIndex.status}) .badge`)
      .should(($ele) => {
        const status = $ele.toArray().map((v) => v.innerText);
        expect(status).to.include('running');
      });
  }

  checkExist(serviceName: string, exist: boolean) {
    this.getTableCell(this.columnIndex.service_name, serviceName).should(($elements) => {
      const services = $elements.map((_, el) => el.textContent).get();
      if (exist) {
        expect(services).to.include(serviceName);
      } else {
        expect(services).to.not.include(serviceName);
      }
    });
  }

  deleteService(serviceName: string) {
    const getRow = this.getTableCell.bind(this, this.columnIndex.service_name);
    getRow(serviceName).click();

    // Clicks on table Delete button
    this.clickActionButton('delete');

    // Confirms deletion
    cy.get('cd-modal .custom-control-label').click();
    cy.contains('cd-modal button', 'Delete').click();

    // Wait for modal to close
    cy.get('cd-modal').should('not.exist');
    this.checkExist(serviceName, false);
  }
}
