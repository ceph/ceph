import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/services', id: 'cd-services' },
  create: { url: '#/services/create', id: 'cd-service-form' }
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

  check_for_service() {
    this.getTableCount('total').should('not.be.eq', 0);
  }

  private selectServiceType(serviceType: string) {
    return this.selectOption('service_type', serviceType);
  }

  @PageHelper.restrictTo(pages.create.url)
  addService(serviceType: string, exist?: boolean) {
    cy.get(`${this.pages.create.id}`).within(() => {
      this.selectServiceType(serviceType);
      if (serviceType === 'rgw') {
        cy.get('#service_id').type('rgw.foo');
        cy.get('#count').type('1');
      } else if (serviceType === 'ingress') {
        this.selectOption('backend_service', 'rgw.rgw.foo');
        cy.get('#service_id').should('have.value', 'rgw.rgw.foo');
        cy.get('#virtual_ip').type('192.168.20.1/24');
        cy.get('#frontend_port').type('8081');
        cy.get('#monitor_port').type('8082');
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

  @PageHelper.restrictTo(pages.index.url)
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

  @PageHelper.restrictTo(pages.index.url)
  deleteService(serviceName: string, wait: number) {
    const getRow = this.getTableCell.bind(this, this.columnIndex.service_name);
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

    this.checkExist(serviceName, false);
  }
}
