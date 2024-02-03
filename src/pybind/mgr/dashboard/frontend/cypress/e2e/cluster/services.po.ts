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
    daemonName: 2,
    status: 4
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

  addService(
    serviceType: string,
    exist?: boolean,
    count = 1,
    snmpVersion?: string,
    snmpPrivProtocol?: boolean,
    unmanaged = false
  ) {
    cy.get(`${this.pages.create.id}`).within(() => {
      this.selectServiceType(serviceType);
      switch (serviceType) {
        case 'rgw':
          cy.get('#service_id').type('foo');
          unmanaged ? cy.get('label[for=unmanaged]').click() : cy.get('#count').type(String(count));
          break;

        case 'ingress':
          if (unmanaged) {
            cy.get('label[for=unmanaged]').click();
          }
          this.selectOption('backend_service', 'rgw.foo');
          cy.get('#service_id').should('have.value', 'rgw.foo');
          cy.get('#virtual_ip').type('192.168.100.1/24');
          cy.get('#frontend_port').type('8081');
          cy.get('#monitor_port').type('8082');
          break;

        case 'nfs':
          cy.get('#service_id').type('testnfs');
          unmanaged ? cy.get('label[for=unmanaged]').click() : cy.get('#count').type(String(count));
          break;

        case 'snmp-gateway':
          this.selectOption('snmp_version', snmpVersion);
          cy.get('#snmp_destination').type('192.168.0.1:8443');
          if (snmpVersion === 'V2c') {
            cy.get('#snmp_community').type('public');
          } else {
            cy.get('#engine_id').type('800C53F00000');
            this.selectOption('auth_protocol', 'SHA');
            if (snmpPrivProtocol) {
              this.selectOption('privacy_protocol', 'DES');
              cy.get('#snmp_v3_priv_password').type('testencrypt');
            }

            // Credentials
            cy.get('#snmp_v3_auth_username').type('test');
            cy.get('#snmp_v3_auth_password').type('testpass');
          }
          break;

        default:
          cy.get('#service_id').type('test');
          unmanaged ? cy.get('label[for=unmanaged]').click() : cy.get('#count').type(String(count));
          break;
      }
      if (serviceType === 'snmp-gateway') {
        cy.get('cd-submit-button').dblclick();
      } else {
        cy.get('cd-submit-button').click();
      }
    });
    if (exist) {
      cy.get('#service_id').should('have.class', 'ng-invalid');
    } else {
      // back to service list
      cy.get(`${this.pages.index.id}`);
    }
  }

  editService(name: string, daemonCount: string) {
    this.navigateEdit(name, true, false);
    cy.get(`${this.pages.create.id}`).within(() => {
      cy.get('#service_type').should('be.disabled');
      cy.get('#service_id').should('be.disabled');
      cy.get('#count').clear().type(daemonCount);
      cy.get('cd-submit-button').click();
    });
  }

  checkServiceStatus(daemon: string, expectedStatus = 'running') {
    let daemonNameIndex = this.serviceDetailColumnIndex.daemonName;
    let statusIndex = this.serviceDetailColumnIndex.status;

    // since hostname row is hidden from the hosts details table,
    // we'll need to manually override the indexes when this check is being
    // done for the daemons in host details page. So we'll get the url and
    // verify if the current page is not the services index page
    cy.url().then((url) => {
      if (!url.includes(pages.index.url)) {
        daemonNameIndex = 1;
        statusIndex = 3;
      }

      cy.get('cd-service-daemon-list').within(() => {
        this.getTableCell(daemonNameIndex, daemon, true)
          .parent()
          .find(`datatable-body-cell:nth-child(${statusIndex}) .badge`)
          .should(($ele) => {
            const status = $ele.toArray().map((v) => v.innerText);
            expect(status).to.include(expectedStatus);
          });
      });
    });
  }

  expectPlacementCount(serviceName: string, expectedCount: string) {
    this.getTableCell(this.columnIndex.service_name, serviceName)
      .parent()
      .find(`datatable-body-cell:nth-child(${this.columnIndex.placement})`)
      .should(($ele) => {
        const running = $ele.text().split(';');
        expect(running).to.include(`count:${expectedCount}`);
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

  isUnmanaged(serviceName: string, unmanaged: boolean) {
    this.getTableCell(this.columnIndex.service_name, serviceName)
      .parent()
      .find(`datatable-body-cell:nth-child(${this.columnIndex.placement})`)
      .should(($ele) => {
        const placement = $ele.text().split(';');
        unmanaged
          ? expect(placement).to.include('unmanaged')
          : expect(placement).to.not.include('unmanaged');
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

  daemonAction(daemon: string, action: string) {
    cy.get('cd-service-daemon-list').within(() => {
      this.getTableRow(daemon).click();
      this.clickActionButton(action);
    });
  }
}
