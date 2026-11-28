import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/hosts', id: 'cd-hosts' },
  add: { url: '#/hosts/(modal:add)', id: 'cd-host-form' }
};

export class HostsPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    hostname: 1,
    labels: 2,
    status: 3
  };

  clickHostTab(hostname: string, tabName: 'Service instances' | 'Storage Devices') {
    this.navigateTo();
    this.getResourcePage(hostname).click();
    cy.contains('cd-host-sidebar cds-sidenav a.cds--side-nav__link', tabName).click();
    cy.get('cd-host-resource-page').should('exist');
  }

  check_for_host() {
    this.getTableCount('total').should('not.be.eq', 0);
  }

  add(hostname: string, exist?: boolean, maintenance?: boolean, labels: string[] = []) {
    cy.get(`${this.pages.add.id}`).within(() => {
      cy.get('#hostname').type(hostname, { force: true });
      if (maintenance) {
        cy.get('label[for=maintenance]').click();
      }
      if (exist) {
        cy.get('#hostname').should('have.class', 'ng-invalid');
      }
    });

    if (labels.length) {
      this.selectLabels(labels);
    }

    cy.get('cd-submit-button').click();
    cy.get('cds-modal').should('not.exist');
    // back to host list
    cy.get(`${this.pages.index.id}`);
  }

  selectLabels(labels: string[], add = true) {
    for (const label of labels) {
      cy.get('cds-modal input#labels').clear().type(label);
      cy.get('cds-dropdown-list li')
        .should('have.length', 1)
        .and('have.attr', 'title', label)
        .and('have.attr', 'aria-selected', String(!add));
      cy.get('cds-modal input#labels').type('{downarrow}');
      cy.get('cds-dropdown-list [role="listbox"]').should('be.focused').type('{enter}');
    }
    cy.get('cds-modal cds-modal-header h3').click();
  }

  checkExist(hostname: string, exist: boolean, shouldReload = false) {
    if (shouldReload) {
      cy.reload(true, { log: true, timeout: 5 * 1000 });
    }
    this.getTableCell(this.columnIndex.hostname, hostname, true)
      .parent()
      .find(`[cdstabledata]:nth-child(${this.columnIndex.hostname}) span`)
      .should(($elements) => {
        const hosts = $elements.toArray().map((v) => v.innerText);
        if (exist) {
          expect(hosts).to.include(hostname);
        } else {
          expect(hosts).to.not.include(hostname);
        }
      });
  }

  remove(hostname: string) {
    super.delete(hostname, this.columnIndex.hostname, 'hosts', true, false, true, true);
  }

  // Add or remove labels on a host, then verify labels in the table
  editLabels(hostname: string, labels: string[], add: boolean) {
    this.clickRowActionButton(hostname, 'edit');
    this.selectLabels(labels, add);
    cy.get('cds-modal button[aria-label="Save changes"]').click();
    cy.get('cds-modal').should('not.exist');
    this.checkLabelExists(hostname, labels, add);
  }

  checkLabelExists(hostname: string, labels: string[], add: boolean) {
    // Verify labels are added or removed from Labels column
    // First find row with hostname, then find labels in the row
    this.getTableCell(this.columnIndex.hostname, hostname, true)
      .parent()
      .find(`[cdstabledata]:nth-child(${this.columnIndex.labels}) cds-tag span`)
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

  @PageHelper.restrictTo(pages.index.url)
  maintenance(hostname: string, exit = false, force = false) {
    this.clearTableSearchInput();
    if (force) {
      this.clickRowActionButton(hostname, 'enter-maintenance');

      cy.get('cds-modal').within(() => {
        cy.contains('button', 'Continue').click();
      });

      this.getTableCell(this.columnIndex.hostname, hostname, true)
        .parent()
        .find(`[cdstabledata]:nth-child(${this.columnIndex.status}) cds-tag`)
        .should(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          expect(status).to.include('maintenance');
        });
    }
    if (exit) {
      this.getTableCell(this.columnIndex.hostname, hostname, true)
        .parent()
        .find(`[cdstabledata]:nth-child(${this.columnIndex.status})`)
        .then(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          if (status[0].includes('maintenance')) {
            this.clickRowActionButton(hostname, 'exit-maintenance');
          }
        });

      this.getTableCell(this.columnIndex.hostname, hostname, true)
        .parent()
        .find(`[cdstabledata]:nth-child(${this.columnIndex.status})`)
        .should(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          expect(status).to.not.include('maintenance');
        });
    } else {
      this.clickRowActionButton(hostname, 'enter-maintenance');

      this.getTableCell(this.columnIndex.hostname, hostname, true)
        .parent()
        .find(`[cdstabledata]:nth-child(${this.columnIndex.status}) cds-tag`)
        .should(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          expect(status).to.include('maintenance');
        });
    }
  }

  @PageHelper.restrictTo(pages.index.url)
  drain(hostname: string) {
    this.clickRowActionButton(hostname, 'start-drain');
    cy.wait(1000);
    this.checkLabelExists(hostname, ['_no_schedule'], true);

    this.clickHostTab(hostname, 'Service instances');
    cy.get('cd-host-resource-page cd-service-daemon-list').within(() => {
      cy.wait(20000);
      this.expectTableCount('total', 0);
    });
    this.navigateTo();
  }
}
