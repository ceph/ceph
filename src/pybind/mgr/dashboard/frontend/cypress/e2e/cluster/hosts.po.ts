import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/hosts', id: 'cd-hosts' },
  add: { url: '#/hosts/(modal:add)', id: 'cd-host-form' }
};

export class HostsPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    hostname: 2,
    services: 3,
    labels: 4,
    status: 5
  };

  check_for_host() {
    this.getTableCount('total').should('not.be.eq', 0);
  }

  add(hostname: string, exist?: boolean, maintenance?: boolean, labels: string[] = []) {
    cy.get(`${this.pages.add.id}`).within(() => {
      cy.get('#hostname').type(hostname);
      if (maintenance) {
        cy.get('label[for=maintenance]').click();
      }
      if (exist) {
        cy.get('#hostname').should('have.class', 'ng-invalid');
      }
    });

    if (labels.length) {
      this.selectPredefinedLabels(labels);
    }

    cy.get('cd-submit-button').click();
    // back to host list
    cy.get(`${this.pages.index.id}`);
  }

  selectPredefinedLabels(labels: string[]) {
    cy.get('a[data-testid=select-menu-edit]').click();
    for (const label of labels) {
      cy.get('.popover-body div.select-menu-item-content').contains(label).click();
    }
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
    super.delete(hostname, this.columnIndex.hostname, 'hosts', true, false, true);
  }

  // Add or remove labels on a host, then verify labels in the table
  editLabels(hostname: string, labels: string[], add: boolean) {
    this.clickRowActionButton(hostname, 'edit');

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
    this.getTableCell(this.columnIndex.hostname, hostname, true).as('row').click();
    cy.get('@row')
      .parent()
      .find(`[cdstabledata]:nth-child(${this.columnIndex.labels}) .badge`)
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
        .find(`[cdstabledata]:nth-child(${this.columnIndex.status}) .badge`)
        .should(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          expect(status).to.include('maintenance');
        });
    }
    if (exit) {
      this.getTableCell(this.columnIndex.hostname, hostname, true)
        .click()
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
        .find(`[cdstabledata]:nth-child(${this.columnIndex.status}) .badge`)
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

    this.clickTab('cd-host-details', hostname, 'Daemons');
    cy.get('cd-host-details').within(() => {
      cy.wait(20000);
      this.expectTableCount('total', 0);
    });
  }

  checkServiceInstancesExist(hostname: string, instances: string[]) {
    this.getTableCell(this.columnIndex.hostname, hostname, true)
      .parent()
      .find(`[cdstabledata]:nth-child(${this.columnIndex.services}) .badge`)
      .should(($ele) => {
        const serviceInstances = $ele.toArray().map((v) => v.innerText);
        for (const instance of instances) {
          expect(serviceInstances).to.include(instance);
        }
      });
  }
}
