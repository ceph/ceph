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

  // function that checks all services links work for first
  // host in table
  check_services_links() {
    // check that text (links) is present in services box
    let links_tested = 0;

    cy.get('cd-hosts a.service-link')
      .should('have.length.greaterThan', 0)
      .then(($elems) => {
        $elems.each((_i, $el) => {
          // click link, check it worked by looking for changed breadcrumb,
          // navigate back to hosts page, repeat until all links checked
          cy.contains('a', $el.innerText).should('exist').click();
          this.expectBreadcrumbText('Performance Counters');
          this.navigateTo();
          links_tested++;
        });
        // check if any links were actually tested
        expect(links_tested).gt(0);
      });
  }

  @PageHelper.restrictTo(pages.index.url)
  clickHostTab(hostname: string, tabName: string) {
    this.getExpandCollapseElement(hostname).click();
    cy.get('cd-host-details').within(() => {
      this.getTab(tabName).click();
    });
  }

  @PageHelper.restrictTo(pages.add.url)
  add(hostname: string, exist?: boolean, maintenance?: boolean) {
    cy.get(`${this.pages.add.id}`).within(() => {
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

  @PageHelper.restrictTo(pages.index.url)
  checkExist(hostname: string, exist: boolean) {
    this.getTableCell(this.columnIndex.hostname, hostname).should(($elements) => {
      const hosts = $elements.map((_, el) => el.textContent).get();
      if (exist) {
        expect(hosts).to.include(hostname);
      } else {
        expect(hosts).to.not.include(hostname);
      }
    });
  }

  @PageHelper.restrictTo(pages.index.url)
  delete(hostname: string) {
    super.delete(hostname, this.columnIndex.hostname);
  }

  // Add or remove labels on a host, then verify labels in the table
  @PageHelper.restrictTo(pages.index.url)
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

  @PageHelper.restrictTo(pages.index.url)
  maintenance(hostname: string, exit = false, force = false) {
    if (force) {
      this.getTableCell(this.columnIndex.hostname, hostname).click();
      this.clickActionButton('enter-maintenance');

      cy.contains('cd-modal button', 'Continue').click();

      this.getTableCell(this.columnIndex.hostname, hostname)
        .parent()
        .find(`datatable-body-cell:nth-child(${this.columnIndex.status}) .badge`)
        .should(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          expect(status).to.include('maintenance');
        });
    }
    if (exit) {
      this.getTableCell(this.columnIndex.hostname, hostname)
        .click()
        .parent()
        .find(`datatable-body-cell:nth-child(${this.columnIndex.status})`)
        .then(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          if (status[0].includes('maintenance')) {
            this.clickActionButton('exit-maintenance');
          }
        });

      this.getTableCell(this.columnIndex.hostname, hostname)
        .parent()
        .find(`datatable-body-cell:nth-child(${this.columnIndex.status})`)
        .should(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          expect(status).to.not.include('maintenance');
        });
    } else {
      this.getTableCell(this.columnIndex.hostname, hostname).click();
      this.clickActionButton('enter-maintenance');

      this.getTableCell(this.columnIndex.hostname, hostname)
        .parent()
        .find(`datatable-body-cell:nth-child(${this.columnIndex.status}) .badge`)
        .should(($ele) => {
          const status = $ele.toArray().map((v) => v.innerText);
          expect(status).to.include('maintenance');
        });
    }
  }
}
