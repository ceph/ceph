import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/multi-cluster/overview', id: 'cd-multi-cluster' },
  'manage-clusters': { url: '#/multi-cluster/manage-clusters', id: 'cd-multi-cluster-list' },
  connect: { url: '#/multi-cluster/manage-clusters/connect', id: 'cd-multi-cluster-form' }
};

const WAIT_TIMER = 1000;

export class MultiClusterPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    alias: 2,
    connection: 3
  };

  auth(url: string, alias: string, username: string, password: string) {
    cy.contains('button', 'Connect').click();
    cy.get('cd-multi-cluster-form').should('exist');
    this.navigateTo('connect');
    cy.get('input[name=remoteClusterUrl]').type(url);
    cy.get('input[name=clusterAlias]').type(alias);
    cy.get('input[name=username]').type(username);
    cy.get('#password').type(password);
    cy.get('[data-testid=submitBtn]').click();
    cy.wait(WAIT_TIMER);
  }

  disconnect(alias: string) {
    this.searchTable(alias);
    cy.wait(3000);
    cy.get(`[cdstablerow] [cdstabledata]:nth-child(${this.columnIndex.alias})`)
      .filter((_i, el) => el.innerText.trim() === alias)
      .parent('[cdstablerow]')
      .find('[cdstabledata] [data-testid="table-action-btn"]')
      .click({ force: true });
    cy.get(`button.disconnect`).click({ force: true });
    cy.get('cds-modal').within(() => {
      cy.get('#confirmation_input').click({ force: true });
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }

  reconnect(alias: string, password: string) {
    this.searchTable(alias);
    cy.wait(3000);
    cy.get(`[cdstablerow] [cdstabledata]:nth-child(${this.columnIndex.alias})`)
      .filter((_i, el) => el.innerText.trim() === alias)
      .parent('[cdstablerow]')
      .find('[cdstabledata] [data-testid="table-action-btn"]')
      .click({ force: true });
    cy.get(`button.reconnect`).click({ force: true });
    cy.get('#password').type(password);
    cy.get('[data-testid=submitBtn]').click();
    cy.wait(WAIT_TIMER);
  }

  edit(alias: string, newAlias: string, password: string) {
    this.searchTable(alias);
    cy.wait(3000);
    cy.get(`[cdstablerow] [cdstabledata]:nth-child(${this.columnIndex.alias})`)
      .filter((_i, el) => el.innerText.trim() === alias)
      .parent('[cdstablerow]')
      .find('[cdstabledata] [data-testid="table-action-btn"]')
      .click({ force: true });
    cy.get(`button.edit`).click({ force: true });
    cy.get('input[name=clusterAlias]').clear().type(newAlias);
    cy.get('#password').type(password);
    cy.get('[data-testid=submitBtn]').click();
    cy.wait(WAIT_TIMER);
  }

  checkConnectionStatus(alias: string, expectedStatus = 'CONNECTED', shouldReload = true) {
    const aliasIndex = this.columnIndex.alias;
    const statusIndex = this.columnIndex.connection;
    if (shouldReload) {
      cy.reload(true, { log: true, timeout: 5 * 1000 });
    }
    this.searchTable(alias);
    cy.wait(3000);
    cy.get(`[cdstablerow] [cdstabledata]:nth-child(${aliasIndex})`)
      .filter((_i, el) => el.innerText.trim() === alias)
      .parent()
      .find(`[cdstabledata]:nth-child(${statusIndex}) cds-tag`)
      .should(($ele) => {
        const status = $ele.toArray().map((v) => v.innerText.trim());
        expect(status).to.include(expectedStatus);
      });
  }
}
