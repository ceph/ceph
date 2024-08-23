import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/multi-cluster/overview', id: 'cd-multi-cluster' },
  'manage-clusters': { url: '#/multi-cluster/manage-clusters', id: 'cd-multi-cluster-list' }
};

const WAIT_TIMER = 1000;

export class MultiClusterPageHelper extends PageHelper {
  pages = pages;

  auth(url: string, alias: string, username: string, password: string) {
    cy.contains('button', 'Connect').click();
    cy.get('cd-multi-cluster-form').should('exist');
    cy.get('cd-modal').within(() => {
      cy.get('input[name=remoteClusterUrl]').type(url);
      cy.get('input[name=clusterAlias]').type(alias);
      cy.get('input[name=username]').type(username);
      cy.get('input[name=password]').type(password);
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }

  disconnect(alias: string) {
    this.clickRowActionButton(alias, 'disconnect');
    cy.get('cds-modal').within(() => {
      cy.get('#confirmation_input').click({ force: true });
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }

  reconnect(alias: string, password: string) {
    this.clickRowActionButton(alias, 'reconnect');
    cy.get('cd-modal').within(() => {
      cy.get('input[name=password]').type(password);
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }

  edit(alias: string, newAlias: string) {
    this.clickRowActionButton(alias, 'edit');
    cy.get('cd-modal').within(() => {
      cy.get('input[name=clusterAlias]').clear().type(newAlias);
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }
}
