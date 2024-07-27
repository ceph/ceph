import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/multi-cluster/overview', id: 'cd-multi-cluster' },
  'manage-clusters': { url: '#/multi-cluster/manage-clusters', id: 'cd-multi-cluster-list' }
};

const WAIT_TIMER = 1000;

export class MultiClusterPageHelper extends PageHelper {
  pages = pages;

  auth(url: string, alias: string, username: string, password: string) {
    this.clickActionButton('connect');
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
    this.getFirstTableCell(alias).click();
    this.clickActionButton('disconnect');
    cy.get('cd-modal').within(() => {
      cy.get('#confirmation').click();
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }

  reconnect(alias: string, password: string) {
    this.getFirstTableCell(alias).click();
    this.clickActionButton('reconnect');
    cy.get('cd-modal').within(() => {
      cy.get('input[name=password]').type(password);
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }

  edit(alias: string, newAlias: string) {
    this.getFirstTableCell(alias).click();
    this.clickActionButton('edit');
    cy.get('cd-modal').within(() => {
      cy.get('input[name=clusterAlias]').clear().type(newAlias);
      cy.get('cd-submit-button').click();
    });
    cy.wait(WAIT_TIMER);
  }
}
