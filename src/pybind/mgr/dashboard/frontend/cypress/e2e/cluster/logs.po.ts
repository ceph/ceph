import { PageHelper } from '../page-helper.po';

export class LogsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/logs', id: 'cd-logs' }
  };

  checkAuditForPoolFunction(poolname: string, poolfunction: string, hour: number, minute: number) {
    this.navigateTo();

    // sometimes the modal from deleting pool is still present at this point.
    // This wait makes sure it isn't
    cy.contains('.modal-dialog', 'Delete Pool').should('not.exist');

    // go to audit logs tab
    cy.contains('.nav-link', 'Audit Logs').click();

    // Enter an earliest time so that no old messages with the same pool name show up
    cy.get('.ngb-tp-input').its(0).clear();

    if (hour < 10) {
      cy.get('.ngb-tp-input').its(0).type('0');
    }
    cy.get('.ngb-tp-input').its(0).type(`${hour}`);

    cy.get('.ngb-tp-input').its(1).clear();
    if (minute < 10) {
      cy.get('.ngb-tp-input').its(1).type('0');
    }
    cy.get('.ngb-tp-input').its(1).type(`${minute}`);

    // Enter the pool name into the filter box
    cy.get('input.form-control.ng-valid').first().clear().type(poolname);

    cy.get('.tab-pane.active')
      .get('.card-body')
      .get('.message')
      .should('contain.text', poolname)
      .and('contain.text', `pool ${poolfunction}`);
  }

  checkAuditForConfigChange(configname: string, setting: string, hour: number, minute: number) {
    this.navigateTo();

    // go to audit logs tab
    cy.contains('.nav-link', 'Audit Logs').click();

    // Enter an earliest time so that no old messages with the same config name show up
    cy.get('.ngb-tp-input').its(0).clear();
    if (hour < 10) {
      cy.get('.ngb-tp-input').its(0).type('0');
    }
    cy.get('.ngb-tp-input').its(0).type(`${hour}`);

    cy.get('.ngb-tp-input').its(1).clear();
    if (minute < 10) {
      cy.get('.ngb-tp-input').its(1).type('0');
    }
    cy.get('.ngb-tp-input').its(1).type(`${minute}`);

    // Enter the config name into the filter box
    cy.get('input.form-control.ng-valid').first().clear().type(configname);

    cy.get('.tab-pane.active')
      .get('.card-body')
      .get('.message')
      .should('contain.text', configname)
      .and('contain.text', setting);
  }
}
