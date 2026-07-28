import { PageHelper } from '../page-helper.po';

export class LogsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/logs', id: 'cd-logs' }
  };

  private setTimepickerValue(index: number, value: number) {
    cy.get('.ngb-tp-input')
      .eq(index)
      .then(($input) => {
        const input = $input[0] as HTMLInputElement;
        input.value = String(value).padStart(2, '0');
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
      });
  }

  checkAuditForPoolFunction(poolname: string, poolfunction: string, hour: number, minute: number) {
    this.navigateTo();

    // sometimes the modal from deleting pool is still present at this point.
    // This wait makes sure it isn't
    cy.contains('.modal-dialog', 'Delete Pool').should('not.exist');

    // go to audit logs tab
    cy.contains('.nav-link', 'Audit Logs').click();

    // Enter an earliest time so that no old messages with the same pool name show up
    this.setTimepickerValue(0, hour);
    this.setTimepickerValue(1, minute);

    // Enter the pool name into the filter box
    cy.get('#logs-keyword').clear();
    cy.get('#logs-keyword').type(poolname);

    cy.get('.tab-pane.active')
      .get('.log-viewer')
      .get('.log-entry__message')
      .should('contain.text', poolname)
      .and('contain.text', `pool ${poolfunction}`);
  }

  checkAuditForConfigChange(configname: string, setting: string, hour: number, minute: number) {
    this.navigateTo();

    // go to audit logs tab
    cy.contains('.nav-link', 'Audit Logs').click();

    // Enter an earliest time so that no old messages with the same config name show up
    this.setTimepickerValue(0, hour);
    this.setTimepickerValue(1, minute);

    // Enter the config name into the filter box
    cy.get('#logs-keyword').clear();
    cy.get('#logs-keyword').type(configname);

    cy.get('.tab-pane.active')
      .get('.log-viewer')
      .get('.log-entry__message')
      .should('contain.text', configname)
      .and('contain.text', setting);
  }
}
