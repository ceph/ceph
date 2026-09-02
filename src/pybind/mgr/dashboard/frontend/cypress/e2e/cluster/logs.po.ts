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

    // The filter toolbar is re-created on every tab switch. Wait until the
    // new pane's keyword filter is interactable before driving the filter
    // inputs, otherwise the clear/type intermittently hits the input while
    // it is still initializing and fails on a disabled element.
    cy.get('.tab-pane.active #logs-keyword').should('be.visible').and('be.enabled');

    // Enter an earliest time so that no old messages with the same pool name show up
    this.setTimepickerValue(0, hour);
    this.setTimepickerValue(1, minute);

    // Enter the pool name into the filter box
    cy.get('.tab-pane.active #logs-keyword').clear();
    cy.get('.tab-pane.active #logs-keyword').type(poolname);
    cy.get('.tab-pane.active #logs-keyword').should('have.value', poolname);

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

    // See checkAuditForPoolFunction: wait for the re-created filter toolbar.
    cy.get('.tab-pane.active #logs-keyword').should('be.visible').and('be.enabled');

    // Enter an earliest time so that no old messages with the same config name show up
    this.setTimepickerValue(0, hour);
    this.setTimepickerValue(1, minute);

    // Enter the config name into the filter box
    cy.get('.tab-pane.active #logs-keyword').clear();
    cy.get('.tab-pane.active #logs-keyword').type(configname);
    cy.get('.tab-pane.active #logs-keyword').should('have.value', configname);

    cy.get('.tab-pane.active')
      .get('.log-viewer')
      .get('.log-entry__message')
      .should('contain.text', configname)
      .and('contain.text', setting);
  }
}
