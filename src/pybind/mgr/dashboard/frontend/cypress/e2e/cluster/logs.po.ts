import { PageHelper } from '../page-helper.po';

export class LogsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/logs', id: 'cd-logs' }
  };

  private setTimepickerValue(index: number, value: number) {
    cy.get('.ngb-tp-input')
      .should('have.length.gte', index + 1)
      .eq(index)
      .then(($input) => {
        const input = $input[0] as HTMLInputElement;
        input.value = String(value).padStart(2, '0');
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
      });
  }

  private typeInKeywordFilter(text: string) {
    // The logs component polls every 5s, which can detach DOM elements mid-interaction.
    // Set value directly via DOM to avoid detached element errors during clear/type.
    cy.get('#logs-keyword')
      .should('be.visible')
      .then(($input) => {
        const input = $input[0] as HTMLInputElement;
        input.value = text;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('keyup', { bubbles: true }));
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
    this.typeInKeywordFilter(poolname);

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
    this.typeInKeywordFilter(configname);

    cy.get('.tab-pane.active')
      .get('.log-viewer')
      .get('.log-entry__message')
      .should('contain.text', configname)
      .and('contain.text', setting);
  }
}
