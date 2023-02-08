import { PageHelper } from '../page-helper.po';

export class Input {
  id: string;
  oldValue: string;
  newValue: string;
}

export class ManagerModulesPageHelper extends PageHelper {
  pages = { index: { url: '#/mgr-modules', id: 'cd-mgr-module-list' } };

  /**
   * Selects the Manager Module and then fills in the desired fields.
   */
  editMgrModule(name: string, inputs: Input[]) {
    this.navigateEdit(name);

    for (const input of inputs) {
      // Clears fields and adds edits
      cy.get(`#${input.id}`).clear().type(input.newValue);
    }

    cy.contains('button', 'Update').click();
    // Checks if edits appear
    this.getExpandCollapseElement(name).should('be.visible').click();

    for (const input of inputs) {
      cy.get('.datatable-body').last().contains(input.newValue);
    }

    // Clear mgr module of all edits made to it
    this.navigateEdit(name);

    // Clears the editable fields
    for (const input of inputs) {
      if (input.oldValue) {
        const id = `#${input.id}`;
        cy.get(id).clear();
        if (input.oldValue) {
          cy.get(id).type(input.oldValue);
        }
      }
    }

    // Checks that clearing represents in details tab of module
    cy.contains('button', 'Update').click();
    this.getExpandCollapseElement(name).should('be.visible').click();
    for (const input of inputs) {
      if (input.oldValue) {
        cy.get('.datatable-body')
          .eq(1)
          .should('contain', input.id)
          .and('not.contain', input.newValue);
      }
    }
  }
}
