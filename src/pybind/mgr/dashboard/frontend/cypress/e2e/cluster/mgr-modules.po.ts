import { PageHelper } from '../page-helper.po';

export class Input {
  id: string;
  oldValue: string;
  newValue: string;
}

export class ManagerModulesPageHelper extends PageHelper {
  pages = { index: { url: '#/mgr-modules', id: 'cd-mgr-module-list' } };

  getOverviewField(label: string) {
    return cy
      .get('.cd-overview-label')
      .filter((_index, el) => el.textContent?.includes(label))
      .closest('.cd-overview-item');
  }

  private formatOverviewLabel(key: string): string {
    const normalizedKey = key.split('_').filter(Boolean).join(' ').toLowerCase();

    if (!normalizedKey) {
      return '';
    }

    return normalizedKey.charAt(0).toUpperCase() + normalizedKey.slice(1);
  }

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
    this.getResourcePage(name).should('be.visible').click();
    for (const input of inputs) {
      this.getOverviewField(this.formatOverviewLabel(input.id)).should(
        'contain.text',
        input.newValue
      );
    }

    // Goes back to list page from resource page
    this.navigateBack();

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
    this.getResourcePage(name).should('be.visible').click();
    for (const input of inputs) {
      if (input.oldValue) {
        this.getOverviewField(this.formatOverviewLabel(input.id)).should(
          'not.contain.text',
          input.newValue
        );
      }
    }
  }
}
