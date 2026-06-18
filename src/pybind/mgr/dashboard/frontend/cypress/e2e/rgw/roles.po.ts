import { PageHelper } from '../page-helper.po';

export class RolesPageHelper extends PageHelper {
  pages = {};

  columnIndex = {
    roleName: 1,
    path: 2,
    arn: 3,
    createDate: 4,
    maxSessionDuration: 5
  };

  create(name: string, path: string, policyDocument: string) {
    cy.get('cd-rgw-account-roles-list cd-table-actions button[aria-label="Create"]')
      .should('exist')
      .click();
    cy.get('cds-modal').should('be.visible');
    cy.get('#role_name').type(name);
    cy.get('#role_assume_policy_doc')
      .clear()
      .type(policyDocument, { parseSpecialCharSequences: false, delay: 0 });
    cy.get('#role_path').type(path);
    cy.get('cds-modal').contains('button', 'Create').click();
    cy.get('cds-modal').should('not.exist');
  }

  edit(name: string, maxSessionDuration: number) {
    this.getRolesTableCell(this.columnIndex.roleName, name).click();
    this.getRolesTableCell(this.columnIndex.roleName, name)
      .parent('tr')
      .find('[data-testid="table-action-btn"]')
      .should('exist')
      .click();
    cy.get('cds-overflow-menu-option[aria-label="Edit"]').should('exist').click();
    cy.get('cds-modal').should('be.visible');

    cy.get('cds-number[formControlName="max_session_duration"] input')
      .clear()
      .type(maxSessionDuration.toString());
    cy.get('cds-modal').contains('button', 'Edit').click();
    cy.get('cds-modal').should('not.exist');

    this.getRolesTableCell(this.columnIndex.roleName, name)
      .parent()
      .find(`td:nth-child(${this.columnIndex.maxSessionDuration})`)
      .should(($elements) => {
        const roleName = $elements.map((_, el) => el.textContent).get();
        expect(roleName).to.include(`${maxSessionDuration} hours`);
      });
  }

  deleteRole(name: string) {
    this.getRolesTableCell(this.columnIndex.roleName, name).click();
    this.getRolesTableCell(this.columnIndex.roleName, name)
      .parent('tr')
      .find('[data-testid="table-action-btn"]')
      .should('exist')
      .click();
    cy.get('cds-overflow-menu-option[aria-label="Delete"]').should('exist').click();
    cy.get('cds-modal').should('be.visible');
    cy.get('cds-modal [aria-label="confirmation"]').click({ force: true });
    cy.get('cds-modal').contains('button', 'Delete Role').click();
    cy.get('cds-modal').should('not.exist');
  }

  private getRolesTableCell(columnIndex: number, exactContent: string, partialMatch = false) {
    cy.get('cd-rgw-account-roles-list').within(() => {
      cy.get('.cds--search-close').first().click({ force: true });
      cy.get('.cds--search-input').first().clear({ force: true }).type(exactContent, { delay: 35 });
    });
    const selector = `tbody tr td:nth-child(${columnIndex})`;
    if (partialMatch) {
      return cy.get('cd-rgw-account-roles-list').contains(selector, exactContent);
    }
    return cy
      .get('cd-rgw-account-roles-list')
      .contains(selector, new RegExp(`^\\s*${exactContent}\\s*$`, 'i'));
  }

  checkExist(name: string, exist: boolean) {
    if (exist) {
      this.getRolesTableCell(this.columnIndex.roleName, name).should('exist');
    } else {
      cy.get('cd-rgw-account-roles-list').contains(name).should('not.exist');
    }
  }
}
