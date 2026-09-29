import { PageHelper } from '../page-helper.po';

export class RolesPageHelper extends PageHelper {
  pages = {};

  columnIndex = {
    roleName: 1,
    policiesCount: 2,
    maxSessionDuration: 3,
    createDate: 4
  };

  create(name: string, path: string, policyDocument: string) {
    cy.intercept('GET', '**/api/rgw/accounts/*/roles/*').as('roleExists');
    cy.intercept('POST', '**/api/rgw/accounts/*/roles').as('createRole');

    cy.get('cd-rgw-account-roles-list cd-table-actions button[aria-label="Create"]')
      .should('exist')
      .click();
    cy.get('cd-tearsheet').should('be.visible');

    cy.get('#role_name').should('be.visible').clear().type(name);
    // Unique-name async validator waits DUE_TIMER (500ms) before GET exists.
    cy.wait('@roleExists');

    cy.get('#role_path').should('be.visible').clear().type(path);
    cy.get('#role_assume_policy_doc')
      .should('be.visible')
      .clear()
      .type(policyDocument, { parseSpecialCharSequences: false, delay: 0 });

    cy.get('cd-tearsheet .tearsheet-footer-submit')
      .should('be.visible')
      .and('not.be.disabled')
      .click();
    cy.wait('@createRole').its('response.statusCode').should('be.oneOf', [200, 201]);
    cy.get('cd-tearsheet').should('not.exist');
  }

  edit(name: string, maxSessionDuration: number) {
    cy.intercept('PUT', '**/api/rgw/accounts/*/roles').as('updateRole');

    this.getRolesTableCell(this.columnIndex.roleName, name).click();
    this.getRolesTableCell(this.columnIndex.roleName, name)
      .parent('tr')
      .find('[data-testid="table-action-btn"]')
      .should('exist')
      .click();
    cy.get('cds-overflow-menu-option[aria-label="Edit"]').should('exist').click();
    cy.get('cd-tearsheet').should('be.visible');

    // cds-number clear()+type() is flaky and can leave a leftover digit (e.g. 1 + 3 -> 13).
    cy.get('cds-number[formControlName="max_session_duration"] input')
      .should('be.visible')
      .click()
      .type('{selectall}{backspace}')
      .type(String(maxSessionDuration))
      .should('have.value', String(maxSessionDuration));
    cy.get('cd-tearsheet .tearsheet-footer-submit')
      .should('be.visible')
      .and('not.be.disabled')
      .click();
    cy.wait('@updateRole').its('response.statusCode').should('be.oneOf', [200, 201]);
    cy.get('cd-tearsheet').should('not.exist');

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
