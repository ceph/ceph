import { PageHelper } from '../page-helper.po';

export class RolesPageHelper extends PageHelper {
  pages = {};

  // Carbon cd-table: col 1 is the selection checkbox; data columns start at 2.
  columnIndex = {
    roleName: 2,
    policiesCount: 3,
    maxSessionDuration: 4,
    createDate: 5
  };

  create(name: string, path: string, policyDocument: string) {
    cy.intercept('POST', '**/api/rgw/accounts/*/roles').as('createRole');

    cy.get('cd-rgw-account-roles-list cd-table-actions button[aria-label="Create"]')
      .should('exist')
      .click();
    cy.get('cd-tearsheet').should('be.visible');
    cy.get('cd-tearsheet form').should('exist');

    cy.get('#role_name')
      .should('be.visible')
      .clear()
      .type(name, { delay: 0 })
      .blur()
      .should('have.value', name)
      // Unique-name async validator (DUE_TIMER + GET exists).
      .should('not.have.class', 'ng-pending')
      .should('have.class', 'ng-valid');

    cy.get('#role_path')
      .should('be.visible')
      .clear()
      .type(path, { delay: 0 })
      .blur()
      .should('have.value', path)
      .should('have.class', 'ng-valid');

    // Valid IAM trust policy JSON — empty "{}" is rejected by RGW.
    cy.get('#role_assume_policy_doc')
      .should('be.visible')
      .clear({ force: true })
      .type(policyDocument, { parseSpecialCharSequences: false, delay: 0 })
      .blur()
      .should(($el) => {
        expect(($el.val() as string).replace(/\s/g, '')).to.eq(policyDocument.replace(/\s/g, ''));
      })
      .should('have.class', 'ng-valid');

    cy.get('cd-tearsheet form').should('have.class', 'ng-valid');

    cy.get('cd-tearsheet .tearsheet-footer-submit')
      .should('be.visible')
      .and('not.be.disabled')
      .click();
    cy.wait('@createRole', { timeout: 120000 })
      .its('response.statusCode')
      .should('be.oneOf', [200, 201]);
    cy.get('cd-tearsheet').should('not.exist');
  }

  edit(name: string, maxSessionDuration: number) {
    cy.intercept('PUT', '**/api/rgw/accounts/*/roles').as('updateRole');

    this.openRowAction(name, 'edit-role');
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
    cy.wait('@updateRole', { timeout: 120000 })
      .its('response.statusCode')
      .should('be.oneOf', [200, 201]);
    cy.get('cd-tearsheet').should('not.exist');

    this.getRolesTableCell(this.columnIndex.roleName, name)
      .parent()
      .find(`td:nth-child(${this.columnIndex.maxSessionDuration})`)
      .should(($elements) => {
        const texts = $elements.map((_, el) => el.textContent).get();
        expect(texts.join(' ')).to.match(new RegExp(`${maxSessionDuration}\\s*hours?`, 'i'));
      });
  }

  deleteRole(name: string) {
    this.openRowAction(name, 'delete');
    cy.get('cds-modal').should('be.visible');
    cy.get('cds-modal [aria-label="confirmation"]').click({ force: true });
    cy.get('cds-modal')
      .contains('button', /Delete Role|Delete/)
      .click();
    cy.get('cds-modal').should('not.exist');
  }

  /**
   * Filter + select the role row, then run a row overflow action
   * (class from tableActions.toClassName — e.g. edit-role, delete).
   *
   * Roles use selectionType="single" (no batch bar). Edit/Delete require a
   * selection (disable: !hasSelection) and live in the row overflow menu.
   * Match page-helper clickRowActionButton — do not filter :visible; Carbon
   * keeps closed-menu options in the DOM and :visible never matches in CI.
   */
  private openRowAction(name: string, actionClass: string) {
    cy.get('cd-rgw-account-roles-list').within(() => {
      cy.get('.cds--search-close').first().click({ force: true });
      cy.get('.cds--search-input').first().clear({ force: true }).type(name, { delay: 35 });
    });
    cy.wait(500);
    // Select the row so Edit/Delete are enabled, then open the row kebab.
    cy.contains('cd-rgw-account-roles-list [cdstablerow] [cdstabledata]', name)
      .parent('[cdstablerow]')
      .as('roleRow')
      .click({ force: true });

    cy.get('@roleRow')
      .find('[cdstabledata] [data-testid="table-action-btn"]')
      .click({ force: true });
    cy.wait(500);
    // Options are portaled or rendered in Carbon cds-overflow-menu-option.
    cy.get(
      `cds-overflow-menu-option.${actionClass}, cds-overflow-menu-option.${actionClass} button, button.${actionClass}, .${actionClass}`
    )
      .first()
      .should('not.be.disabled')
      .click({ force: true });
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
