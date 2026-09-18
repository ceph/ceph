import { PageHelper } from '../page-helper.po';

export class PoliciesPageHelper extends PageHelper {
  columnIndex = {
    policyName: 1,
    arn: 2,
    defaultVersion: 3,
    createDate: 4
  };

  create(name: string, path: string, policyDocument: string) {
    cy.get('cd-rgw-account-policies-list cd-table-actions button[aria-label="Create"]')
      .should('exist')
      .click();
    cy.get('cds-modal').should('be.visible');
    cy.get('#policy_name').type(name);
    cy.get('#policy_path').clear().type(path);
    cy.get('#policy_doc')
      .clear()
      .type(policyDocument, { parseSpecialCharSequences: false, delay: 0 });
    cy.get('cds-modal').contains('button', 'Create').click();
    cy.get('cds-modal').should('not.exist');
  }

  openPolicy(name: string) {
    this.getPoliciesTableCell(this.columnIndex.policyName, name).click();
    this.getPoliciesTableCell(this.columnIndex.policyName, name)
      .parent('tr')
      .find('[data-testid="table-action-btn"]')
      .should('exist')
      .click();
    cy.get('cds-overflow-menu-option[aria-label="View policy"]').should('exist').click();
    cy.get('cds-modal').should('be.visible');
    cy.get('cds-modal').contains('button', 'Close').click();
    cy.get('cds-modal').should('not.exist');
  }

  deletePolicy(name: string) {
    this.getPoliciesTableCell(this.columnIndex.policyName, name).click();
    this.getPoliciesTableCell(this.columnIndex.policyName, name)
      .parent('tr')
      .find('[data-testid="table-action-btn"]')
      .should('exist')
      .click();
    cy.get('cds-overflow-menu-option[aria-label="Delete"]').should('exist').click();
    cy.get('cds-modal').should('be.visible');
    cy.get('cds-modal [aria-label="confirmation"]').click({ force: true });
    cy.get('cds-modal').contains('button', 'Delete IAM Policy').click();
    cy.get('cds-modal').should('not.exist');
  }

  testVersions(name: string, newPolicyDoc: string) {
    this.getPoliciesTableCell(this.columnIndex.policyName, name).click();
    this.getPoliciesTableCell(this.columnIndex.policyName, name)
      .parent('tr')
      .find('[data-testid="table-action-btn"]')
      .should('exist')
      .click();
    cy.get('cds-overflow-menu-option[aria-label="View policy"]').should('exist').click();
    cy.get('cds-modal').should('be.visible');
    cy.get('cds-modal cds-loading').should('not.exist');

    this.getCdsTab('Versions').click({ force: true });
    cy.get('cds-modal').contains('button', 'Create policy version').click();
    cy.get('#version_policy_doc')
      .clear()
      .type(newPolicyDoc, { parseSpecialCharSequences: false, delay: 0 });
    cy.get('cds-modal').contains('button', 'Save version').click();
    cy.get('cds-modal cds-loading').should('not.exist');

    cy.get('cds-modal table').contains('td', 'v2').should('exist');

    cy.get('cds-modal').contains('button', 'Close').click();
    cy.get('cds-modal').should('not.exist');
  }

  testTags(name: string, key: string, value: string) {
    this.getPoliciesTableCell(this.columnIndex.policyName, name).click();
    this.getPoliciesTableCell(this.columnIndex.policyName, name)
      .parent('tr')
      .find('[data-testid="table-action-btn"]')
      .should('exist')
      .click();
    cy.get('cds-overflow-menu-option[aria-label="View policy"]').should('exist').click();
    cy.get('cds-modal').should('be.visible');
    cy.get('cds-modal cds-loading').should('not.exist');

    this.getCdsTab('Tags').click({ force: true });
    cy.get('input[formcontrolname="tag_key"]').type(key);
    cy.get('input[formcontrolname="tag_value"]').type(value);
    cy.get('cds-modal').contains('button', 'Add tag').click();
    cy.get('cds-modal cds-loading').should('not.exist');

    cy.get('cds-modal table').contains('td', key).should('exist');
    cy.get('cds-modal table').contains('td', value).should('exist');

    cy.get('cds-modal table').contains('button', 'Remove').click();
    cy.get('cds-modal cds-loading').should('not.exist');

    cy.get('cds-modal').contains('button', 'Close').click();
    cy.get('cds-modal').should('not.exist');
  }

  private getPoliciesTableCell(columnIndex: number, exactContent: string) {
    cy.get('cd-rgw-account-policies-list').within(() => {
      cy.get('.cds--search-close').first().click({ force: true });
    });
    cy.get('.cds--search-input').first().clear({ force: true }).type(exactContent, { delay: 35 });
    const selector = `tbody tr td:nth-child(${columnIndex})`;
    return cy
      .get('cd-rgw-account-policies-list')
      .contains(selector, new RegExp(`^\\s*${exactContent}\\s*$`, 'i'));
  }

  checkExist(name: string, exist: boolean) {
    if (exist) {
      this.getPoliciesTableCell(this.columnIndex.policyName, name).should('exist');
    } else {
      cy.get('cd-rgw-account-policies-list').contains(name).should('not.exist');
    }
  }
}
