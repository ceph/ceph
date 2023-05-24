import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/rgw/roles', id: 'cd-crud-table' },
  create: { url: '#/rgw/roles/create', id: 'cd-crud-form' }
};

export class RolesPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    roleName: 2,
    path: 3,
    arn: 4
  };

  @PageHelper.restrictTo(pages.create.url)
  create(name: string, path: string, policyDocument: string) {
    cy.get('#formly_3_string_role_name_0').type(name);
    cy.get('#formly_3_textarea_role_assume_policy_doc_2').type(policyDocument);
    cy.get('#formly_3_string_role_path_1').type(path);
    cy.get("[aria-label='Create Role']").should('exist').click();
    cy.get('cd-crud-table').should('exist');
  }

  @PageHelper.restrictTo(pages.index.url)
  checkExist(name: string, exist: boolean) {
    this.getTableCell(this.columnIndex.roleName, name).should(($elements) => {
      const roleName = $elements.map((_, el) => el.textContent).get();
      if (exist) {
        expect(roleName).to.include(name);
      } else {
        expect(roleName).to.not.include(name);
      }
    });
  }
}
