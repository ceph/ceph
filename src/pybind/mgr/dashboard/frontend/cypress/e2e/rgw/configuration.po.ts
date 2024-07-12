import { PageHelper } from '../page-helper.po';

export class ConfigurationPageHelper extends PageHelper {
  pages = {
    index: { url: '#/rgw/configuration', id: 'cd-rgw-configuration-page' }
  };

  columnIndex = {
    address: 4
  };

  create(provider: string, auth_method: string, secret_engine: string, address: string) {
    cy.contains('button', 'Create').click();
    this.selectKmsProvider(provider);
    cy.get('#kms_provider').should('have.class', 'ng-valid');
    this.selectAuthMethod(auth_method);
    cy.get('#auth_method').should('have.class', 'ng-valid');
    this.selectSecretEngine(secret_engine);
    cy.get('#secret_engine').should('have.class', 'ng-valid');
    cy.get('#address').type(address);
    cy.get('#address').should('have.class', 'ng-valid');
    cy.contains('button', 'Submit').click();
    this.getFirstTableCell('SSE_KMS').should('exist');
  }

  edit(new_address: string) {
    this.navigateEdit('SSE_KMS', true, false);
    cy.get('#address').clear().type(new_address);
    cy.get('#address').should('have.class', 'ng-valid');
    cy.get('#kms_provider').should('be.disabled');
    cy.contains('button', 'Submit').click();
    this.getTableCell(this.columnIndex.address, new_address)
      .parent()
      .find(`datatable-body-cell:nth-child(${this.columnIndex.address})`)
      .should(($elements) => {
        const address = $elements.text();
        expect(address).to.eq(new_address);
      });
  }

  private selectKmsProvider(provider: string) {
    return this.selectOption('kms_provider', provider);
  }

  private selectAuthMethod(auth_method: string) {
    return this.selectOption('auth_method', auth_method);
  }

  private selectSecretEngine(secret_engine: string) {
    return this.selectOption('secret_engine', secret_engine);
  }
}
