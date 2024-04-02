import { PageHelper } from '../page-helper.po';
import { BucketsPageHelper } from './buckets.po';

const buckets = new BucketsPageHelper();

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
    cy.get('#auth').should('have.class', 'ng-valid');
    this.selectSecretEngine(secret_engine);
    cy.get('#secret_engine').should('have.class', 'ng-valid');
    cy.get('#addr').type(address);
    cy.get('#addr').should('have.value', address);
    cy.get('#addr').should('have.class', 'ng-valid');
    cy.contains('button', 'Submit').click();
    cy.wait(500);
    cy.get('cd-table').should('exist');
    this.getFirstTableCell('kms').should('exist');
  }

  edit(new_address: string) {
    this.navigateEdit('kms', true, false);
    cy.get('#addr').clear().type(new_address);
    cy.get('#addr').should('have.class', 'ng-valid');
    cy.get('#kms_provider').should('be.disabled');
    cy.contains('button', 'Submit').click();
    this.getFirstTableCell(new_address);
  }

  private selectKmsProvider(provider: string) {
    return this.selectOption('kms_provider', provider);
  }

  private selectAuthMethod(auth_method: string) {
    return this.selectOption('auth', auth_method);
  }

  private selectSecretEngine(secret_engine: string) {
    return this.selectOption('secret_engine', secret_engine);
  }

  checkBucketEncryption() {
    buckets.navigateTo('create');
    cy.get('input[name=encryption_enabled]').should('be.enabled');
  }
}
