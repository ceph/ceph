import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/rgw/accounts', id: 'cd-rgw-user-accounts' },
  create: { url: '#/rgw/accounts/create', id: 'cd-rgw-user-accounts-form' }
};

export class AccountsPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    tenant: 2,
    account_id: 3,
    email: 4,
    max_users: 5,
    max_roles: 6,
    max_groups: 7,
    max_buckets: 8,
    max_access_keys: 9
  };

  @PageHelper.restrictTo(pages.create.url)
  create(account: { name: string; email: string; tenant?: string }) {
    // Enter in account name
    cy.get('#acc_name').type(account.name);

    if (account.tenant) {
      // Enter tenant
      cy.get('#tenant').type(account.tenant);
    }

    // Enter email
    cy.get('#email').type(account.email);

    // Enter max buckets
    cy.get('input#max_buckets').should('exist').should('have.value', '1000');

    cy.get('input#max_users').should('exist').should('have.value', '1000');

    cy.get('input#max_roles').should('exist').should('have.value', '1000');

    cy.get('input#max_groups').should('exist').should('have.value', '1000');

    cy.get('input#max_access_keys').should('exist').should('have.value', '4');

    // cy.find('cds-checkbox [type="checkbox"]')
    cy.get('input#bucket_checkbox_input').check({ force: true });

    cy.get('input#account_checkbox_input').check({ force: true });

    // Click the create button and wait for account to be made
    cy.contains('button', 'Create Account').click();

    this.getFirstTableCell(account.name).should('have.text', account.name);
    this.getTableRow(account.name).within(() => {
      cy.get('td').eq(this.columnIndex.tenant).should('have.text', account.tenant);
      cy.get('td').eq(this.columnIndex.account_id).should('not.be.empty');
      cy.get('td').eq(this.columnIndex.email).should('have.text', account.email);
      cy.get('td').eq(this.columnIndex.max_users).should('have.text', 1000);
      cy.get('td').eq(this.columnIndex.max_roles).should('have.text', 1000);
      cy.get('td').eq(this.columnIndex.max_groups).should('have.text', 1000);
      cy.get('td').eq(this.columnIndex.max_buckets).should('have.text', 1000);
      cy.get('td').eq(this.columnIndex.max_access_keys).should('have.text', 4);
    });

    this.getExpandCollapseElement(account.name).click().wait(1000);

    cy.get('[data-testid="datatable-row-detail"]').first().as('accountDetailsTable');
    cy.get('@accountDetailsTable').find('legend').its(0).as('accountQuota');
    cy.get('@accountQuota').should('have.text', 'Account quota');
    cy.get('@accountDetailsTable').find('cd-table-key-value').its(0).as('accountQuotaTable');
    cy.get('@accountQuotaTable')
      .find('tbody tr')
      .first()
      .find('td')
      .last()
      .should('have.text', 'Yes');

    cy.get('@accountDetailsTable').find('legend').its(1).as('bucketQuota');
    cy.get('@bucketQuota').should('have.text', 'Bucket quota');
    cy.get('@accountDetailsTable').find('cd-table-key-value').its(1).as('bucketQuotaTable');
    cy.get('@bucketQuotaTable')
      .find('tbody tr')
      .first()
      .find('td')
      .last()
      .should('have.text', 'Yes');
  }

  @PageHelper.restrictTo(pages.create.url)
  edit(account: {
    name: string;
    tenant: string;
    email: string;
    max_buckets: string;
    max_groups: string;
    max_access_keys: string;
  }) {
    this.navigateEdit(account.name, false, false, null);
    // Enter in account name
    cy.get('#acc_name').clear().type(account.name);
    // Enter tenant
    cy.get('#tenant').should('be.disabled');
    // Enter email
    cy.get('#email').clear().type(account.email);

    // Enter max buckets
    this.selectOption('max_buckets_mode', 'Custom');
    cy.get('input#max_buckets').click().clear().type(account.max_buckets);

    this.selectOption('max_users_mode', 'Unlimited');

    this.selectOption('max_roles_mode', 'Disabled');

    cy.get('input#max_groups').click().clear().type(account.max_groups);

    cy.get('input#max_access_keys').click().clear().type(account.max_access_keys);

    cy.get('input#bucket_checkbox_input').should('be.checked');
    cy.get('input#bucket_quota_max_size').clear().type('1234');
    cy.get('input#bucketunlimitedObjects_checkbox_input').uncheck({ force: true });
    cy.get('input#bucket_quota_max_objects').clear().type('200');

    cy.get('input#account_checkbox_input').should('be.checked');
    cy.get('input#account_quota_max_size').clear().type('1234');
    cy.get('input#accountunlimitedObjects_checkbox_input').uncheck({ force: true });
    cy.get('input#account_quota_max_objects').clear().type('200');

    // Click the create button and wait for account to be made
    cy.contains('button', 'Edit Account').click();

    this.getTableRow(account.name).within(() => {
      cy.get('td').eq(this.columnIndex.tenant).should('have.text', account.tenant);
      cy.get('td').eq(this.columnIndex.account_id).should('not.be.empty');
      cy.get('td').eq(this.columnIndex.email).should('have.text', account.email);
      cy.get('td').eq(this.columnIndex.max_users).should('have.text', 'Unlimited');
      cy.get('td').eq(this.columnIndex.max_roles).should('have.text', 'Disabled');
      cy.get('td').eq(this.columnIndex.max_groups).should('have.text', account.max_groups);
      cy.get('td').eq(this.columnIndex.max_buckets).should('have.text', account.max_buckets);
      cy.get('td')
        .eq(this.columnIndex.max_access_keys)
        .should('have.text', account.max_access_keys);
    });
    this.getExpandCollapseElement(account.name).click().wait(1000);

    cy.get('[data-testid="datatable-row-detail"]').first().as('accountDetailsTable');
    cy.get('@accountDetailsTable').find('legend').eq(0).as('accountQuota');
    cy.get('@accountQuota').should('have.text', 'Account quota');
    cy.get('@accountDetailsTable').find('cd-table-key-value').eq(0).as('accountQuotaTable');
    cy.get('@accountQuotaTable').find('tbody tr').should('have.length', 3);
    cy.get('@accountQuotaTable')
      .find('tbody tr')
      .first()
      .find('td')
      .last()
      .should('have.text', 'Yes');

    cy.get('@accountQuotaTable')
      .find('tbody tr')
      .eq(1)
      .within(() => {
        cy.get('td').first().should('have.text', 'Maximum objects');
        cy.get('td').last().should('have.text', '200');
      });

    cy.get('@accountQuotaTable')
      .find('tbody tr')
      .eq(2)
      .within(() => {
        cy.get('td').first().should('have.text', 'Maximum size');
        cy.get('td').last().should('have.text', '1.2 GiB');
      });

    cy.get('@accountDetailsTable').find('legend').eq(1).as('bucketQuota');
    cy.get('@bucketQuota').should('have.text', 'Bucket quota');
    cy.get('@accountDetailsTable').find('cd-table-key-value').eq(1).as('bucketQuotaTable');
    cy.get('@bucketQuotaTable')
      .find('tbody tr')
      .first()
      .find('td')
      .last()
      .should('have.text', 'Yes');

    cy.get('@bucketQuotaTable')
      .find('tbody tr')
      .eq(1)
      .within(() => {
        cy.get('td').first().should('have.text', 'Maximum objects');
        cy.get('td').last().should('have.text', 200);
      });

    cy.get('@bucketQuotaTable')
      .find('tbody tr')
      .eq(2)
      .within(() => {
        cy.get('td').first().should('have.text', 'Maximum size');
        cy.get('td').last().should('have.text', '1.2 GiB');
      });
  }

  invalidCreate() {
    // Enter in account name
    cy.get('#acc_name').type('testAccName');

    // Enter email
    cy.get('#email')
      .type('@test')
      .blur()
      .should('not.have.class', 'ng-pending')
      .should('have.class', 'ng-invalid');

    cy.get('cds-text-label[for=email]')
      .find('.cds--form-requirement')
      .should('have.text', ' Please enter a valid email ');

    cy.get('input#max_buckets').click().clear().type('0').blur();

    cy.get('label[for=max_buckets]')
      .parent()
      .parent()
      .should('not.have.class', 'ng-pending')
      .should('have.class', 'ng-invalid');

    cy.get('label[for=max_buckets]')
      .parent()
      .parent()
      .find('.cds--form-requirement')
      .should('have.text', 'Enter number greater than 0');

    cy.get('input#bucket_checkbox_input').check({ force: true });
    cy.get('input#bucketunlimitedObjects_checkbox_input').uncheck({ force: true });
    cy.get('input#bucket_quota_max_objects').clear().type('-1').blur({ force: true });
    cy.get('input#bucket_quota_max_objects')
      .should('not.have.class', 'ng-pending')
      .should('have.class', 'ng-invalid');
  }

  invalidEdit() {
    this.create({ name: 'test', tenant: 'new', email: 'test@test' });
    this.navigateEdit('test', false, false, null);
    cy.get('#tenant').should('be.disabled');
    // Enter email
    cy.get('#email')
      .type('@test')
      .blur()
      .should('not.have.class', 'ng-pending')
      .should('have.class', 'ng-invalid');

    cy.get('cds-text-label[for=email]')
      .find('.cds--form-requirement')
      .should('have.text', ' Please enter a valid email ');

    cy.get('input#max_buckets').click().clear().type('0').blur();

    cy.get('label[for=max_buckets]')
      .parent()
      .parent()
      .should('not.have.class', 'ng-pending')
      .should('have.class', 'ng-invalid');

    cy.get('label[for=max_buckets]')
      .parent()
      .parent()
      .find('.cds--form-requirement')
      .should('have.text', 'Enter number greater than 0');

    cy.get('input#bucket_checkbox_input').should('be.checked');
    cy.get('input#bucket_quota_max_size').clear().type('0').blur();
    cy.get('label[for=bucket_quota_max_size]')
      .parent()
      .parent()
      .find('.cds--form-requirement')
      .should('have.text', 'Enter a valid value.');

    this.navigateTo();
    this.delete('test', null, null, true, false, false, false);
  }

  // happens when users are linked to account, fn() called from users-e2e
  invalidDelete(account_name: string, action: string) {
    cy.intercept('DELETE', `/api/rgw/accounts/*`, {
      statusCode: 500,
      body: { error: 'Internal Server Error' }
    }).as('apiRequest');

    const actionUpperCase = action.charAt(0).toUpperCase() + action.slice(1);
    this.clickRowActionButton(account_name, action);
    cy.get('[aria-label="confirmation"]').click({ force: true });
    cy.get('cds-modal button').contains(actionUpperCase).click({ force: true });
    cy.wait('@apiRequest').then((interception: any) => {
      cy.log('Intercepted request:', interception);
    });
    // modal should not close as API should be failing
    cy.get('cds-modal').should('exist');
    cy.get('@apiRequest').its('response.statusCode').should('eq', 500);
  }
}
