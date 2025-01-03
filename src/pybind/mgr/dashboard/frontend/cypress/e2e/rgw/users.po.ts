import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/rgw/user', id: 'cd-rgw-user-list' },
  create: { url: '#/rgw/user/create', id: 'cd-rgw-user-form' }
};

export class UsersPageHelper extends PageHelper {
  pages = pages;

  @PageHelper.restrictTo(pages.create.url)
  create(tenant: string, user_id: string, fullname: string, email: string, maxbuckets: string) {
    // Enter in user_id
    cy.get('#user_id').type(user_id);
    // Show Tenant
    cy.get('#show_tenant').click({ force: true });
    // Enter in tenant
    cy.get('#tenant').type(tenant);
    // Enter in full name
    cy.get('#display_name').click().type(fullname);

    // Enter in email
    cy.get('#email').click().type(email);

    // Enter max buckets
    this.selectOption('max_buckets_mode', 'Custom');
    cy.get('#max_buckets').should('exist').should('have.value', '1000');
    cy.get('#max_buckets').click().clear().type(maxbuckets);

    // Click the create button and wait for user to be made
    cy.contains('button', 'Create User').click();
    this.getFirstTableCell(tenant + '$' + user_id).should('exist');
  }

  @PageHelper.restrictTo(pages.index.url)
  edit(name: string, new_fullname: string, new_email: string, new_maxbuckets: string) {
    this.navigateEdit(name, false, true, null, true);

    // Change the full name field
    cy.get('#display_name').click().clear().type(new_fullname);

    // Change the email field
    cy.get('#email').click().clear().type(new_email);

    // Change the max buckets field
    this.selectOption('max_buckets_mode', 'Custom');
    cy.get('#max_buckets').click().clear().type(new_maxbuckets);

    cy.contains('button', 'Edit User').click();

    // Click the user and check its details table for updated content
    this.getExpandCollapseElement(name).click();
    cy.get('[data-testid="datatable-row-detail"]')
      .should('contain.text', new_fullname)
      .and('contain.text', new_email)
      .and('contain.text', new_maxbuckets);
  }

  invalidCreate() {
    const tenant = '000invalid_tenant';
    const uname = '000invalid_create_user';
    // creating this user in order to check that you can't give two users the same name
    this.navigateTo('create');
    this.create(tenant, uname, 'xxx', 'xxx@xxx', '1');

    this.navigateTo('create');

    // Username
    cy.get('#user_id')
      // No username had been entered. Field should be invalid
      .should('have.class', 'ng-invalid')
      // Try to give user already taken name. Should make field invalid.
      .type(uname);
    cy.get('#show_tenant').click({ force: true });
    cy.get('#tenant').type(tenant).should('have.class', 'ng-invalid');
    cy.contains('#tenant + .invalid-feedback', 'The chosen user ID exists in this tenant.');

    // check that username field is marked invalid if username has been cleared off
    cy.get('#user_id').clear().blur().should('have.class', 'ng-invalid');
    cy.contains('#user_id + .invalid-feedback', 'This field is required.');

    // Full name
    cy.get('#display_name')
      // No display name has been given so field should be invalid
      .should('have.class', 'ng-invalid')
      // display name field should also be marked invalid if given input then emptied
      .type('a')
      .clear()
      .blur()
      .should('have.class', 'ng-invalid');
    cy.contains('#display_name + .invalid-feedback', 'This field is required.');

    // put invalid email to make field invalid
    cy.get('#email').type('a').blur().should('have.class', 'ng-invalid');
    cy.contains('#email + .invalid-feedback', 'This is not a valid email address.');

    // put negative max buckets to make field invalid
    this.expectSelectOption('max_buckets_mode', 'Custom');
    cy.get('#max_buckets').clear().type('-5').blur().should('have.class', 'ng-invalid');
    cy.contains('#max_buckets + .invalid-feedback', 'The entered value must be >= 1.');

    this.navigateTo();
    this.delete(tenant + '$' + uname, null, null, true, true);
  }

  invalidEdit() {
    const tenant = '000invalid_tenant';
    const uname = '000invalid_edit_user';
    // creating this user to edit for the test
    this.navigateTo('create');
    this.create(tenant, uname, 'xxx', 'xxx@xxx', '50');
    const name = tenant + '$' + uname;
    this.navigateEdit(name, false, true, null, true);

    // put invalid email to make field invalid
    cy.get('#email')
      .clear()
      .type('a')
      .blur()
      .should('not.have.class', 'ng-pending')
      .should('have.class', 'ng-invalid');
    cy.contains('#email + .invalid-feedback', 'This is not a valid email address.');

    // empty the display name field making it invalid
    cy.get('#display_name').clear().blur().should('have.class', 'ng-invalid');
    cy.contains('#display_name + .invalid-feedback', 'This field is required.');

    // put negative max buckets to make field invalid
    this.selectOption('max_buckets_mode', 'Disabled');
    cy.get('#max_buckets').should('not.exist');
    this.selectOption('max_buckets_mode', 'Custom');
    cy.get('#max_buckets').should('exist').should('have.value', '50');
    cy.get('#max_buckets').clear().type('-5').blur().should('have.class', 'ng-invalid');
    cy.contains('#max_buckets + .invalid-feedback', 'The entered value must be >= 1.');

    this.navigateTo();
    this.delete(tenant + '$' + uname, null, null, true, true);
  }
}
