import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/rgw/bucket', id: 'cd-rgw-bucket-list' },
  create: { url: '#/rgw/bucket/create', id: 'cd-rgw-bucket-form' }
};

export class BucketsPageHelper extends PageHelper {
  pages = pages;

  versioningStateEnabled = 'Enabled';
  versioningStateSuspended = 'Suspended';

  private selectOwner(owner: string) {
    return this.selectOption('owner', owner);
  }

  private selectPlacementTarget(placementTarget: string) {
    return this.selectOption('placement-target', placementTarget);
  }

  private selectLockMode(lockMode: string) {
    return this.selectOption('lock_mode', lockMode);
  }

  @PageHelper.restrictTo(pages.create.url)
  create(name: string, owner: string, placementTarget: string, isLocking = false) {
    // Enter in bucket name
    cy.get('#bid').type(name);

    // Select bucket owner
    this.selectOwner(owner);
    cy.get('#owner').should('have.class', 'ng-valid');

    // Select bucket placement target:
    this.selectPlacementTarget(placementTarget);
    cy.get('#placement-target').should('have.class', 'ng-valid');

    if (isLocking) {
      cy.get('#lock_enabled').click({ force: true });
      // Select lock mode:
      this.selectLockMode('Compliance');
      cy.get('#lock_mode').should('have.class', 'ng-valid');
      cy.get('#lock_retention_period_days').type('3');
    }

    // Click the create button and wait for bucket to be made
    cy.contains('button', 'Create Bucket').click();

    this.getFirstTableCell(name).should('exist');
  }

  @PageHelper.restrictTo(pages.index.url)
  edit(name: string, new_owner: string, isLocking = false) {
    this.navigateEdit(name);

    cy.get('input[name=placement-target]').should('have.value', 'default-placement');
    this.selectOwner(new_owner);

    // If object locking is enabled versioning shouldn't be visible
    if (isLocking) {
      cy.get('input[id=versioning]').should('be.disabled');
      cy.contains('button', 'Edit Bucket').click();

      // wait to be back on buckets page with table visible and click
      this.getExpandCollapseElement(name).click();

      // check its details table for edited owner field
      cy.get('.table.table-striped.table-bordered')
        .first()
        .should('contains.text', new_owner)
        .as('bucketDataTable');

      // Check versioning enabled:
      cy.get('@bucketDataTable').find('tr').its(2).find('td').last().should('have.text', new_owner);
      cy.get('@bucketDataTable').find('tr').its(11).find('td').last().as('versioningValueCell');

      return cy.get('@versioningValueCell').should('have.text', this.versioningStateEnabled);
    }
    // Enable versioning
    cy.get('input[id=versioning]').should('not.be.checked');
    cy.get('label[for=versioning]').click();
    cy.get('input[id=versioning]').should('be.checked');

    cy.contains('button', 'Edit Bucket').click();

    // wait to be back on buckets page with table visible and click
    this.getExpandCollapseElement(name).click();

    // check its details table for edited owner field
    cy.get('.table.table-striped.table-bordered')
      .first()
      .should('contains.text', new_owner)
      .as('bucketDataTable');

    // Check versioning enabled:
    cy.get('@bucketDataTable').find('tr').its(2).find('td').last().should('have.text', new_owner);
    cy.get('@bucketDataTable').find('tr').its(11).find('td').last().as('versioningValueCell');

    cy.get('@versioningValueCell').should('have.text', this.versioningStateEnabled);

    // Disable versioning:
    this.navigateEdit(name);

    cy.get('label[for=versioning]').click();
    cy.get('input[id=versioning]').should('not.be.checked');
    cy.contains('button', 'Edit Bucket').click();

    // Check versioning suspended:
    this.getExpandCollapseElement(name).click();

    return cy.get('@versioningValueCell').should('have.text', this.versioningStateSuspended);
  }

  testInvalidCreate() {
    this.navigateTo('create');
    cy.get('#bid').as('nameInputField'); // Grabs name box field

    // Gives an invalid name (too short), then waits for dashboard to determine validity
    cy.get('@nameInputField').type('rq');

    cy.contains('button', 'Create Bucket').click(); // To trigger a validation

    // Waiting for website to decide if name is valid or not
    // Check that name input field was marked invalid in the css
    cy.get('@nameInputField')
      .should('not.have.class', 'ng-pending')
      .and('have.class', 'ng-invalid');

    // Check that error message was printed under name input field
    cy.get('#bid + .invalid-feedback').should(
      'have.text',
      'Bucket names must be 3 to 63 characters long.'
    );

    // Test invalid owner input
    // select some valid option. The owner drop down error message will not appear unless a valid user was selected at
    // one point before the invalid placeholder user is selected.
    this.selectOwner('dev');

    // select the first option, which is invalid because it is a placeholder
    this.selectOwner('-- Select a user --');

    cy.get('@nameInputField').click();

    // Check that owner drop down field was marked invalid in the css
    cy.get('#owner').should('have.class', 'ng-invalid');

    // Check that error message was printed under owner drop down field
    cy.get('#owner + .invalid-feedback').should('have.text', 'This field is required.');

    // Check invalid placement target input
    this.selectOwner('dev');
    // The drop down error message will not appear unless a valid option is previsously selected.
    this.selectPlacementTarget('default-placement');
    this.selectPlacementTarget('-- Select a placement target --');
    cy.get('@nameInputField').click(); // Trigger validation
    cy.get('#placement-target').should('have.class', 'ng-invalid');
    cy.get('#placement-target + .invalid-feedback').should('have.text', 'This field is required.');

    // Clicks the Create Bucket button but the page doesn't move.
    // Done by testing for the breadcrumb
    cy.contains('button', 'Create Bucket').click(); // Clicks Create Bucket button
    this.expectBreadcrumbText('Create');
    // content in fields seems to subsist through tests if not cleared, so it is cleared
    cy.get('@nameInputField').clear();
    return cy.contains('button', 'Cancel').click();
  }

  testInvalidEdit(name: string) {
    this.navigateEdit(name);

    cy.get('input[id=versioning]').should('exist').and('not.be.checked');

    // Chooses 'Select a user' rather than a valid owner on Edit Bucket page
    // and checks if it's an invalid input

    // select the first option, which is invalid because it is a placeholder
    this.selectOwner('-- Select a user --');

    cy.contains('button', 'Edit Bucket').click();

    // Check that owner drop down field was marked invalid in the css
    cy.get('#owner').should('have.class', 'ng-invalid');

    // Check that error message was printed under owner drop down field
    cy.get('#owner + .invalid-feedback').should('have.text', 'This field is required.');

    this.expectBreadcrumbText('Edit');
  }
}
