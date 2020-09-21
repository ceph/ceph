import { BucketsPageHelper } from './buckets.po';

describe('RGW buckets page', () => {
  const buckets = new BucketsPageHelper();
  const bucket_name = 'e2ebucket';

  beforeEach(() => {
    cy.login();
    buckets.navigateTo();
  });

  describe('breadcrumb tests', () => {
    it('should open and show breadcrumb', () => {
      buckets.expectBreadcrumbText('Buckets');
    });
  });

  describe('create, edit & delete bucket tests', () => {
    it('should create bucket', () => {
      buckets.navigateTo('create');
      buckets.create(
        bucket_name,
        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
        'default-placement'
      );
      buckets.getFirstTableCell(bucket_name).should('exist');
    });

    it('should edit bucket', () => {
      buckets.edit(bucket_name, 'dev');
      buckets.getDataTables().should('contain.text', 'dev');
    });

    it('should delete bucket', () => {
      buckets.delete(bucket_name);
    });
  });

  describe('Invalid Input in Create and Edit tests', () => {
    it('should test invalid inputs in create fields', () => {
      buckets.testInvalidCreate();
    });

    it('should test invalid input in edit owner field', () => {
      buckets.navigateTo('create');
      buckets.create(
        bucket_name,
        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
        'default-placement'
      );
      buckets.testInvalidEdit(bucket_name);
      buckets.navigateTo();
      buckets.delete(bucket_name);
    });
  });
});
