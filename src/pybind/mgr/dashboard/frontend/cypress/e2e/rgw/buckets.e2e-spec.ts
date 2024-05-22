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
      buckets.create(bucket_name, BucketsPageHelper.USERS[0]);
      buckets.getFirstTableCell(bucket_name).should('exist');
    });

    it('should edit bucket', () => {
      buckets.edit(bucket_name, BucketsPageHelper.USERS[1]);
      buckets.getDataTables().should('contain.text', BucketsPageHelper.USERS[1]);
    });

    it('should delete bucket', () => {
      buckets.delete(bucket_name);
    });

    it('should check default encryption is SSE-S3', () => {
      buckets.navigateTo('create');
      buckets.checkForDefaultEncryption();
    });

    it('should create bucket with object locking enabled', () => {
      buckets.navigateTo('create');
      buckets.create(bucket_name, BucketsPageHelper.USERS[0], true);
      buckets.getFirstTableCell(bucket_name).should('exist');
    });

    it('should not allow to edit versioning if object locking is enabled', () => {
      buckets.edit(bucket_name, BucketsPageHelper.USERS[1], true);
      buckets.getDataTables().should('contain.text', BucketsPageHelper.USERS[1]);

      buckets.delete(bucket_name);
    });
  });

  describe('Invalid Input in Create and Edit tests', () => {
    it('should test invalid inputs in create fields', () => {
      buckets.testInvalidCreate();
    });

    it('should test invalid input in edit owner field', () => {
      buckets.navigateTo('create');
      buckets.create(bucket_name, BucketsPageHelper.USERS[0]);
      buckets.testInvalidEdit(bucket_name);
      buckets.navigateTo();
      buckets.delete(bucket_name);
    });
  });
});
