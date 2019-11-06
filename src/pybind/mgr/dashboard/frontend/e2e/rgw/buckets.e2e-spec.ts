import { $ } from 'protractor';
import { BucketsPageHelper } from './buckets.po';

describe('RGW buckets page', () => {
  let buckets: BucketsPageHelper;
  const bucket_name = '000test';

  beforeAll(async () => {
    buckets = new BucketsPageHelper();
  });

  afterEach(async () => {
    await BucketsPageHelper.checkConsole();
  });

  describe('breadcrumb tests', () => {
    beforeEach(async () => {
      await buckets.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await expect($('.breadcrumb-item.active').getText()).toBe('Buckets');
    });
  });

  describe('create, edit & delete bucket tests', () => {
    beforeEach(async () => {
      await buckets.navigateTo();
      await buckets.uncheckAllTableRows();
    });

    it('should create bucket', async () => {
      await buckets.navigateTo('create');
      await buckets.create(
        bucket_name,
        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
        'default-placement'
      );
      await expect(buckets.getFirstTableCellWithText(bucket_name).isPresent()).toBe(true);
    });

    it('should edit bucket', async () => {
      await buckets.edit(bucket_name, 'dev');
      await expect(buckets.getTable().getText()).toMatch('dev');
    });

    it('should delete bucket', async () => {
      await buckets.delete(bucket_name);
    });
  });

  describe('Invalid Input in Create and Edit tests', () => {
    it('should test invalid inputs in create fields', async () => {
      await buckets.testInvalidCreate();
    });

    it('should test invalid input in edit owner field', async () => {
      await buckets.navigateTo('create');
      await buckets.create(
        '000rq',
        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
        'default-placement'
      );
      await buckets.testInvalidEdit('000rq');
      await buckets.navigateTo();
      await buckets.uncheckAllTableRows();
      await buckets.delete('000rq');
    });
  });
});
