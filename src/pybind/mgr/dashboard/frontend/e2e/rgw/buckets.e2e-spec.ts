import { $ } from 'protractor';
import { Helper } from '../helper.po';

describe('RGW buckets page', () => {
  let buckets: Helper['buckets'];

  beforeAll(async () => {
    const helper = new Helper();
    buckets = helper.buckets;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  it('should open and show breadcrumb', async () => {
    await buckets.navigateTo();
    expect(await $('.breadcrumb-item.active').getText()).toBe('Buckets');
  });

  it('should create bucket', async () => {
    await buckets.navigateTo('create');
    await buckets.create(
      '000test',
      '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      'default-placement'
    );
    expect(await buckets.getTableCell('000test').isPresent()).toBe(true);
  });

  it('should edit bucket', async () => {
    await buckets.navigateTo();
    await buckets.edit('000test', 'dev');
    expect(await buckets.getTable().getText()).toMatch('dev');
  });

  it('should delete bucket', async () => {
    await buckets.navigateTo();
    await buckets.delete('000test');
    expect(buckets.getTableCell('000test').isPresent()).toBe(false);
  });

  describe('Invalid Input in Create and Edit tests', () => {
    it('should test invalid inputs in create fields', async () => {
      await buckets.navigateTo('create');
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
      await buckets.delete('000rq');
    });
  });
});
