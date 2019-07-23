import { Helper } from '../helper.po';

describe('RGW buckets page', () => {
  let buckets;

  beforeAll(() => {
    buckets = new Helper().buckets;
    buckets.navigateTo();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      expect(buckets.getBreadcrumbText()).toEqual('Buckets');
    });
  });

  describe('create, edit & delete bucket test', () => {
    it('should create bucket', () => {
      buckets.create(
        '000test',
        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
        'default-placement'
      );
      expect(buckets.getTableCell('000test').isPresent()).toBe(true);
    });

    it('should edit bucket', () => {
      buckets.edit('000test', 'dev');
      expect(buckets.getTable().getText()).toMatch('dev');
    });

    it('should delete bucket', () => {
      buckets.delete('000test');
      expect(buckets.getTableCell('000test').isPresent()).toBe(false);
    });
  });

  describe('Invalid Input in Create and Edit tests', () => {
    it('should test invalid inputs in create fields', () => {
      buckets.invalidCreate();
    });

    it('should test invalid input in edit owner field', () => {
      buckets.create(
        '000rq',
        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
        'default-placement'
      );
      buckets.invalidEdit('000rq');
      buckets.delete('000rq');
    });
  });
});
