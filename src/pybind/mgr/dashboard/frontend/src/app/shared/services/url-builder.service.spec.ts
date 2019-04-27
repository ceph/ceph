import { URLVerbs } from '../constants/app.constants';
import { URLBuilderService } from './url-builder.service';

describe('URLBuilderService', () => {
  const BASE = 'pool';
  const urlBuilder = new URLBuilderService(BASE);

  it('get base', () => {
    expect(urlBuilder.base).toBe(BASE);
  });

  it('build absolute URL', () => {
    expect(URLBuilderService.buildURL(true, urlBuilder.base, URLVerbs.CREATE)).toBe(
      `/${urlBuilder.base}/${URLVerbs.CREATE}`
    );
  });

  it('build relative URL', () => {
    expect(URLBuilderService.buildURL(false, urlBuilder.base, URLVerbs.CREATE)).toBe(
      `${urlBuilder.base}/${URLVerbs.CREATE}`
    );
  });

  it('get Create URL', () => {
    expect(urlBuilder.getCreate()).toBe(`/${urlBuilder.base}/${URLVerbs.CREATE}`);
  });

  it('get Edit URL with item', () => {
    const item = 'test_pool';
    expect(urlBuilder.getEdit(item)).toBe(`/${urlBuilder.base}/${URLVerbs.EDIT}/${item}`);
  });
});
