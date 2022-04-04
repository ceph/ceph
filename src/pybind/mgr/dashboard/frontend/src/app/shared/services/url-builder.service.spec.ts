import { URLVerbs } from '../constants/app.constants';
import { URLBuilderService } from './url-builder.service';

describe('URLBuilderService', () => {
  const base = 'pool';
  const urlBuilder = new URLBuilderService(base);

  it('get base', () => {
    expect(urlBuilder.base).toBe(base);
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

  it('get Create From URL', () => {
    const id = 'someId';
    expect(urlBuilder.getCreateFrom(id)).toBe(`/${urlBuilder.base}/${URLVerbs.CREATE}/${id}`);
  });

  it('get Edit URL with item', () => {
    const item = 'test_pool';
    expect(urlBuilder.getEdit(item)).toBe(`/${urlBuilder.base}/${URLVerbs.EDIT}/${item}`);
  });
});
