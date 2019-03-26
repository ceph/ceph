import { Location } from '@angular/common';

import { URLVerbs } from '../constants/app.constants';

export class URLBuilderService {
  constructor(readonly base: string) {}

  private static concatURLSegments(segments: string[]): string {
    return segments.reduce(Location.joinWithSlash);
  }

  static buildURL(absolute: boolean, ...segments: string[]): string {
    return URLBuilderService.concatURLSegments([...(absolute ? ['/'] : []), ...segments]);
  }

  private getURL(verb: URLVerbs, absolute = true, ...segments: string[]): string {
    return URLBuilderService.buildURL(absolute, this.base, verb, ...segments);
  }

  getCreate(absolute = true): string {
    return this.getURL(URLVerbs.CREATE, absolute);
  }
  getDelete(absolute = true): string {
    return this.getURL(URLVerbs.DELETE, absolute);
  }

  getEdit(item: string, absolute = true): string {
    return this.getURL(URLVerbs.EDIT, absolute, item);
  }
  getUpdate(item: string, absolute = true): string {
    return this.getURL(URLVerbs.UPDATE, absolute, item);
  }

  getAdd(absolute = true): string {
    return this.getURL(URLVerbs.ADD, absolute);
  }
  getRemove(absolute = true): string {
    return this.getURL(URLVerbs.REMOVE, absolute);
  }
}
