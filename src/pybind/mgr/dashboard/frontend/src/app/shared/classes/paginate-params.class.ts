import { HttpParams } from '@angular/common/http';

export class PaginateParams {
  constructor(params: HttpParams, majorVersion = 1, minorVersion = 0) {
    const options = {
      params: params,
      headers: {
        Accept: `application/vnd.ceph.api.v${majorVersion}.${minorVersion}+json`
      }
    };

    options['observe'] = 'response';
    return options;
  }
}
