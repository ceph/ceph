import { HttpParams } from '@angular/common/http';

import { PageInfo } from './cd-table-paging';

export class CdTableFetchDataContext {
  errorConfig = {
    resetData: true, // Force data table to show no data
    displayError: true // Show an error panel above the data table
  };

  /**
   * The function that should be called from within the error handler
   * of the 'fetchData' function to display the error panel and to
   * reset the data table to the correct state.
   */
  error: Function;
  pageInfo: PageInfo = new PageInfo();
  search = '';

  constructor(error: () => void) {
    this.error = error;
  }

  toParams(): HttpParams {
    if (this.pageInfo.limit === null) {
      this.pageInfo.limit = 0;
    }
    if (this.search === null) {
      this.search = '';
    }
    return new HttpParams({
      fromObject: {
        offset: String(this.pageInfo.offset * this.pageInfo.limit),
        limit: String(this.pageInfo.limit),
        search: this.search
      }
    });
  }
}
