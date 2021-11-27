export const PAGE_LIMIT = 10;

export class PageInfo {
  // Total number of rows in a table
  count: number;

  // Current page (current row = offset x limit or pageSize)
  offset: number = 0;

  // Max. number of rows fetched from the server
  limit: number = PAGE_LIMIT;

  /*
  pageSize and limit can be decoupled if hybrid server-side and client-side
  are used. A use-case would be to reduce the amount of queries: that is,
  the pageSize (client-side paging) might be 10, but the back-end queries
  could have a limit of 100. That would avoid triggering requests
  */
  pageSize: number = PAGE_LIMIT;
}
