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

  constructor(error: () => void) {
    this.error = error;
  }
}
