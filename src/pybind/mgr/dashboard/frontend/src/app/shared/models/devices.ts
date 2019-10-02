/**
 * Fields returned by the back-end.
 */
export interface CephDevice {
  devid: string;
  location: { host: string; dev: string }[];
  daemons: string[];
  life_expectancy_min?: string;
  life_expectancy_max?: string;
  life_expectancy_stamp?: string;
}

/**
 * Fields added by the front-end. Fields may be empty if no expectancy is provided for the
 * CephDevice interface.
 */
export interface CdDevice extends CephDevice {
  life_expectancy_weeks?: {
    max: number;
    min: number;
  };
  state?: 'good' | 'warning' | 'bad' | 'stale' | 'unknown';
  readableDaemons?: string; // Human readable daemons (which can wrap lines inside the table cell)
}
