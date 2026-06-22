export const SCHEDULE_TYPE = {
  INTERVAL: 'interval',
  CRON: 'cron'
} as const;

export type ScheduleType = typeof SCHEDULE_TYPE[keyof typeof SCHEDULE_TYPE];

export const START_AM_PM = {
  AM: 'AM',
  PM: 'PM'
} as const;

export type StartAmPm = typeof START_AM_PM[keyof typeof START_AM_PM];

export const MIRRORING_SCHEDULE_TIMEZONES = [
  'UTC',
  'US/Eastern',
  'US/Central',
  'US/Mountain',
  'US/Pacific',
  'Europe/London',
  'Europe/Berlin',
  'Asia/Kolkata',
  'Asia/Tokyo'
] as const;

export type MirroringScheduleTimezone = typeof MIRRORING_SCHEDULE_TIMEZONES[number];

export const EXISTING_SCHEDULE_HANDLING = {
  KEEP_AND_ADD: 'keep_and_add',
  REPLACE: 'replace',
  CONFIGURE_INDIVIDUALLY: 'configure_individually'
} as const;

export type ExistingScheduleHandling = typeof EXISTING_SCHEDULE_HANDLING[keyof typeof EXISTING_SCHEDULE_HANDLING];

export const EXISTING_SCHEDULE_ACTION = {
  KEEP_AND_ADD: 'keep_and_add',
  REPLACE: 'replace'
} as const;

export type ExistingScheduleAction = typeof EXISTING_SCHEDULE_ACTION[keyof typeof EXISTING_SCHEDULE_ACTION];

export interface PathScheduleConflictRow {
  id: string;
  path: string;
  scheduleCopy: string;
  filesReplicating: string;
  action: ExistingScheduleAction;
}

export interface MirroringScheduleFormValue {
  scheduleName: string;
  scheduleType: ScheduleType;
  repeatInterval: number;
  repeatFrequency: string;
  startDate: string;
  startTime: string;
  startAmPm: StartAmPm;
  startTimezone: string;
  retentionInterval: number;
  retentionFrequency: string;
  retentionCount: number;
  existingScheduleHandling: ExistingScheduleHandling;
}
