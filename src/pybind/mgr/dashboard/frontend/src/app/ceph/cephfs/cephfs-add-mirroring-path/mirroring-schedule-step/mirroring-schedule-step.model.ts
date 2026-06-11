export const SCHEDULE_TYPE = {
  INTERVAL: 'interval',
  CRON: 'cron'
} as const;

export type ScheduleType = (typeof SCHEDULE_TYPE)[keyof typeof SCHEDULE_TYPE];

export const START_AM_PM = {
  AM: 'AM',
  PM: 'PM'
} as const;

export type StartAmPm = (typeof START_AM_PM)[keyof typeof START_AM_PM];

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

export type MirroringScheduleTimezone = (typeof MIRRORING_SCHEDULE_TIMEZONES)[number];

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
}
