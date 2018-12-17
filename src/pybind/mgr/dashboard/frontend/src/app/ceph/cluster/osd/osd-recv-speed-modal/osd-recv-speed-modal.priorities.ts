export class OsdRecvSpeedModalPriorities {
  public static KNOWN_PRIORITIES = [
    // TODO: I18n
    {
      name: null,
      text: '-- Select the priority --',
      values: {
        osd_max_backfills: null,
        osd_recovery_max_active: null,
        osd_recovery_max_single_start: null,
        osd_recovery_sleep: null
      }
    },
    {
      name: 'low',
      text: 'Low',
      values: {
        osd_max_backfills: 1,
        osd_recovery_max_active: 1,
        osd_recovery_max_single_start: 1,
        osd_recovery_sleep: 0.5
      }
    },
    {
      name: 'default',
      text: 'Default',
      values: {
        osd_max_backfills: 1,
        osd_recovery_max_active: 3,
        osd_recovery_max_single_start: 1,
        osd_recovery_sleep: 0
      }
    },
    {
      name: 'high',
      text: 'High',
      values: {
        osd_max_backfills: 4,
        osd_recovery_max_active: 4,
        osd_recovery_max_single_start: 4,
        osd_recovery_sleep: 0
      }
    }
  ];
}
