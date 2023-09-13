export class PgCategory {
  static readonly CATEGORY_CLEAN = 'clean';
  static readonly CATEGORY_WORKING = 'working';
  static readonly CATEGORY_WARNING = 'warning';
  static readonly CATEGORY_UNKNOWN = 'unknown';
  static readonly VALID_CATEGORIES = [
    PgCategory.CATEGORY_CLEAN,
    PgCategory.CATEGORY_WORKING,
    PgCategory.CATEGORY_WARNING,
    PgCategory.CATEGORY_UNKNOWN
  ];

  states: string[];

  constructor(public type: string) {
    if (!this.isValidType()) {
      throw new Error('Wrong placement group category type');
    }

    this.setTypeStates();
  }

  private isValidType() {
    return PgCategory.VALID_CATEGORIES.includes(this.type);
  }

  private setTypeStates() {
    switch (this.type) {
      case PgCategory.CATEGORY_CLEAN:
        this.states = ['active', 'clean'];
        break;
      case PgCategory.CATEGORY_WORKING:
        this.states = [
          'activating',
          'backfill_wait',
          'backfilling',
          'creating',
          'deep',
          'degraded',
          'forced_backfill',
          'forced_recovery',
          'peering',
          'peered',
          'recovering',
          'recovery_wait',
          'repair',
          'scrubbing',
          'snaptrim',
          'snaptrim_wait'
        ];
        break;
      case PgCategory.CATEGORY_WARNING:
        this.states = [
          'backfill_toofull',
          'backfill_unfound',
          'down',
          'incomplete',
          'inconsistent',
          'recovery_toofull',
          'recovery_unfound',
          'remapped',
          'snaptrim_error',
          'stale',
          'undersized'
        ];
        break;
      default:
        this.states = [];
    }
  }
}
