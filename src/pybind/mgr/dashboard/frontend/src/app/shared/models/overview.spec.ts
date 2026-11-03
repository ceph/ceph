import { CompressionStats } from '../enum/hardware.enum';
import { buildConsumptionStats, buildSavingsCardVM, toTrendPoints, TrendPoint } from './overview';

const compressionStats: CompressionStats = {
  fcm_drive_count: 4,
  phy_util_percent: 38.5,
  log_util_percent: 61.2,
  savings_bytes: 1024,
  compression_ratio: 2.4,
  efficiency_percent: 58.3
};

const trendOf = (...values: number[]): TrendPoint[] =>
  values.map((value, i) => ({ timestamp: new Date(i * 1000), values: { Used: value } }));

describe('overview models', () => {
  describe('toTrendPoints', () => {
    it('should convert [seconds, value] pairs to trend points', () => {
      expect(toTrendPoints([[60, '5']])).toEqual([
        { timestamp: new Date(60000), values: { Used: 5 } }
      ]);
    });

    it('should return an empty list when there is no data', () => {
      expect(toTrendPoints()).toEqual([]);
    });
  });

  describe('buildConsumptionStats', () => {
    it('should expose the time until full and the average consumption', () => {
      const stats = buildConsumptionStats('30 days', '12 GiB/day');

      expect(stats.map((stat) => stat.value)).toEqual(['30 days', '12 GiB/day']);
      expect(stats.every((stat) => stat.label && stat.description)).toBe(true);
    });
  });

  describe('buildSavingsCardVM', () => {
    it('should format the stats and pass the total and trend through', () => {
      const points = trendOf(1, 2);
      const vm = buildSavingsCardVM(compressionStats, points, '1 KiB');

      expect(vm.total).toBe('1 KiB');
      expect(vm.trendData).toBe(points);
      expect(vm.stats.map((stat) => stat.value)).toEqual(['2.4', '58.3%', '38.5%']);
    });

    it('should leave values that are gated below 100 GiB empty but keep utilisation', () => {
      const vm = buildSavingsCardVM(
        {
          ...compressionStats,
          savings_bytes: null,
          compression_ratio: null,
          efficiency_percent: null
        },
        [],
        null
      );

      expect(vm.total).toBeNull();
      expect(vm.stats.map((stat) => stat.value)).toEqual([null, null, '38.5%']);
    });

    it.each([
      ['rising', trendOf(5, 6, 7), true],
      ['flat', trendOf(7, 7, 7), false],
      ['falling', trendOf(7, 6, 5), false],
      ['a single point', trendOf(7), false],
      ['empty', [], false]
    ])('should set isTrendingUp for a %s trend', (_name, points, expected) => {
      expect(buildSavingsCardVM(compressionStats, points, null).isTrendingUp).toBe(expected);
    });
  });
});
