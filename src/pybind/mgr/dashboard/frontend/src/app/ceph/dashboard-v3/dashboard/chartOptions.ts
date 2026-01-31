import { AreaChartOptions, ScaleTypes } from '@carbon/charts-angular';
import { format } from 'd3-format';

const areaCommon: AreaChartOptions = {
  height: '200px',
  grid: {
    x: { enabled: false },
    y: { enabled: false }
  },
  points: { enabled: false },
  legend: { clickable: true },
  toolbar: {
    enabled: false
  },
  axes: {
    left: {
      mapsTo: 'value',
      ticks: { formatter: format('.2~s') }
    },
    bottom: {
      title: 'Date',
      visible: false,
      scaleType: ScaleTypes.TIME,
      mapsTo: 'date'
    }
  }
};

export const usedCapacity: AreaChartOptions = {
  title: 'Used Capacity (RAW)',
  ...areaCommon
};

export const IOPS: AreaChartOptions = {
  title: 'IOPS',
  ...areaCommon
};

export const osdLatencies: AreaChartOptions = {
  title: 'OSD latencies',
  ...areaCommon
};

export const clientThroughput: AreaChartOptions = {
  title: 'Client throughput',
  ...areaCommon
};

export const recoveryThroughput: AreaChartOptions = {
  title: 'Recovery throughput',
  ...areaCommon
};
