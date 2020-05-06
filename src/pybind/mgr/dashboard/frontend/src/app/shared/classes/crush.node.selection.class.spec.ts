import { FormControl } from '@angular/forms';

import { configureTestBed, Mocks } from '../../../testing/unit-test-helper';
import { CrushNode } from '../models/crush-node';
import { CrushNodeSelectionClass } from './crush.node.selection.class';

describe('CrushNodeSelectionService', () => {
  const nodes = Mocks.getCrushNodes()

  let service: CrushNodeSelectionClass;
  let controls: {
    root: FormControl;
    failure: FormControl;
    device: FormControl;
  };

  // Object contains functions to get something
  const get = {
    nodeByName: (name: string): CrushNode => nodes.find((node) => node.name === name),
    nodesByNames: (names: string[]): CrushNode[] => names.map(get.nodeByName)
  };

  // Expects that are used frequently
  const assert = {
    failureDomains: (nodes: CrushNode[], types: string[]) => {
      const expectation = {};
      types.forEach((type) => (expectation[type] = nodes.filter((node) => node.type === type)));
      const keys = service.failureDomainKeys;
      expect(keys).toEqual(types);
      keys.forEach((key) => {
        expect(service.failureDomains[key].length).toBe(expectation[key].length);
      });
    },
    formFieldValues: (root: CrushNode, failureDomain: string, device: string) => {
      expect(controls.root.value).toEqual(root);
      expect(controls.failure.value).toBe(failureDomain);
      expect(controls.device.value).toBe(device);
    },
    valuesOnRootChange: (
      rootName: string,
      expectedFailureDomain: string,
      expectedDevice: string
    ) => {
      const node = get.nodeByName(rootName);
      controls.root.setValue(node);
      assert.formFieldValues(node, expectedFailureDomain, expectedDevice);
    }
  };

  configureTestBed({
    providers: [CrushNodeSelectionClass]
  });

  beforeEach(() => {
    controls = {
      root: new FormControl(null),
      failure: new FormControl(''),
      device: new FormControl('')
    };
    // Normally this should be extended by the class using it
    service = new CrushNodeSelectionClass();
    // Therefore to get it working correctly use "this" instead of "service"
    service.initCrushNodeSelection(nodes, controls.root, controls.failure, controls.device);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
    expect(nodes.length).toBe(12);
  });

  describe('lists', () => {
    afterEach(() => {
      // The available buckets should not change
      expect(service.buckets).toEqual(
        get.nodesByNames(['default', 'hdd-rack', 'mix-host', 'ssd-host', 'ssd-rack'])
      );
    });

    it('has the following lists after init', () => {
      assert.failureDomains(nodes, ['host', 'osd', 'osd-rack', 'rack']); // Not root as root only exist once
      expect(service.devices).toEqual(['hdd', 'ssd']);
    });

    it('has the following lists after selection of ssd-host', () => {
      controls.root.setValue(get.nodeByName('ssd-host'));
      assert.failureDomains(get.nodesByNames(['osd.0', 'osd.1', 'osd.2']), ['osd']); // Not host as it only exist once
      expect(service.devices).toEqual(['ssd']);
    });

    it('has the following lists after selection of mix-host', () => {
      controls.root.setValue(get.nodeByName('mix-host'));
      expect(service.devices).toEqual(['hdd', 'ssd']);
      assert.failureDomains(
        get.nodesByNames(['hdd-rack', 'ssd-rack', 'osd2.0', 'osd2.1', 'osd3.0', 'osd3.1']),
        ['osd-rack', 'rack']
      );
    });
  });

  describe('selection', () => {
    it('selects the first root after init automatically', () => {
      assert.formFieldValues(get.nodeByName('default'), 'osd-rack', '');
    });

    it('should select all values automatically by selecting "ssd-host" as root', () => {
      assert.valuesOnRootChange('ssd-host', 'osd', 'ssd');
    });

    it('selects automatically the most common failure domain', () => {
      // Select mix-host as mix-host has multiple failure domains (osd-rack and rack)
      assert.valuesOnRootChange('mix-host', 'osd-rack', '');
    });

    it('should override automatic selections', () => {
      assert.formFieldValues(get.nodeByName('default'), 'osd-rack', '');
      assert.valuesOnRootChange('ssd-host', 'osd', 'ssd');
      assert.valuesOnRootChange('mix-host', 'osd-rack', '');
    });

    it('should not override manual selections if possible', () => {
      controls.failure.setValue('rack');
      controls.failure.markAsDirty();
      controls.device.setValue('ssd');
      controls.device.markAsDirty();
      assert.valuesOnRootChange('mix-host', 'rack', 'ssd');
    });

    it('should preselect device by domain selection', () => {
      controls.failure.setValue('osd');
      assert.formFieldValues(get.nodeByName('default'), 'osd', 'ssd');
    });
  });

  describe('get available OSDs count', () => {
    it('should have 4 available OSDs with the default selection', () => {
      expect(service.deviceCount).toBe(4);
    });

    it('should reduce available OSDs to 2 if a device type is set', () => {
      controls.device.setValue('ssd');
      controls.device.markAsDirty();
      expect(service.deviceCount).toBe(2);
    });

    it('should show 3 OSDs when selecting "ssd-host"', () => {
      assert.valuesOnRootChange('ssd-host', 'osd', 'ssd');
      expect(service.deviceCount).toBe(3);
    });
  });
});
