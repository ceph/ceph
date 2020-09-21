import { ConfigFormModel } from './config-option.model';
import { ConfigOptionTypes } from './config-option.types';

describe('ConfigOptionTypes', () => {
  describe('getType', () => {
    it('should return uint type', () => {
      const ret = ConfigOptionTypes.getType('uint');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('uint');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Unsigned integer value');
      expect(ret.defaultMin).toBe(0);
      expect(ret.patternHelpText).toBe('The entered value needs to be an unsigned number.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(false);
    });

    it('should return int type', () => {
      const ret = ConfigOptionTypes.getType('int');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('int');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Integer value');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe('The entered value needs to be a number.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(true);
    });

    it('should return size type', () => {
      const ret = ConfigOptionTypes.getType('size');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('size');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Unsigned integer value (>=16bit)');
      expect(ret.defaultMin).toBe(0);
      expect(ret.patternHelpText).toBe('The entered value needs to be a unsigned number.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(false);
    });

    it('should return secs type', () => {
      const ret = ConfigOptionTypes.getType('secs');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('secs');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Number of seconds');
      expect(ret.defaultMin).toBe(1);
      expect(ret.patternHelpText).toBe('The entered value needs to be a number >= 1.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(false);
    });

    it('should return float type', () => {
      const ret = ConfigOptionTypes.getType('float');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('float');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Double value');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe('The entered value needs to be a number or decimal.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(true);
    });

    it('should return str type', () => {
      const ret = ConfigOptionTypes.getType('str');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('str');
      expect(ret.inputType).toBe('text');
      expect(ret.humanReadable).toBe('Text');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBeUndefined();
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should return addr type', () => {
      const ret = ConfigOptionTypes.getType('addr');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('addr');
      expect(ret.inputType).toBe('text');
      expect(ret.humanReadable).toBe('IPv4 or IPv6 address');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe('The entered value needs to be a valid IP address.');
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should return uuid type', () => {
      const ret = ConfigOptionTypes.getType('uuid');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('uuid');
      expect(ret.inputType).toBe('text');
      expect(ret.humanReadable).toBe('UUID');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe(
        'The entered value is not a valid UUID, e.g.: 67dcac9f-2c03-4d6c-b7bd-1210b3a259a8'
      );
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should return bool type', () => {
      const ret = ConfigOptionTypes.getType('bool');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('bool');
      expect(ret.inputType).toBe('checkbox');
      expect(ret.humanReadable).toBe('Boolean value');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBeUndefined();
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should throw an error for unknown type', () => {
      expect(() => ConfigOptionTypes.getType('unknown')).toThrowError(
        'Found unknown type "unknown" for config option.'
      );
    });
  });

  describe('getTypeValidators', () => {
    it('should return two validators for type uint, secs and size', () => {
      const types = ['uint', 'size', 'secs'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = ConfigOptionTypes.getTypeValidators(configOption);
        expect(ret).toBeTruthy();
        expect(ret.validators.length).toBe(2);
      });
    });

    it('should return a validator for types float, int, addr and uuid', () => {
      const types = ['float', 'int', 'addr', 'uuid'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = ConfigOptionTypes.getTypeValidators(configOption);
        expect(ret).toBeTruthy();
        expect(ret.validators.length).toBe(1);
      });
    });

    it('should return undefined for type bool and str', () => {
      const types = ['str', 'bool'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = ConfigOptionTypes.getTypeValidators(configOption);
        expect(ret).toBeUndefined();
      });
    });

    it('should return a pattern and a min validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int';
      configOption.min = 2;

      const ret = ConfigOptionTypes.getTypeValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.validators.length).toBe(2);
      expect(ret.min).toBe(2);
      expect(ret.max).toBeUndefined();
    });

    it('should return a pattern and a max validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int';
      configOption.max = 5;

      const ret = ConfigOptionTypes.getTypeValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.validators.length).toBe(2);
      expect(ret.min).toBeUndefined();
      expect(ret.max).toBe(5);
    });

    it('should return multiple validators', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'float';
      configOption.max = 5.2;
      configOption.min = 1.5;

      const ret = ConfigOptionTypes.getTypeValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.validators.length).toBe(3);
      expect(ret.min).toBe(1.5);
      expect(ret.max).toBe(5.2);
    });

    it(
      'should return a pattern help text for type uint, int, size, secs, ' + 'float, addr and uuid',
      () => {
        const types = ['uint', 'int', 'size', 'secs', 'float', 'addr', 'uuid'];

        types.forEach((valType) => {
          const configOption = new ConfigFormModel();
          configOption.type = valType;

          const ret = ConfigOptionTypes.getTypeValidators(configOption);
          expect(ret).toBeTruthy();
          expect(ret.patternHelpText).toBeDefined();
        });
      }
    );
  });

  describe('getTypeStep', () => {
    it('should return the correct step for type uint and value 0', () => {
      const ret = ConfigOptionTypes.getTypeStep('uint', 0);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type int and value 1', () => {
      const ret = ConfigOptionTypes.getTypeStep('int', 1);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type int and value null', () => {
      const ret = ConfigOptionTypes.getTypeStep('int', null);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type size and value 2', () => {
      const ret = ConfigOptionTypes.getTypeStep('size', 2);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type secs and value 3', () => {
      const ret = ConfigOptionTypes.getTypeStep('secs', 3);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type float and value 1', () => {
      const ret = ConfigOptionTypes.getTypeStep('float', 1);
      expect(ret).toBe(0.1);
    });

    it('should return the correct step for type float and value 0.1', () => {
      const ret = ConfigOptionTypes.getTypeStep('float', 0.1);
      expect(ret).toBe(0.1);
    });

    it('should return the correct step for type float and value 0.02', () => {
      const ret = ConfigOptionTypes.getTypeStep('float', 0.02);
      expect(ret).toBe(0.01);
    });

    it('should return the correct step for type float and value 0.003', () => {
      const ret = ConfigOptionTypes.getTypeStep('float', 0.003);
      expect(ret).toBe(0.001);
    });

    it('should return the correct step for type float and value null', () => {
      const ret = ConfigOptionTypes.getTypeStep('float', null);
      expect(ret).toBe(0.1);
    });

    it('should return undefined for unknown type', () => {
      const ret = ConfigOptionTypes.getTypeStep('unknown', 1);
      expect(ret).toBeUndefined();
    });
  });
});
