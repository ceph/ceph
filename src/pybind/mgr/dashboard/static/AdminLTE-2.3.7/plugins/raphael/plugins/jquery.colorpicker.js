/*!
 * Color Picker 0.1.0 - Raphael plugin
 *
 * Copyright (c) 2010 Dmitry Baranovskiy (http://raphaeljs.com)
 * Based on Color Wheel (http://jweir.github.com/colorwheel) by John Weir (http://famedriver.com)
 * Licensed under the MIT (http://www.opensource.org/licenses/mit-license.php) license.
 */
(function ($, R) {
    $.fn.colorpicker = function (size, initcolor) {
        if (R) {
            var offset = this.offset();
            return R.colorpicker(offset.left, offset.top, size, initcolor, this[0]);
        }
        return null;
    };
})(window.jQuery, window.Raphael);