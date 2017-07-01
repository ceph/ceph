(function ($, R) {
    $.fn.colorwheel = function (size, initcolor) {
        if (R) {
            var offset = this.offset();
            return R.colorwheel(offset.left, offset.top, size, initcolor, this[0]);
        }
        return null;
    };
})(window.jQuery, window.Raphael);