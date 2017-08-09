/*!
 * Raphael Shadow plugin 0.3
 *
 * Copyright (c) 2008 - 2009 Dmitry Baranovskiy (http://raphaeljs.com)
 * Licensed under the MIT (http://www.opensource.org/licenses/mit-license.php) license.
 */
Raphael.shadow = function (x, y, w, h, options) {
    // options format: {
    //     size: 0..1, shadow size
    //     color: "#000", placeholder colour
    //     stroke: "#000", placeholder stroke colour
    //     shadow: "#000", shadow colour
    //     target: "someID" | htmlElement
    //     r: 5, radius of placeholder rounded corners
    // }
    options = options || {};
    var t = ~~(size * .3 + .5),
        size = (options.size || 1) * 10,
        color = options.color || "#fff",
        stroke = options.stroke || color,
        shadowColor = options.shadow || "#000",
        target = options.target || null,
        R = options.r == null ? 3 : options.r,
        s = size,
        b = size * 2,
        r = b + s,
        rg = this.format("r{0}-{0}", shadowColor),
        rect = "rect",
        none = "none",
        res,
        set;

        if (target) {
            res = this(target, w + (x = s) * 2, h + (y = t) + b);
        } else {
            res = this(x - s, y - t, w + (x = s) * 2, h + (y = t) + b);
        }

        set = res.set(
            res.rect(x - s, y - t, b + s, h + y + b).attr({stroke: none, fill: this.format("180-{0}-{0}", shadowColor), opacity: 0, "clip-rect": [x - s + 1, y - t + r, b, h + y + b - r * 2 + .9]}),
            res.rect(x + w - b, y - t, b + s, h + y + b).attr({stroke: none, fill: this.format("0-{0}-{0}", shadowColor), opacity: 0, "clip-rect": [x + w - s + 1, y - t + r, b, h + y + b - r * 2]}),
            res.rect(x + b - 1, y + h - s, w + b, b + s).attr({stroke: none, fill: this.format("270-{0}-{0}", shadowColor), opacity: 0, "clip-rect": [x + b, y + h - s, w + b - r * 2, b + s]}),
            res.rect(x + s - 1, y - t, w + b, b + s).attr({stroke: none, fill: this.format("90-{0}-{0}", shadowColor), opacity: 0, "clip-rect": [x + b, y - t, w + b - r * 2, s + t + 1]}),
            res.circle(x + b, y + h - s, r).attr({stroke: none, fill: rg, opacity: 0, "clip-rect": [x - s, y + h - s + .999, r, r]}),
            res.circle(x + w - b, y + h - s, r).attr({stroke: none, fill: rg, opacity: 0, "clip-rect": [x + w - b, y + h - s, r, r]}),
            res.circle(x + b, y - t + r, r).attr({stroke: none, fill: rg, opacity: 0, "clip-rect": [x - s, y - t, r, r]}),
            res.circle(x + w - b, y - t + r, r).attr({stroke: none, fill: rg, opacity: 0, "clip-rect": [x + w - b, y - t, r, r]}),
            res.rect(x, y, w, h, R).attr({fill: color, stroke: stroke})
        );

    return set[0].paper;
};
