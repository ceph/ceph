/*!
 * Raphael Primitives Plugin 0.2
 *
 * Copyright (c) 2009 Dmitry Baranovskiy (http://raphaeljs.com)
 * Licensed under the MIT (http://www.opensource.org/licenses/mit-license.php) license.
 */

Raphael.fn.star = function (cx, cy, r, r2, rays) {
    r2 = r2 || r * .382;
    rays = rays || 5;
    var points = ["M", cx, cy + r2, "L"],
        R;
    for (var i = 1; i < rays * 2; i++) {
        R = i % 2 ? r : r2;
        points = points.concat([(cx + R * Math.sin(i * Math.PI / rays)), (cy + R * Math.cos(i * Math.PI / rays))]);
    }
    points.push("z");
    return this.path(points.join());
};
Raphael.fn.flower = function (cx, cy, rout, rin, n) {
    rin = rin || rout * .5;
    n = +n < 3 || !n ? 5 : n;
    var points = ["M", cx, cy + rin, "Q"],
        R;
    for (var i = 1; i < n * 2 + 1; i++) {
        R = i % 2 ? rout : rin;
        points = points.concat([+(cx + R * Math.sin(i * Math.PI / n)).toFixed(3), +(cy + R * Math.cos(i * Math.PI / n)).toFixed(3)]);
    }
    points.push("z");
    return this.path(points);
};
Raphael.fn.spike = function (cx, cy, rout, rin, n) {
    rin = rin || rout * .5;
    n = +n < 3 || !n ? 5 : n;
    var points = ["M", cx, cy - rout, "Q"],
        R;
    for (var i = 1; i < n * 2 + 1; i++) {
        R = i % 2 ? rin : rout;
        points = points.concat([cx + R * Math.sin(i * Math.PI / n - Math.PI), cy + R * Math.cos(i * Math.PI / n - Math.PI)]);
    }
    points.push("z");
    return this.path(points);
};
Raphael.fn.polyline = function () {
    var points = "M".concat(arguments[0] || 0, ",", arguments[1] || 0, "L");
    for (var i = 2, ii = arguments.length - 1; i < ii; i++) {
        points += arguments[i] + "," + arguments[++i];
    }
    arguments[ii].toLowerCase() == "z" && (points += "z");
    return this.path(points);
};
Raphael.fn.polygon = function (cx, cy, r, n) {
    n = +n < 3 || !n ? 5 : n;
    var points = ["M", cx, cy - r, "L"],
        R;
    for (var i = 1; i < n; i++) {
        points = points.concat([cx + r * Math.sin(i * Math.PI * 2 / n - Math.PI), cy + r * Math.cos(i * Math.PI * 2 / n - Math.PI)]);
    }
    points.push("z");
    return this.path(points);
};
Raphael.fn.line = function (x1, y1, x2, y2) {
    return this.path(["M", x1, y1, "L", x2, y2]);
};
Raphael.fn.drawGrid = function (x, y, w, h, wv, hv, color) {
    color = color || "#000";
    var path = ["M", x, y, "L", x + w, y, x + w, y + h, x, y + h, x, y],
        rowHeight = h / hv,
        columnWidth = w / wv;
    for (var i = 1; i < hv; i++) {
        path = path.concat(["M", x, y + i * rowHeight, "L", x + w, y + i * rowHeight]);
    }
    for (var i = 1; i < wv; i++) {
        path = path.concat(["M", x + i * columnWidth, y, "L", x + i * columnWidth, y + h]);
    }
    return this.path(path.join(",")).attr({stroke: color});
};
Raphael.fn.square = function (cx, cy, r) {
    r = r * .7;
    return this.rect(cx - r, cy - r, 2 * r, 2 * r);
};
Raphael.fn.triangle = function (cx, cy, r) {
    r *= 1.75;
    return this.path("M".concat(cx, ",", cy, "m0-", r * .58, "l", r * .5, ",", r * .87, "-", r, ",0z"));
};
Raphael.fn.diamond = function (cx, cy, r) {
    return this.path(["M", cx, cy - r, "l", r, r, -r, r, -r, -r, r, -r, "z"]);
};
Raphael.fn.cross = function (cx, cy, r) {
    r = r / 2.5;
    return this.path("M".concat(cx - r, ",", cy, "l", [-r, -r, r, -r, r, r, r, -r, r, r, -r, r, r, r, -r, r, -r, -r, -r, r, -r, -r, "z"]));
};
Raphael.fn.plus = function (cx, cy, r) {
    r = r / 2;
    return this.path("M".concat(cx - r / 2, ",", cy - r / 2, "l", [0, -r, r, 0, 0, r, r, 0, 0, r, -r, 0, 0, r, -r, 0, 0, -r, -r, 0, 0, -r, "z"]));
};
Raphael.fn.arrow = function (cx, cy, r) {
    return this.path("M".concat(cx - r * .7, ",", cy - r * .4, "l", [r * .6, 0, 0, -r * .4, r, r * .8, -r, r * .8, 0, -r * .4, -r * .6, 0], "z"));
};
