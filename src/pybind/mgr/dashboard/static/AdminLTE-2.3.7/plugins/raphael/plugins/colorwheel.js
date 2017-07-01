(function (Raphael) {
    Raphael.colorwheel = function (x, y, size, initcolor, element) {
        return new ColorWheel(x, y, size, initcolor, element);
    };
    var pi = Math.PI;
    function angle(x, y) {
        return (x < 0) * 180 + Math.atan(-y / -x) * 180 / pi;
    }
    var doc = document, win = window;
    var addEvent = (function () {
        if (doc.addEventListener) {
            return function (obj, type, fn, element) {
                var f = function (e) {
                    return fn.call(element, e);
                };
                obj.addEventListener(type, f, false);
                return function () {
                    obj.removeEventListener(type, f, false);
                    return true;
                };
            };
        } else if (doc.attachEvent) {
            return function (obj, type, fn, element) {
                var f = function (e) {
                    return fn.call(element, e || win.event);
                };
                obj.attachEvent("on" + type, f);
                var detacher = function () {
                    obj.detachEvent("on" + type, f);
                    return true;
                };
                return detacher;
            };
        }
    })();
    var ColorWheel = function (x, y, size, initcolor, element) {
        size = size || 200;
        var w3 = 3 * size / 200,
            w1 = size / 200,
            fi = 1.6180339887,
            segments = pi * size / 5,
            size20 = size / 20,
            size2 = size / 2,
            padding = 2 * size / 200,
            t = this;

        var H = 1, S = 1, B = 1, s = size - (size20 * 4);
        var r = element ? Raphael(element, size, size) : Raphael(x, y, size, size),
            xy = s / 6 + size20 * 2 + padding,
            wh = s * 2 / 3 - padding * 2;
        w1 < 1 && (w1 = 1);
        w3 < 1 && (w3 = 1);


        // ring drawing
        var a = pi / 2 - pi * 2 / segments * 1.3,
            R = size2 - padding,
            R2 = size2 - padding - size20 * 2,
            path = ["M", size2, padding, "A", R, R, 0, 0, 1, R * Math.cos(a) + R + padding, R - R * Math.sin(a) + padding, "L", R2 * Math.cos(a) + R + padding, R - R2 * Math.sin(a) + padding, "A", R2, R2, 0, 0, 0, size2, padding + size20 * 2, "z"].join();
        for (var i = 0; i < segments; i++) {
            r.path(path).attr({
                stroke: "none",
                fill: "hsb(" + i * (255 / segments) + ", 255, 200)",
                rotation: [(360 / segments) * i, size2, size2]
            });
        }
        r.path(["M", size2, padding, "A", R, R, 0, 1, 1, size2 - 1, padding, "l1,0", "M", size2, padding + size20 * 2, "A", R2, R2, 0, 1, 1, size2 - 1, padding + size20 * 2, "l1,0"]).attr({
            "stroke-width": w3,
            stroke: "#fff"
        });
        t.cursorhsb = r.set();
        var h = size20 * 2 + 2;
        t.cursorhsb.push(r.rect(size2 - h / fi / 2, padding - 1, h / fi, h, 3 * size / 200).attr({
            stroke: "#000",
            opacity: .5,
            "stroke-width": w3
        }));
        t.cursorhsb.push(t.cursorhsb[0].clone().attr({
            stroke: "#fff",
            opacity: 1,
            "stroke-width": w1
        }));
        t.ring = r.path(["M", size2, padding, "A", R, R, 0, 1, 1, size2 - 1, padding, "l1,0M", size2, padding + size20 * 2, "A", R2, R2, 0, 1, 1, size2 - 1, padding + size20 * 2, "l1,0"]).attr({
            fill: "#000",
            opacity: 0,
            stroke: "none"
        });

        // rect drawing
        t.main = r.rect(xy, xy, wh, wh).attr({
            stroke: "none",
            fill: "#f00",
            opacity: 1
        });
        t.main.clone().attr({
            stroke: "none",
            fill: "0-#fff-#fff",
            opacity: 0
        });
        t.square = r.rect(xy - 1, xy - 1, wh + 2, wh + 2).attr({
            r: 2,
            stroke: "#fff",
            "stroke-width": w3,
            fill: "90-#000-#000",
            opacity: 0,
            cursor: "crosshair"
        });
        t.cursor = r.set();
        t.cursor.push(r.circle(size2, size2, size20 / 2).attr({
            stroke: "#000",
            opacity: .5,
            "stroke-width": w3
        }));
        t.cursor.push(t.cursor[0].clone().attr({
            stroke: "#fff",
            opacity: 1,
            "stroke-width": w1
        }));
        t.H = t.S = t.B = 1;
        t.raphael = r;
        t.size2 = size2;
        t.wh = wh;
        t.x = x;
        t.xy = xy;
        t.y = y;

        // events
        t.hsbon = addEvent(t.ring.node, "mousedown", function (e) {
            this.hsbOnTheMove = true;
            this.setH(e.clientX - this.x - this.size2, e.clientY - this.y - this.size2);
            this.docmove = addEvent(doc, "mousemove", this.docOnMove, this);
            this.docup = addEvent(doc, "mouseup", this.docOnUp, this);
        }, t);
        t.clron = addEvent(t.square.node, "mousedown", function (e) {
            this.clrOnTheMove = true;
            this.setSB(e.clientX - this.x, e.clientY - this.y);
            this.docmove = addEvent(doc, "mousemove", this.docOnMove, this);
            this.docup = addEvent(doc, "mouseup", this.docOnUp, this);
        }, t);
        t.winunload = addEvent(win, "unload", function () {
            this.hsbon();
            this.clron();
            this.docmove && this.docmove();
            this.docup && this.docup();
            this.winunload();
        }, t);
        
        t.color(initcolor || "#f00");
        this.onchanged && this.onchanged(this.color());
    };
    ColorWheel.prototype.setH = function (x, y) {
        var d = angle(x, y),
            rd = d * pi / 180;
        this.cursorhsb.rotate(d + 90, this.size2, this.size2);
        this.H = (d + 90) / 360;
        this.main.attr({fill: "hsb(" + this.H + ",1,1)"});
        this.onchange && this.onchange(this.color());
    };
    ColorWheel.prototype.setSB = function (x, y) {
        x < this.size2 - this.wh / 2 && (x = this.size2 - this.wh / 2);
        x > this.size2 + this.wh / 2 && (x = this.size2 + this.wh / 2);
        y < this.size2 - this.wh / 2 && (y = this.size2 - this.wh / 2);
        y > this.size2 + this.wh / 2 && (y = this.size2 + this.wh / 2);
        this.cursor.attr({cx: x, cy: y});
        this.B = 1 - (y - this.xy) / this.wh;
        this.S = (x - this.xy) / this.wh;
        this.onchange && this.onchange(this.color());
    };
    ColorWheel.prototype.docOnMove = function (e) {
        if (this.hsbOnTheMove) {
            this.setH(e.clientX - this.x - this.size2, e.clientY - this.y - this.size2);
        }
        if (this.clrOnTheMove) {
            this.setSB(e.clientX - this.x, e.clientY - this.y);
        }
        e.preventDefault && e.preventDefault();
        e.returnValue = false;
        return false;
    };
    ColorWheel.prototype.docOnUp = function (e) {
        this.hsbOnTheMove = this.clrOnTheMove = false;
        this.docmove();
        delete this.docmove;
        this.docup();
        delete this.docup;
        this.onchanged && this.onchanged(this.color());
    };
    ColorWheel.prototype.remove = function () {
        this.raphael.remove();
        this.color = function () {
            return false;
        };
    };
    ColorWheel.prototype.color = function (color) {
        if (color) {
            color = Raphael.getRGB(color);
            color = Raphael.rgb2hsb(color.r, color.g, color.b);
            var d = color.h * 360;
            this.H = color.h;
            this.S = color.s;
            this.B = color.b;
            this.cursorhsb.rotate(d, this.size2, this.size2);
            this.main.attr({fill: "hsb(" + this.H + ",1,1)"});
            var x = this.S * this.wh + this.xy,
                y = (1 - this.B) * this.wh + this.xy;
            this.cursor.attr({cx: x, cy: y});
            return this;
        } else {
            return Raphael.hsb2rgb(this.H, this.S, this.B).hex;
        }
    };
})(window.Raphael);
