Raphael(function () {
    
    // TOC
    (function (ol) {
        if (!ol) {
            return;
        }
        var li = document.createElement("li"),
            isABBR = function (str, abbr) {
                var letters = abbr.toUpperCase().split(""),
                    first = letters.shift(),
                    rg = new RegExp("^[" + first.toLowerCase() + first + "][a-z]*" + letters.join("[a-z]*") + "[a-z]*$");
                return !!String(str).match(rg);
            },
            score = function (me, search) {
                me = String(me);
                search = String(search);
                var score = 0,
                    chunk;
                if (me == search) {
                    return 1;
                }
                if (!me || !search) {
                    return 0;
                }
                if (isABBR(me, search)) {
                    return .9;
                }
                score = 0;
                chunk = me.toLowerCase();
                for (var j, i = 0, ii = search.length; i < ii; i++) {
                    j = chunk.indexOf(search.charAt(i));
                    if (~j) {
                        chunk = chunk.substring(j + 1);
                        score += 1 / (j + 1);
                    }
                }
                score = Math.max(score / ii - Math.abs(me.length - ii) / me.length / 2, 0);
                return score;
            };
        li.innerHTML = '<input type="search" id="dr-filter" results="0">';
        var lis = ol.getElementsByTagName("span"),
            names = [],
            rgName = /[^\.\(]*(?=(\(\))?$)/;
        for (var i = 0, ii = lis.length; i < ii; i++) {
            names[i] = {
                li: lis[i].parentNode.parentNode,
                text: lis[i].innerHTML.match(rgName)[0]
            };
        }
        ol.insertBefore(li, ol.firstChild);
        var input = document.getElementById("dr-filter");
        input.style.width = "100%";
        input.style.marginTop = "10px";
        input.onclick = input.onchange = input.onkeydown = input.onkeyup = function () {
            var v = input.value,
                res = [];
            if (v.length > 1) {
                for (var i = 0, ii = names.length; i < ii; i++) {
                    res[i] = {
                        li: names[i].li,
                        weight: score(names[i].text, v)
                    };
                }
                res.sort(function (a, b) {
                    return b.weight - a.weight;
                });
                for (i = 0, ii = res.length; i < ii; i++) {
                    ol.appendChild(res[i].li);
                }
            } else {
                for (i = 0, ii = names.length; i < ii; i++) {
                    ol.appendChild(names[i].li);
                }
            }
        };
    })(document.getElementById("dr-toc"));
    
    function prepare(id) {
        var div = document.getElementById(id);
        div.style.cssText = "float:right;padding:10px;width:99px;height:99px;background:#2C53B0 url(http://raphaeljs.com/blueprint-min.png) no-repeat";
        return Raphael(div, 99, 99);
    }
    
    var line = {
            stroke: "#fff",
            "stroke-width": 2,
            "stroke-linecap": "round",
            "stroke-linejoin": "round"
        },
        dots = {
            stroke: "#fff",
            "stroke-width": 2,
            "stroke-dasharray": "- ",
            "stroke-linecap": "round",
            "stroke-linejoin": "round"
        },
        fill = {
            stroke: "#fff",
            fill: "#fff",
            "fill-opacity": .5,
            "stroke-width": 2,
            "stroke-linecap": "round",
            "stroke-linejoin": "round"
        },
        none = {
            fill: "#000",
            opacity: 0
        };
    prepare("Paper.path-extra").path("M10,10R90,50 10,90").attr(line);
    
    (function (r) {
        var there;
        r.circle(15, 15, 10).attr(fill).click(function () {
            var clr = Raphael.hsb(Math.random(), .6, 1);
            this.animate(there ? {
                cx: 15,
                cy: 15,
                r: 10,
                stroke: "#fff",
                fill: "#fff"
            } : {
                cx: 80,
                cy: 80,
                r: 15,
                stroke: clr,
                fill: clr
            }, 600, ["bounce", "<>", "elastic", ""][Math.round(Math.random() * 3)]);
            there = !there;
        });
    })(prepare("Element.animate-extra"));

    (function (r) {
        var x, y;
        r.circle(15, 15, 10).attr(fill).drag(function (dx, dy) {
            this.attr({
                cx: Math.min(Math.max(x + dx, 15), 85),
                cy: Math.min(Math.max(y + dy, 15), 85)
            });
        }, function () {
            x = this.attr("cx");
            y = this.attr("cy");
        });
        
    })(prepare("Element.drag-extra"));

    (function (r) {
        var e = r.ellipse(50, 50, 40, 30).attr(fill).click(function () {
                this.animate({
                    transform: "r180"
                }, 1000, function () {
                    this.attr({
                        transform: ""
                    });
                });
            }),
            bb = r.rect().attr(e.getBBox()).attr(dots);
        eve.on("anim.frame." + e.id, function (anim) {
            bb.attr(e.getBBox());
        });
    })(prepare("Element.getBBox-extra"));

    (function (r) {
        var e = r.path("M10,10R20,70 30,40 40,80 50,10 60,50 70,20 80,30 90,90").attr(line),
            l = e.getTotalLength(),
            to = 1;
        r.customAttributes.along = function (a) {
            var p = e.getPointAtLength(a * l);
            return {
                transform: "t" + [p.x, p.y] + "r" + p.alpha
            };
        };
        var c = r.ellipse(0, 0, 5, 2).attr({
            along: 0
        }).attr(line);
        r.rect(0, 0, 100, 100).attr(none).click(function () {
            c.stop().animate({
                along: to
            }, 5000);
            to = +!to;
        });
    })(prepare("Element.getPointAtLength-extra"));

    (function (r) {
        var e = r.path("M10,10R20,70 30,40 40,80 50,10 60,50 70,20 80,30 90,90").attr(line),
            l = e.getTotalLength() - 10,
            to = 1;
        r.customAttributes.along = function (a) {
            return {
                path: e.getSubpath(a * l, a * l + 11)
            };
        };
        var c = r.path().attr(line).attr({
            along: 0,
            stroke: "#f00",
            "stroke-width": 3
        });
        r.rect(0, 0, 100, 100).attr(none).click(function () {
            c.stop().animate({
                along: to
            }, 5000);
            to = +!to;
        });
    })(prepare("Element.getSubpath-extra"));

    (function (r) {
        r.circle(50, 50, 40).attr(line).glow({color: "#fff"});
    })(prepare("Element.glow-extra"));

    (function (r) {
        r.rect(10, 10, 40, 30).attr(dots);
        r.rect(10, 10, 40, 30).attr(line).transform("r-30, 50, 10t10, 20s1.5");
        r.text(50, 90, "r-30, 50, 10\nt10, 20s1.5").attr({fill: "#fff"});
    })(prepare("Element.transform-extra"));

    (function (r) {
        r.circle(50, 50, 40).attr(line);
    })(prepare("Paper.circle-extra"));

    (function (r) {
        r.ellipse(50, 50, 40, 30).attr(line);
    })(prepare("Paper.ellipse-extra"));

    (function (r) {
        r.rect(10, 10, 50, 50).attr(line);
        r.rect(40, 40, 50, 50, 10).attr(line);
    })(prepare("Paper.rect-extra"));

    (function (r) {
        var set = r.set(
            r.rect(10, 10, 50, 50).attr(fill),
            r.rect(40, 40, 50, 50, 10).attr(fill)
        ).hover(function () {
            set.stop().animate({stroke: "#f00"}, 600, "<>");
        }, function () {
            set.stop().animate({stroke: "#fff"}, 600, "<>");
        });
    })(prepare("Paper.set-extra"));

    (function (r) {
        r.text(50, 50, "Raphaël\nkicks\nbutt!").attr({
            fill: "#fff",
            font: "italic 20px Georgia",
            transform: "r-10"
        });
    })(prepare("Paper.text-extra"));

});