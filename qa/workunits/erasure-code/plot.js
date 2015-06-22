$(function() {
    $.plot("#encode", [
        {
	    data: encode_vandermonde_isa,
            label: "ISA, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_vandermonde_jerasure_generic,
            label: "Jerasure Generic, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_vandermonde_jerasure_sse4,
            label: "Jerasure SIMD, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_cauchy_isa,
            label: "ISA, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_cauchy_jerasure_generic,
            label: "Jerasure, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	},
    ], {
	xaxis: {
	    mode: "categories",
	    tickLength: 0
	},
    }
          );

    $.plot("#decode", [
        {
	    data: decode_vandermonde_isa,
            label: "ISA, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_vandermonde_jerasure_generic,
            label: "Jerasure Generic, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_vandermonde_jerasure_sse4,
            label: "Jerasure SIMD, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_cauchy_isa,
            label: "ISA, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_cauchy_jerasure_generic,
            label: "Jerasure, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	},
    ], {
	xaxis: {
	    mode: "categories",
	    tickLength: 0
	},
    }
          );

});
