$(function() {
    encode = [];
    if (typeof encode_vandermonde_isa != 'undefined') {
        encode.push({
	    data: encode_vandermonde_isa,
            label: "ISA, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof encode_vandermonde_jerasure_generic != 'undefined') {
        encode.push({
	    data: encode_vandermonde_jerasure_generic,
            label: "Jerasure Generic, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof encode_vandermonde_jerasure_sse4 != 'undefined') {
        encode.push({
	    data: encode_vandermonde_jerasure_sse4,
            label: "Jerasure SIMD, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof encode_cauchy_isa != 'undefined') {
        encode.push({
	    data: encode_cauchy_isa,
            label: "ISA, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof encode_cauchy_jerasure_generic != 'undefined') {
        encode.push({
	    data: encode_cauchy_jerasure_generic,
            label: "Jerasure, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    $.plot("#encode", encode, {
	xaxis: {
	    mode: "categories",
	    tickLength: 0
	},
    });

    decode = [];
    if (typeof decode_vandermonde_isa != 'undefined') {
        decode.push({
	    data: decode_vandermonde_isa,
            label: "ISA, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof decode_vandermonde_jerasure_generic != 'undefined') {
        decode.push({
	    data: decode_vandermonde_jerasure_generic,
            label: "Jerasure Generic, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof decode_vandermonde_jerasure_sse4 != 'undefined') {
        decode.push({
	    data: decode_vandermonde_jerasure_sse4,
            label: "Jerasure SIMD, Vandermonde",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof decode_cauchy_isa != 'undefined') {
        decode.push({
	    data: decode_cauchy_isa,
            label: "ISA, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    if (typeof decode_cauchy_jerasure_generic != 'undefined') {
        decode.push({
	    data: decode_cauchy_jerasure_generic,
            label: "Jerasure, Cauchy",
	    points: { show: true },
	    lines: { show: true },
	});
    }
    $.plot("#decode", decode, {
	xaxis: {
	    mode: "categories",
	    tickLength: 0
	},
    });

});
