$(function() {
    $.plot("#encode", [
        {
	    data: encode_cauchy_good_4096,
            label: "Cauchy 4KB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_cauchy_good_1048576,
            label: "Cauchy 1MB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_reed_sol_van_4096,
            label: "Reed Solomon 4KB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_reed_sol_van_1048576,
            label: "Reed Solomon 1MB",
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
	    data: decode_cauchy_good_4096,
            label: "Cauchy 4KB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_cauchy_good_1048576,
            label: "Cauchy 1MB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_reed_sol_van_4096,
            label: "Reed Solomon 4KB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_reed_sol_van_1048576,
            label: "Reed Solomon 1MB",
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

    $.plot("#encode4KB", [
        {
	    data: encode_cauchy_good_4096,
            label: "Cauchy 4KB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_reed_sol_van_4096,
            label: "Reed Solomon 4KB",
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

    $.plot("#decode4KB", [
        {
	    data: decode_cauchy_good_4096,
            label: "Cauchy 4KB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_reed_sol_van_4096,
            label: "Reed Solomon 4KB",
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

    $.plot("#encode1MB", [
        {
	    data: encode_cauchy_good_1048576,
            label: "Cauchy 1MB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: encode_reed_sol_van_1048576,
            label: "Reed Solomon 1MB",
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

    $.plot("#decode1MB", [
        {
	    data: decode_cauchy_good_1048576,
            label: "Cauchy 1MB",
	    points: { show: true },
	    lines: { show: true },
	},
        {
	    data: decode_reed_sol_van_1048576,
            label: "Reed Solomon 1MB",
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
