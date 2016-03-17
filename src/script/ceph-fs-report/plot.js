$(function() {
    ages = []
    ages.push({
	data: ages_over_time,
        label: "rados and RBD pull requests",
    });
    $.plot("#ages", ages, {
	xaxis: {
	    mode: "time",
	},
    });

    $.plot("#closers", closers, {
	xaxis: {
	    mode: "time",
	},
    });
});
