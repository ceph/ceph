$(function() {
  var releases_url = "http://docs.ceph.com/docs/master/releases.json";
  //var releases_url = "/releases.json";

  function is_eol(branch, data) {
    if (data && data.releases && branch in data.releases) {
      return ("actual_eol" in data.releases[branch]);
    }
    return false;
  }

  function get_branch() {
    var path = window.location.pathname;
    var res = path.match(/^\/docs\/([a-z]+)\/?/i)
    if (res) {
      return res[1]
    }
    return null;
  }

  function show_releases_select(branch, data) {

    // Sort the releases according the last release
    var releases = [];

    for(var release in data.releases) {
      if (is_eol(release, data)) {
        continue;
      }
      // try to avoid modern JS: https://stackoverflow.com/a/36411645
      if(data.releases.hasOwnProperty(release)) {
        releases.push({
          release_name: release,
          released: data.releases[release].releases[0].released,
        });
      }
    }

    // RTFM: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/sort
    releases.sort(function (a, b) {
      if (a.released > b.released) { // newer release come first
        return -1;
      }
      if (a.released < b.released) {
        return 1;
      }
      return 0;
    });

    releases.unshift({
      release_name: "master",
      released: 0
    })

    var select = $("#ceph-release-select");

    var found = false;
    select.append('<option selected disabled>Select branch</option>');
    for (var i = 0; i < releases.length; i++) {
      var release = releases[i].release_name;
      var option = '<option value="' + release + '">' + release + '</option>';
      select.append(option);
      if (branch === release) {
        found = true;
      }
    }
    if (found) {
      // choose the current release
      select.val(branch);
    }

    select.change(function (){
      var tgt_branch = select.val();
      var pathname = window.location.pathname;

      var tgt_path = pathname.replace(/^\/docs\/([a-z]+)\/?/i, '/docs/' + tgt_branch + '/');

      console.log(tgt_path)

      window.location.pathname = tgt_path;
    });

    $("#ceph-releases").show()
  }

  $.getJSON(releases_url, function(data) {
    var branch = get_branch();

    show_releases_select(branch, data);

    if (branch === null) {
      $("#dev-warning").show();
    }

    if (branch === "master") {
      $("#dev-warning").show();
    }

    if (is_eol(branch, data)) {
      $("#eol-warning").show();
    } else {
      // patch the edit-on-github URL for correct branch
      var url = $("#edit-on-github").attr("href");
      url = url.replace("master", branch);
      $("#edit-on-github").attr("href", url);

      $("#docubetter").show();
    }
  });
});
