$(function() {
  var releases_url = "http://docs.ceph.com/docs/master/releases.json";

  function show_edit(branch, data) {
    if (branch) {
      if (branch === "master") {
        $("#dev-warning").show();
        return true;
      }
      if (data && data.releases && branch in data.releases) {
        var eol = ("actual_eol" in data.releases[branch]);
        if (eol) {
          $("#eol-warning").show();
        }
        return !eol;
      }
    }
    $("#dev-warning").show();
    return false;
  }

  function get_branch() {
    var url = window.location.href;
    var res = url.match(/docs.ceph.com\/docs\/([a-z]+)\/?/i)
    if (res) {
      return res[1]
    }
    return null;
  }

  $.getJSON(releases_url, function(data) {
    var branch = get_branch();
    if (show_edit(branch, data)) {
      // patch the edit-on-github URL for correct branch
      var url = $("#edit-on-github").attr("href");
      url = url.replace("master", branch);
      $("#edit-on-github").attr("href", url);
      $("#docubetter").show();
    }
  });
});
