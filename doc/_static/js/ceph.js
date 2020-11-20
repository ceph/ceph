$(function() {
  var releases_url = DOCUMENTATION_OPTIONS.URL_ROOT + '_static/releases.json';

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
    var pathname = window.location.pathname;
    var res = pathname.match(/en\/([a-z]+)\/?/i)
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
      if (url)  {
        url = url.replace("master", branch);
        $("#edit-on-github").attr("href", url);
      }
      $("#docubetter").show();
    }
  });
});
