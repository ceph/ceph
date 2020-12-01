$(function() {
  var releases_url = DOCUMENTATION_OPTIONS.URL_ROOT + '_static/releases.json';

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
    if (branch === "master") {
      $("#dev-warning").show();
    } else if (data && data.releases && branch in data.releases) {
      return;
    } else {
      $("#dev-warning").show();
    }
  });
});
