$(function() {
  var releases_url = "/docs/master/releases.json";

  function is_eol(branch, data) {
    if (data && data.releases && branch in data.releases) {
      var eol = ("actual_eol" in data.releases[branch]);
      if (eol) {
        $("#eol-warning").show();
      }
      return !eol;
    }
    return false;
  }

  function get_branch() {
    var path = window.location.pathname;
    var res = path.match(/\/docs\/([a-z]+)\/?/i)
    if (res) {
      return res[1]
    }
    return null;
  }

  function show_releases_select(branch, data) {

    var select = $("#ceph-release-select");
    for (var release in data.releases) {
      var option = '<option value="' + release + '">' + release + '</option>';
      select.append(option);
    }
    select.append('<option value="master">master</option>');
    select.val(branch);

    select.change(function (){
      var tgt_branch = select.val();
      var pathname = window.location.pathname;

      var tgt_path = pathname.replace(/^\/docs\/([a-z]+)\/?/i, '/docs/' + tgt_branch + '/');

      window.location.pathname = tgt_path;
    });

    $("#ceph-releases").show()
  }

  $.getJSON(releases_url, function(data) {
    var branch = get_branch();

    if (branch === null) {
      $("#dev-warning").show();
      return;
    }
    if (branch === "master") {
      $("#dev-warning").show();
    }

    // show select regardless of eol release
    show_releases_select(branch, data);

    if (is_eol(branch, data)) {
      // patch the edit-on-github URL for correct branch
      var url = $("#edit-on-github").attr("href");
      url = url.replace("master", branch);
      $("#edit-on-github").attr("href", url);

      $("#docubetter").show();
    }

  });
});
