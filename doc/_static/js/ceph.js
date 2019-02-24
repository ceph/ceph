$(function() {
  var releases_url = "/docs/master/releases.json";

  function is_eol(branch, data) {
    if (data && data.releases && branch in data.releases) {
      var eol = ("actual_eol" in data.releases[branch]);
      if (eol) {
        $("#eol-warning").show();
      }
      return eol;
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
      // try to avoid modern JS: https://stackoverflow.com/a/36411645
      if(data.releases.hasOwnProperty(release)) {
        releases.push({
          release_name: release,
          released: data.releases[release].releases[0].released,
        });
      }
    }

    releases.sort(function (a, b) {
      return a.released < b.released;
    });

    var select = $("#ceph-release-select");

    select.append('<option value="master">master</option>');
    for (var i = 0; i < releases.length; i++) {
      var release = releases[i].release_name;
      var option = '<option value="' + release + '">' + release + '</option>';
      select.append(option);
    }
    // choose the current release
    select.val(branch);

    select.change(function (){
      var tgt_branch = select.val();
      var pathname = window.location.pathname;

      var tgt_path = pathname.replace(/^\/docs\/([a-z]+)\/?/i, '/docs/' + tgt_branch + '/');

      window.location.pathname = tgt_path;
    });

    $("#ceph-releases").show()
  }

  // show horizontal color line to easy distinguish the releases
  // even in the middle of the page
  function draw_release_line(branch) {
    var color_map = {
      // colors are got from https://github.com/d3/d3-scale-chromatic#schemeSet3
      "dumpling": "#8dd3c7",
      "emperor": "#ffffb3",
      "firefly": "#bebada",
      "giant": "#fb8072",
      "hammer": "#80b1d3",
      "infernalis": "#fdb462",
      "jewel": "#b3de69",
      "kraken": "#fccde5",
      "luminous": "#d9d9d9",
      "mimic": "#bc80bd",
      // free colors:
      // "": "#ccebc5",
      // "": "#ffed6f",
      // development documentation is always red to attract attention
      "master": "#FF0000",
    };
    var color = (branch in color_map) ? color_map[branch] : "#000";
    $(".body").css("border-left", "5px solid " + color);
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

    // show select and color line regardless of eol release
    show_releases_select(branch, data);
    draw_release_line(branch);

    if (!is_eol(branch, data)) {
      // patch the edit-on-github URL for correct branch
      var url = $("#edit-on-github").attr("href");
      url = url.replace("master", branch);
      $("#edit-on-github").attr("href", url);

      $("#docubetter").show();
    }

  });
});
