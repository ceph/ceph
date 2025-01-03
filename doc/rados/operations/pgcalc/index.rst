.. _pgcalc:

  
=======
PG Calc
=======


.. raw:: html


   <link rel="stylesheet" id="wp-job-manager-job-listings-css" href="https://web.archive.org/web/20230614135557cs_/https://old.ceph.com/wp-content/plugins/wp-job-manager/assets/dist/css/job-listings.css" type="text/css" media="all"/>
   <link rel="stylesheet" id="ceph/googlefont-css" href="https://web.archive.org/web/20230614135557cs_/https://fonts.googleapis.com/css?family=Raleway%3A300%2C400%2C700&amp;ver=5.7.2" type="text/css" media="all"/>
   <link rel="stylesheet" id="Stylesheet-css" href="https://web.archive.org/web/20230614135557cs_/https://old.ceph.com/wp-content/themes/cephTheme/Resources/Styles/style.min.css" type="text/css" media="all"/>
   <link rel="stylesheet" id="tablepress-default-css" href="https://web.archive.org/web/20230614135557cs_/https://old.ceph.com/wp-content/plugins/tablepress/css/default.min.css" type="text/css" media="all"/>
   <link rel="stylesheet" id="jetpack_css-css" href="https://web.archive.org/web/20230614135557cs_/https://old.ceph.com/wp-content/plugins/jetpack/css/jetpack.css" type="text/css" media="all"/>
   <script type="text/javascript" src="https://web.archive.org/web/20230614135557js_/https://old.ceph.com/wp-content/themes/cephTheme/foundation_framework/js/vendor/jquery.js" id="jquery-js"></script>

   <link rel="stylesheet" href="https://web.archive.org/web/20230614135557cs_/https://ajax.googleapis.com/ajax/libs/jqueryui/1.11.2/themes/smoothness/jquery-ui.css"/>
   <link rel="stylesheet" href="https://web.archive.org/web/20230614135557cs_/https://old.ceph.com/pgcalc_assets/pgcalc.css"/>
   <script src="https://ajax.googleapis.com/ajax/libs/jqueryui/1.11.2/jquery-ui.min.js"></script>

        <script src="../../../_static/js/pgcalc.js"></script>
        	<div id="pgcalcdiv">
                <div id="instructions">
                <h2>Ceph PGs per Pool Calculator</h2><br/><fieldset><legend>Instructions</legend>
                <ol>
                        <li>Confirm your understanding of the fields by reading through the Key below.</li>
                        <li>Select a <b>"Ceph Use Case"</b> from the drop down menu.</li>
                        <li>Adjust the values in the <span class="inputColor addBorder" style="font-weight: bold;">"Green"</span> shaded fields below.<br/>
                                <b>Tip:</b> Headers can be clicked to change the value throughout the table.</li>
                        <li>You will see the Suggested PG Count update based on your inputs.</li>
                        <li>Click the <b>"Add Pool"</b> button to create a new line for a new pool.</li>
                        <li>Click the <span class="ui-icon ui-icon-trash" style="display:inline-block;"></span> icon to delete the specific Pool.</li>
                        <li>For more details on the logic used and some important details, see the area below the table.</li>
                        <li>Once all values have been adjusted, click the <b>"Generate Commands"</b> button to get the pool creation commands.</li>
                </ol></fieldset>
                </div>
                <div id="beforeTable"></div>
                <br/>
                <p class="validateTips">&nbsp;</p>
                <label for="presetType">Ceph Use Case Selector:</label><br/><select id="presetType"></select><button style="margin-left: 200px;" id="btnAddPool" type="button">Add Pool</button><button type="button" id="btnGenCommands" download="commands.txt">Generate Commands</button>
                <div id="pgsPerPoolTable">
                        <table id="pgsperpool">
                        </table>
                </div> <!-- id = pgsPerPoolTable -->
                <br/>
                <div id="afterTable"></div>
                <div id="countLogic"><fieldset><legend>Logic behind Suggested PG Count</legend>
                        <br/>
                        <div class="upperFormula">( Target PGs per OSD ) x ( OSD # ) x ( %Data )</div>
                        <div class="lowerFormula">( Size )</div>
                        <ol id="countLogicList">
                                <li>If the value of the above calculation is less than the value of <b>( OSD# ) / ( Size )</b>, then the value is updated to the value of <b>( OSD# ) / ( Size )</b>.  This is to ensure even load / data distribution by allocating at least one Primary or Secondary PG to every OSD for every Pool.</li>
                                <li>The output value is then rounded to the <b>nearest power of 2</b>.<br/><b>Tip:</b> The nearest power of 2 provides a marginal improvement in efficiency of the <a href="https://web.archive.org/web/20230614135557/http://ceph.com/docs/master/rados/operations/crush-map/" title="CRUSH Map Details">CRUSH</a> algorithm.</li>
                                <li>If the nearest power of 2 is more than <b>25%</b> below the original value, the next higher power of 2 is used.</li>
                        </ol>
                        <b>Objective</b>
                        <ul><li>The objective of this calculation and the target ranges noted in the &quot;Key&quot; section above are to ensure that there are sufficient Placement Groups for even data distribution throughout the cluster, while not going high enough on the PG per OSD ratio to cause problems during Recovery and/or Backfill operations.</li></ul>
                        <b>Effects of enpty or non-active pools:</b>
                        <ul>
                                <li>Empty or otherwise non-active pools should not be considered helpful toward even data distribution throughout the cluster.</li>
                                <li>However, the PGs associated with these empty / non-active pools still consume memory and CPU overhead.</li>
                        </ul>
                </fieldset>
                </div>
                <div id="commands" title="Pool Creation Commands"><code><pre id="commandCode"></pre></code></div>
                </div>
