var _____WB$wombat$assign$function_____ = function(name) {return (self._wb_wombat && self._wb_wombat.local_init && self._wb_wombat.local_init(name)) || self[name]; };
if (!self.__WB_pmw) { self.__WB_pmw = function(obj) { this.__WB_source = obj; return this; } }
{
  let window = _____WB$wombat$assign$function_____("window");
  let self = _____WB$wombat$assign$function_____("self");
  let document = _____WB$wombat$assign$function_____("document");
  let location = _____WB$wombat$assign$function_____("location");
  let top = _____WB$wombat$assign$function_____("top");
  let parent = _____WB$wombat$assign$function_____("parent");
  let frames = _____WB$wombat$assign$function_____("frames");
  let opener = _____WB$wombat$assign$function_____("opener");

var pow2belowThreshold = 0.25
var key_values={};
key_values['poolName']		={'name':'Pool Name','default':'newPool','description': 'Name of the pool in question.  Typical pool names are included below.', 'width':'30%; text-align: left'};
key_values['size']		={'name':'Size','default': 3, 'description': 'Number of replicas the pool will have. Default value of 3 is pre-filled.', 'width':'10%', 'global':1};
key_values['osdNum']		={'name':'OSD #','default': 100, 'description': 'Number of OSDs which this Pool will have PGs in. Typically, this is the entire Cluster OSD count, but could be less based on CRUSH rules. (e.g. Separate SSD and SATA disk sets)', 'width':'10%', 'global':1};
key_values['percData']		={'name':'%Data', 'default': 5, 'description': 'This value represents the approximate percentage of data which will be contained in this pool for that specific OSD set. Examples are pre-filled below for guidance.','width':'10%'};
key_values['targPGsPerOSD']	={'name':'Target PGs per OSD', 'default':100, 'description': 'This value should be populated based on the following guidance:', 'width':'10%', 'global':1, 'options': [ ['100','If the cluster OSD count is not expected to increase in the foreseeable future.'], ['200', 'If the cluster OSD count is expected to increase (up to double the size) in the foreseeable future.']]}

var notes ={
	'totalPerc':'<b>"Total Data Percentage"</b> below table should be a multiple of 100%.',
	'totalPGs':'<b>"Total PG Count"</b> below table will be the count of Primary PG copies. However, when calculating total PGs per OSD average, you must include all copies.',
	'noDecrease':'It\'s also important to know that the PG count can be increased, but <b>NEVER</b> decreased without destroying / recreating the pool. However, increasing the PG Count of a pool is one of the most impactful events in a Ceph Cluster, and should be avoided for production clusters if possible.',
};

var presetTables={};
presetTables['All-in-One']=[
	{ 'poolName' : 'rbd', 'size' : '3', 'osdNum' : '100', 'percData' : '100', 'targPGsPerOSD' : '100'},
];
presetTables['OpenStack']=[
	{ 'poolName' : 'cinder-backup', 'size' : '3', 'osdNum' : '100', 'percData' : '25', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'cinder-volumes', 'size' : '3', 'osdNum' : '100', 'percData' : '53', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'ephemeral-vms', 'size' : '3', 'osdNum' : '100', 'percData' : '15', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'glance-images', 'size' : '3', 'osdNum' : '100', 'percData' : '7', 'targPGsPerOSD' : '100'},
];
presetTables['OpenStack w RGW - Jewel and later']=[
        { 'poolName' : '.rgw.root', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.control', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.data.root', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.gc', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.intent-log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.meta', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.usage', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.keys', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.email', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.swift', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.uid', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.buckets.extra', 'size' : '3', 'osdNum' : '100', 'percData' : '1.0', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.buckets.index', 'size' : '3', 'osdNum' : '100', 'percData' : '3.0', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.buckets.data', 'size' : '3', 'osdNum' : '100', 'percData' : '19', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'cinder-backup', 'size' : '3', 'osdNum' : '100', 'percData' : '18', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'cinder-volumes', 'size' : '3', 'osdNum' : '100', 'percData' : '42.8', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'ephemeral-vms', 'size' : '3', 'osdNum' : '100', 'percData' : '10', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'glance-images', 'size' : '3', 'osdNum' : '100', 'percData' : '5', 'targPGsPerOSD' : '100'},
];

presetTables['Rados Gateway Only - Jewel and later']=[
        { 'poolName' : '.rgw.root', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.control', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.data.root', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.gc', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.intent-log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.meta', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.usage', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.keys', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.email', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.swift', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.users.uid', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.buckets.extra', 'size' : '3', 'osdNum' : '100', 'percData' : '1.0', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.buckets.index', 'size' : '3', 'osdNum' : '100', 'percData' : '3.0', 'targPGsPerOSD' : '100'},
        { 'poolName' : 'default.rgw.buckets.data', 'size' : '3', 'osdNum' : '100', 'percData' : '94.8', 'targPGsPerOSD' : '100'},
];

presetTables['OpenStack w RGW - Infernalis and earlier']=[
	{ 'poolName' : '.intent-log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.buckets', 'size' : '3', 'osdNum' : '100', 'percData' : '18', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.buckets.extra', 'size' : '3', 'osdNum' : '100', 'percData' : '1.0', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.buckets.index', 'size' : '3', 'osdNum' : '100', 'percData' : '3.0', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.control', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.gc', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.root', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.usage', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users.email', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users.swift', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users.uid', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'cinder-backup', 'size' : '3', 'osdNum' : '100', 'percData' : '19', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'cinder-volumes', 'size' : '3', 'osdNum' : '100', 'percData' : '42.9', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'ephemeral-vms', 'size' : '3', 'osdNum' : '100', 'percData' : '10', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'glance-images', 'size' : '3', 'osdNum' : '100', 'percData' : '5', 'targPGsPerOSD' : '100'},
];

presetTables['Rados Gateway Only - Infernalis and earlier']=[
	{ 'poolName' : '.intent-log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.log', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.buckets', 'size' : '3', 'osdNum' : '100', 'percData' : '94.9', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.buckets.extra', 'size' : '3', 'osdNum' : '100', 'percData' : '1.0', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.buckets.index', 'size' : '3', 'osdNum' : '100', 'percData' : '3.0', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.control', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.gc', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.rgw.root', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.usage', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users.email', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users.swift', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
	{ 'poolName' : '.users.uid', 'size' : '3', 'osdNum' : '100', 'percData' : '0.1', 'targPGsPerOSD' : '100'},
];
presetTables['RBD and libRados']=[
	{ 'poolName' : 'rbd', 'size' : '3', 'osdNum' : '100', 'percData' : '75', 'targPGsPerOSD' : '100'},
	{ 'poolName' : 'myObjects', 'size' : '3', 'osdNum' : '100', 'percData' : '25', 'targPGsPerOSD' : '100'},
];

$(function() {
	$("#presetType").on("change",changePreset);
	$("#btnAddPool").on("click",addPool);
	$("#btnGenCommands").on("click",generateCommands);
	$.each(presetTables,function(index,value) {
		selIndex='';
		if ( index == 'OpenStack w RGW - Jewel and later' )
			selIndex=' selected';
		$("#presetType").append("<option value=\""+index+"\""+selIndex+">"+index+"</option>");
	});
	changePreset();
	$("#beforeTable").html("<fieldset id='keyFieldset'><legend>Key</legend><dl class='table-display' id='keyDL'></dl></fieldset>");
	$.each(key_values, function(index, value) {
		pre='';
		post='';
		if ('global' in value) {
			pre='<a href="javascript://" onClick="globalChange(\''+index+'\');" title="Change the \''+value['name']+'\' parameter globally">';
			post='</a>'
		}

		var dlAdd="<dt id='dt_"+index+"'>"+pre+value['name']+post+"</dt><dd id='dd_"+index+"'>"+value['description'];
		if ( 'options' in value ) {
			dlAdd+="<dl class='sub-table'>";
			$.each(value['options'], function (subIndex, subValue) {
				dlAdd+="<dt><a href=\"javascript://\" onClick=\"massUpdate('"+index+"','"+subValue[0]+"');\" title=\"Set all '"+value['name']+"' fields to '"+subValue[0]+"'.\">"+subValue[0]+"</a></dt><dd>"+subValue[1]+"</dd>";
			});
			dlAdd+="</dl>";
		}
		dlAdd+="</dd>";
		$("#keyDL").append(dlAdd);
	});
	$("#afterTable").html("<fieldset id='notesFieldset'><legend>Notes</legend><ul id='notesUL'>\n<ul></fieldset>");
	$.each(notes,function(index, value) {
		$("#notesUL").append("\t<li id=\"li_"+index+"\">"+value+"</li>\n");
	});

});

function changePreset() {
	resetTable();
	fillTable($("#presetType").val());
}

function resetTable() {
	$("#pgsperpool").html("");
	$("#pgsperpool").append("<tr id='headerRow'>\n</tr>\n");
	$("#headerRow").append("\t<th>&nbsp;</th>\n");
	var fieldCount=0;
	var percDataIndex=0;
	$.each(key_values, function(index, value) {
		fieldCount++;
		pre='';
		post='';
		var widthAdd='';
		if ( index == 'percData' )
			percDataIndex=fieldCount;
		if ('width' in value)
			widthAdd=' style=\'width: '+value['width']+'\'';
		if ('global' in value) {
			pre='<a href="javascript://" onClick="globalChange(\''+index+'\');" title="Change the \''+value['name']+'\' parameter globally">';
			post='</a>'
		}
		$("#headerRow").append("\t<th"+widthAdd+">"+pre+value['name']+post+"</th>\n");
	});
	percDataIndex++;
	$("#headerRow").append("\t<th class='center'>Suggested PG Count</th>\n");
	$("#pgsperpool").append("<tr id='totalRow'><td colspan='"+percDataIndex+"' id='percTotal' style='text-align: right; margin-right: 10px;'><strong>Total Data Percentage:</strong> <span id='percTotalValue'>0</span>%</td><td>&nbsp;</td><td id='pgTotal' class='bold pgcount' style='text-align: right;'>PG Total Count: <span id='pgTotalValue'>0</span></td></tr>");
}

function nearestPow2( aSize ){
	var tmp=Math.pow(2, Math.round(Math.log(aSize)/Math.log(2)));
	if(tmp<(aSize*(1-pow2belowThreshold)))
		tmp*=2;
	return tmp;
}

function globalChange(field) {
	dialogHTML='<div title="Change \''+key_values[field]['name']+'\' Globally"><form>';
	dialogHTML+='<label for="value">New '+key_values[field]['name']+' value:</label><br />\n';
	dialogHTML+='<input type="text" name="globalValue" id="globalValue" value="'+$("#row0_"+field+"_input").val()+'" style="text-align: right;"/>';
	dialogHTML+='<input type="hidden" name="globalField" id="globalField" value="'+field+'"/>';
	dialogHTML+='<input type="submit" tabindex="-1" style="position:absolute; top:-1000px">';
	dialogHTML+='</form>';
	globalDialog=$(dialogHTML).dialog({
		autoOpen: true,
		width: 350,
		show: 'fold',
		hide: 'fold',
		modal: true,
		buttons: {
			"Update Value": function() { massUpdate($("#globalField").val(),$("#globalValue").val()); globalDialog.dialog("close"); setTimeout(function() { globalDialog.dialog("destroy"); }, 1000); },
			"Cancel": function() { globalDialog.dialog("close"); setTimeout(function() { globalDialog.dialog("destroy"); }, 1000); }
		}
	});
}

var rowCount=0;
function fillTable(presetType) {
	rowCount=0;
	$.each(presetTables[presetType], function(index,value) {
		addTableRow(value);
	});
}

function addPool() {
	dialogHTML='<div title="Add Pool"><form>';
	$.each(key_values, function(index,value) {
		dialogHTML+='<br /><label for="new'+index+'">'+value['name']+':</label><br />\n';
		classAdd='right';
		if ( index == 'poolName' )
			classAdd='left';
		dialogHTML+='<input type="text" name="new'+index+'" id="new'+index+'" value="'+value['default']+'" class="'+classAdd+'"/><br />';
	});
	dialogHTML+='<input type="submit" tabindex="-1" style="position:absolute; top:-1000px">';
	dialogHTML+='</form>';
	addPoolDialog=$(dialogHTML).dialog({
		autoOpen: true,
		width: 350,
		show: 'fold',
		hide: 'fold',
		modal: true,
		buttons: {
			"Add Pool": function() {
				var newPoolValues={};
				$.each(key_values,function(index,value) {
					newPoolValues[index]=$("#new"+index).val();
				});
				addTableRow(newPoolValues);
				addPoolDialog.dialog("close");
				setTimeout(function() { addPoolDialog.dialog("destroy"); }, 1000); },
			"Cancel": function() { addPoolDialog.dialog("close"); setTimeout(function() { addPoolDialog.dialog("destroy"); }, 1000); }
		}
	});

//		addTableRow({'poolName':'newPool','size':3, 'osdNum':100,'targPGsPerOSD': 100, 'percData':0});
}

function addTableRow(rowValues) {
	rowAdd="<tr id='row"+rowCount+"'>\n";
	rowAdd+="\t<td width='15px' class='inputColor'><a href='javascript://' title='Remove Pool' onClick='$(\"#row"+rowCount+"\").remove();updateTotals();'><span class='ui-icon ui-icon-trash'></span></a></td>\n";
	$.each(key_values, function(index,value) {
		classAdd=' center';
		modifier='';
		if ( index == 'percData' ) {
			classAdd='" style="text-align: right;';
	//		modifier=' %';
		} else if ( index == 'poolName' )
			classAdd=' left';
		rowAdd+="\t<td id=\"row"+rowCount+"_"+index+"\"><input type=\"text\" class=\"inputColor "+index+classAdd+"\" id=\"row"+rowCount+"_"+index+"_input\" value=\""+rowValues[index]+"\" onFocus=\"focusMe("+rowCount+",'"+index+"');\" onKeyUp=\"keyMe("+rowCount+",'"+index+"');\" onBlur=\"blurMe("+rowCount+",'"+index+"');\">"+modifier+"</td>\n";
	});
	rowAdd+="\t<td id=\"row"+rowCount+"_pgCount\" class='pgcount' style='text-align: right;'>0</td></tr>";
	$("#totalRow").before(rowAdd);
	updatePGCount(rowCount);
	$("[id$='percData_input']").each(function() { var fieldVal=parseFloat($(this).val()); $(this).val(fieldVal.toFixed(2)); });
	rowCount++;
}

function updatePGCount(rowID) {
	if(rowID==-1) {
		for(var i=0;i<rowCount;i++) {
			updatePGCount(i);
		}
	} else {
		minValue=nearestPow2(Math.floor($("#row"+rowID+"_osdNum_input").val()/$("#row"+rowID+"_size_input").val())+1);
		if(minValue<$("#row"+rowID+"_osdNum_input").val())
			minValue*=2;
		calcValue=nearestPow2(Math.floor(($("#row"+rowID+"_targPGsPerOSD_input").val()*$("#row"+rowID+"_osdNum_input").val()*$("#row"+rowID+"_percData_input").val())/(100*$("#row"+rowID+"_size_input").val())));
		if(minValue>calcValue)
			$("#row"+rowID+"_pgCount").html(minValue);
		else
			$("#row"+rowID+"_pgCount").html(calcValue);
	}
	updateTotals();
}

function focusMe(rowID,field) {
	$("#row"+rowID+"_"+field+"_input").toggleClass('inputColor');
	$("#row"+rowID+"_"+field+"_input").toggleClass('highlightColor');
	$("#dt_"+field).toggleClass('highlightColor');
	$("#dd_"+field).toggleClass('highlightColor');
	updatePGCount(rowID);
}

function blurMe(rowID,field) {
	focusMe(rowID,field);
	$("[id$='percData_input']").each(function() { var fieldVal=parseFloat($(this).val()); $(this).val(fieldVal.toFixed(2)); });
}

function keyMe(rowID,field) {
	updatePGCount(rowID);
}

function massUpdate(field,value) {
	$("[id$='_"+field+"_input']").val(value);
	key_values[field]['default']=value;
	updatePGCount(-1);
}

function updateTotals() {
	var totalPerc=0;
	var totalPGs=0;
	$("[id$='percData_input']").each(function() {
		totalPerc+=parseFloat($(this).val());
		if ( parseFloat($(this).val()) > 100 ) 
			$(this).addClass('ui-state-error');
		else
			$(this).removeClass('ui-state-error');
	});
	$("[id$='_pgCount']").each(function() { 
		totalPGs+=parseInt($(this).html()); 
	});
	$("#percTotalValue").html(totalPerc.toFixed(2));
	$("#pgTotalValue").html(totalPGs);
	if(parseFloat(totalPerc.toFixed(2)) % 100 != 0) {
		$("#percTotalValue").addClass('ui-state-error');
		$("#li_totalPerc").addClass('ui-state-error');
	} else {
		$("#percTotalValue").removeClass('ui-state-error');
		$("#li_totalPerc").removeClass('ui-state-error');
	}
	$("#commandCode").html("");
}

function generateCommands() {
	outputCommands="## Note: The 'while' loops below pause between pools to allow all\n\
##       PGs to be created.  This is a safety mechanism to prevent\n\
##       saturating the Monitor nodes.\n\
## -------------------------------------------------------------------\n\n";
	for(i=0;i<rowCount;i++) {
		console.log(i);
		outputCommands+="ceph osd pool create "+$("#row"+i+"_poolName_input").val()+" "+$("#row"+i+"_pgCount").html()+"\n";
		outputCommands+="ceph osd pool set "+$("#row"+i+"_poolName_input").val()+" size "+$("#row"+i+"_size_input").val()+"\n";
		outputCommands+="while [ $(ceph -s | grep creating -c) -gt 0 ]; do echo -n .;sleep 1; done\n\n";
	}
	window.location.href = "data:application/download," + encodeURIComponent(outputCommands);
}


}
