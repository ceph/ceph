// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * gui_monitor_interface.cc -- Handles the GUI of the Ceph monitor
 *
 *
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * Copyright (C) 2009-2010 Michael McThrow <mmcthrow@gmail.com>
 * Portions Copyright Dreamhost 2010
 *
 */

#include "common/Clock.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Thread.h"
#include "mon/MonClient.h"
#include "mon/MonMap.h"
#include "tools/common.h"
#include "tools/gui.h"
#include "tools/gui_resources.h"

#include <gtkmm.h>
#include <libgen.h>
#include <set>
#include <string>

using std::set;
using std::string;

#define MAX_VIEW_ICONS 6

// Computes floor(m/n), where m and n are integers.
#define FLOOR(x, y) ((x) % (y)) ? ((x) / (y) + 1) : ((x) / (y))

#define MY_GET_WIDGET(x) do { builder->get_widget(#x, x); } while(0);

///////////////// Functions /////////////////
static std::string resource_path_prefix(".");

static std::string resource_path(const std::string &loc)
{
  ostringstream oss;
  oss << resource_path_prefix << "/" << loc;
  return oss.str();
}

//
// Figure out where our XML files are for the GUI.
//
// The first place we look is in "D/gui_resources"
// (where D is the directory where the executable lives.)
// If we find it there, we assume that the developer has built the application and is running it
// from the source tree.
//
// If that doesn't work, we use the GUI_RESOURCE_DIR set up by automake.
// When the user runs make install, all the XML files will get placed there.
//
static int calculate_resource_path_prefix(const char *argv0)
{
  char d_input[strlen(argv0) + 1];
  memset(d_input, 0, sizeof(d_input));
  char *d_name = dirname(d_input);

  ostringstream oss;
  oss << d_name << "/tools/gui_resources";
  resource_path_prefix = oss.str();
  if (!access(resource_path(GUI_MONITOR_BUILDER_FILE).c_str(), R_OK))
    return 0;

  resource_path_prefix = CEPH_TOOL_GUIDIR;
  if (!access(resource_path(GUI_MONITOR_BUILDER_FILE).c_str(), R_OK))
    return 0;

  return EIO;
}

// Given an integer m and an array of length n, divides m into n categories of
// equal or near-equal values.
static void gen_ranges(unsigned int m, unsigned int n,
		       unsigned int *ranges)
{
  unsigned int multiple = FLOOR(m, n);
  unsigned int remainder = m % n;
  long int i, j;

  if (m <= n) {
    for (i = 0; i < n; i++)
      ranges[i] = (i < m) ? 1 : 0;
  }
  else {
    for (i = n - 1, j = (remainder) ? n - remainder: 0; i >= 0; i--) {
      if (j) {
	ranges[i] = multiple - 1;
	--j;
      }
      else
	ranges[i] = multiple;
    }
  }
}

// Converts a Ceph entity_addr_t to a C++ string.
static std::string addr_to_str(const entity_addr_t& addr)
{
  ostringstream oss;
  oss << addr;
  return oss.str();
}

///////////////// Classes /////////////////

class GuiMonitorThread : public Thread
{
public:
  GuiMonitorThread(GuiMonitor *gui_)
    : gui(gui_),
      shutting_down(false)
  {
  }

  void *entry()
  {
    g.lock.Lock();
    while (true) {
      utime_t t(g_clock.now());
      t += 3.0;
      g.gui_cond.WaitUntil(g.lock, t);
      if (shutting_down) {
	g.lock.Unlock();
	return NULL;
      }
      gui->check_status();
    }
  }

  void shutdown()
  {
    shutting_down = true;
    g.gui_cond.Signal();
    this->join();
  }

private:
  GuiMonitor *gui;
  bool shutting_down;
};

GuiMonitor::GuiMonitor(Glib::RefPtr<Gtk::Builder> builder)
  : pg_cluster_zoom(0),
    osd_cluster_zoom(0),
    mds_cluster_zoom(0),
    send_command_success(false),
    view_node_success(false),
    thread(NULL)
{
  MY_GET_WIDGET(guiMonitorWindow);
  MY_GET_WIDGET(guiMonitorQuitImageMenuItem);
  MY_GET_WIDGET(guiMonitorCopyImageMenuItem);
  MY_GET_WIDGET(guiMonitorSendCommandMenuItem);
  MY_GET_WIDGET(guiMonitorViewNodeMenuItem);
  MY_GET_WIDGET(guiMonitorAboutImageMenuItem);
  MY_GET_WIDGET(guiMonitorPGClusterStatsLabel);
  MY_GET_WIDGET(guiMonitorPGClusterIconView);
  MY_GET_WIDGET(guiMonitorPGClusterBackButton);
  MY_GET_WIDGET(guiMonitorPGClusterViewAllButton);
  MY_GET_WIDGET(guiMonitorPGClusterStatsButton);
  MY_GET_WIDGET(guiMonitorMonitorClusterStatsLabel);
  MY_GET_WIDGET(guiMonitorMonitorClusterTreeView);
  MY_GET_WIDGET(guiMonitorMonitorClusterStatsButton);
  MY_GET_WIDGET(guiMonitorOSDClusterStatsLabel);
  MY_GET_WIDGET(guiMonitorOSDClusterIconView);
  MY_GET_WIDGET(guiMonitorOSDClusterBackButton);
  MY_GET_WIDGET(guiMonitorOSDClusterViewAllButton);
  MY_GET_WIDGET(guiMonitorOSDClusterStatsButton);
  MY_GET_WIDGET(guiMonitorMDSClusterStatsLabel);
  MY_GET_WIDGET(guiMonitorMDSClusterIconView);
  MY_GET_WIDGET(guiMonitorMDSClusterBackButton);
  MY_GET_WIDGET(guiMonitorMDSClusterViewAllButton);
  MY_GET_WIDGET(guiMonitorMDSClusterStatsButton);
  MY_GET_WIDGET(guiMonitorLogTextView);
  MY_GET_WIDGET(guiMonitorStatusbar);
  MY_GET_WIDGET(guiMonitorAboutDialog);
  MY_GET_WIDGET(viewNodeDialog);
  MY_GET_WIDGET(viewNodeNameEntry);
  MY_GET_WIDGET(viewNodeTypeComboBox);
  MY_GET_WIDGET(viewNodeNameLabel);
  MY_GET_WIDGET(sendCommandDialog);
  MY_GET_WIDGET(sendCommandPromptEntry);
}

GuiMonitor::~GuiMonitor()
{
  delete guiMonitorWindow;
  delete viewNodeDialog;
  delete sendCommandDialog;
  delete guiMonitorAboutDialog;
}

bool GuiMonitor::init()
{
  assert(!thread);

  if (!guiMonitorWindow)
    return false;
  if (!open_icon(blacklistIcon, resource_path(BLACKLIST_ICON_PATH)))
    return false;
  if (!open_icon(clientIcon, resource_path(CLIENT_ICON_PATH)))
    return false;
  if (!open_icon(MDSIcon, resource_path(MDS_ICON_PATH)))
    return false;
  //if (!open_icon(failedMDSIcon, resource_path(FAILED_MDS_ICON_PATH)))
  //  return false;
  //if (!open_icon(stoppedMDSIcon, resource_path(STOPPED_MDS_ICON_PATH)))
  //  return false;
  if (!open_icon(monitorIcon, resource_path(MONITOR_ICON_PATH)))
    return false;
  if (!open_icon(upOSDIcon, resource_path(UP_OSD_ICON_PATH)))
    return false;
  if (!open_icon(downOSDIcon, resource_path(DOWN_OSD_ICON_PATH)))
    return false;
  if (!open_icon(outOSDIcon, resource_path(OUT_OSD_ICON_PATH)))
    return false;
  if (!open_icon(PGIcon, resource_path(PG_ICON_PATH)))
    return false;

  // Link elements (e.g., text buffers, list stores, etc.) to their
  // containers (e.g., text views, list views, etc.)
  link_elements();

  // connect callbacks to their corresponding signals.
  connect_signals();

  thread = new GuiMonitorThread(this);
  thread->create();
  return true;
}

void GuiMonitor::run_main_loop(Gtk::Main &kit)
{
  kit.run(*guiMonitorWindow);
}

void GuiMonitor::thread_shutdown()
{
  if (!thread)
    return;
  thread->shutdown();
  delete thread;
  thread = NULL;
}

bool GuiMonitor::open_icon(Glib::RefPtr<Gdk::Pixbuf> &icon, const std::string &path)
{
  try {
      icon = Gdk::Pixbuf::create_from_file(path);
  }
  catch (const Gdk::PixbufError& e) {
      derr << "Problem making graphic from " << path << "; error code: "
	   << e.code() << ": " << e.what() << dendl;
      return false;
  }
  catch (const Glib::FileError& e) {
      derr << "Problem open " << path << dendl;
      return false;
  }

  return true;
}

// Connects signals to the GUI elements such that they respond to events.
void GuiMonitor::connect_signals()
{
  //Gtk::Main::signal_run().connect(sigc::mem_fun(this,
  //   &GuiMonitor::init_check_status));

  Gtk::Main::signal_quit().connect(sigc::mem_fun(this,
    &GuiMonitor::quit_signal_handler));

  // guiMonitorWindow
  guiMonitorWindow->signal_delete_event().connect(
    sigc::mem_fun(this, &GuiMonitor::quit_gui));

  // guiMonitorQuitImageMenuItem
  guiMonitorQuitImageMenuItem->signal_activate().connect(
    sigc::mem_fun(this, &GuiMonitor::gui_monitor_quit));

  // guiMonitorCopyImageMenuItem
  guiMonitorCopyImageMenuItem->signal_activate().connect(
    sigc::mem_fun(this, &GuiMonitor::copy_log));

  // grey out the copy menu item until the log text box gets data in it.
  guiMonitorCopyImageMenuItem->set_sensitive(false);

  // guiMonitorSendCommandMenuItem
  guiMonitorSendCommandMenuItem->signal_activate().connect(
    sigc::mem_fun(this, &GuiMonitor::open_send_command));

  // guiMonitorViewNodeMenuItem
  guiMonitorViewNodeMenuItem->signal_activate().connect(
    sigc::mem_fun(this, &GuiMonitor::open_view_mode));

  // guiMonitorAboutImageMenuItem
  guiMonitorAboutImageMenuItem->signal_activate().connect(
    sigc::mem_fun(this, &GuiMonitor::open_about_dialog));

  // sendCommandDialog
  sendCommandDialog->signal_response().connect(sigc::mem_fun(this,
    &GuiMonitor::handle_send_command_response));

  // viewNodeDialog
  viewNodeDialog->signal_response().connect(sigc::mem_fun(this,
      &GuiMonitor::handle_view_node_response));
  viewNodeTypeComboBox->signal_changed().connect(sigc::mem_fun(this,
      &GuiMonitor::handle_view_node_change));

  /*
    * OSD Cluster
    */
  // guiMonitorOSDClusterIconView
  guiMonitorOSDClusterIconView->signal_item_activated().connect(
      sigc::mem_fun(this, &GuiMonitor::osdc_cluster_zoom_in));

  // guiMonitorOSDClusterBackButton
  guiMonitorOSDClusterBackButton->signal_activate().connect(sigc::mem_fun(this,
      &GuiMonitor::osdc_cluster_back));
  guiMonitorOSDClusterBackButton->signal_clicked().connect(sigc::mem_fun(this,
      &GuiMonitor::osdc_cluster_back));

  // guiMonitorOSDClusterViewAllButton
  guiMonitorOSDClusterViewAllButton->signal_activate().connect(
      sigc::mem_fun(this, &GuiMonitor::osdc_cluster_view_all));
  guiMonitorOSDClusterViewAllButton->signal_clicked().connect(
      sigc::mem_fun(this, &GuiMonitor::osdc_cluster_view_all));

  // Grey out the "Back" and "View All" icons of the OSD cluster until the
  // OSDCluterZoom > 0.
  guiMonitorOSDClusterBackButton->set_sensitive(false);
  guiMonitorOSDClusterViewAllButton->set_sensitive(false);

  // guiMonitorOSDClusterStatsButton
  guiMonitorOSDClusterStatsButton->signal_activate().connect(
      sigc::mem_fun(this, &GuiMonitor::osdc_cluster_stats));
  guiMonitorOSDClusterStatsButton->signal_clicked().connect(
      sigc::mem_fun(this, &GuiMonitor::osdc_cluster_stats));

  /*
  * MDS Cluster
  */
  // guiMonitorMDSClusterIconView
  guiMonitorMDSClusterIconView->signal_item_activated().connect(
      sigc::mem_fun(this, &GuiMonitor::mds_cluster_zoom_in));

  // guiMonitorMDSClusterBackButton
  guiMonitorMDSClusterBackButton->signal_activate().connect(sigc::mem_fun(this,
      &GuiMonitor::mds_cluster_back));
  guiMonitorMDSClusterBackButton->signal_clicked().connect(sigc::mem_fun(this,
      &GuiMonitor::mds_cluster_back));

  // guiMonitorMDSClusterViewAllButton
  guiMonitorMDSClusterViewAllButton->signal_activate().connect(
      sigc::mem_fun(this, &GuiMonitor::mds_cluster_view_all));
  guiMonitorMDSClusterViewAllButton->signal_clicked().connect(
      sigc::mem_fun(this, &GuiMonitor::mds_cluster_view_all));

  // Grey out the "Back" and "View All" icons of the MDS cluster until the
  // MDSCluterZoom > 0.
  guiMonitorMDSClusterBackButton->set_sensitive(false);
  guiMonitorMDSClusterViewAllButton->set_sensitive(false);

  // guiMonitorMDSClusterStatsButton
  guiMonitorMDSClusterStatsButton->signal_activate().connect(
      sigc::mem_fun(this, &GuiMonitor::mds_cluster_stats));
  guiMonitorMDSClusterStatsButton->signal_clicked().connect(
      sigc::mem_fun(this, &GuiMonitor::mds_cluster_stats));

  /*
  * PG Cluster
  */
  // guiMonitorPGClusterIconView
  guiMonitorPGClusterIconView->signal_item_activated().connect(
      sigc::mem_fun(this, &GuiMonitor::pg_cluster_zoom_in));

  // guiMonitorPGClusterBackButton
  guiMonitorPGClusterBackButton->signal_activate().connect(sigc::mem_fun(this,
      &GuiMonitor::pg_cluster_back));
  guiMonitorPGClusterBackButton->signal_clicked().connect(sigc::mem_fun(this,
      &GuiMonitor::pg_cluster_back));

  // guiMonitorPGClusterViewAllButton
  guiMonitorPGClusterViewAllButton->signal_activate().connect(
      sigc::mem_fun(this, &GuiMonitor::pg_cluster_view_all));
  guiMonitorPGClusterViewAllButton->signal_clicked().connect(
      sigc::mem_fun(this, &GuiMonitor::pg_cluster_view_all));

  // Grey out the "Back" and "View All" icons of the PG cluster until the
  // PGCluterZoom > 0.
  guiMonitorPGClusterBackButton->set_sensitive(false);
  guiMonitorPGClusterViewAllButton->set_sensitive(false);

  // guiMonitorPGClusterStatsButton
  guiMonitorPGClusterStatsButton->signal_activate().connect(
      sigc::mem_fun(this, &GuiMonitor::pg_cluster_stats));
  guiMonitorPGClusterStatsButton->signal_clicked().connect(
      sigc::mem_fun(this, &GuiMonitor::pg_cluster_stats));

  /*
  * Monitor Cluster
  */
  // guiMonitorMonitorClusterStatsButton
  guiMonitorMonitorClusterStatsButton->signal_activate().connect(
      sigc::mem_fun(this, &GuiMonitor::monitor_cluster_stats));
  guiMonitorMonitorClusterStatsButton->signal_clicked().connect(
      sigc::mem_fun(this, &GuiMonitor::monitor_cluster_stats));
}

// Connects elements to their GUI containers.
void GuiMonitor::link_elements()
{
  // get the buffer from the log's TextView
  guiMonitorLogTextBuffer = guiMonitorLogTextView->get_buffer();

  // create tree models
  guiMonitorMonitorClusterEntries = Gtk::ListStore::create(monitorColumns);
  guiMonitorOSDClusterIcons = Gtk::ListStore::create(icon_columns);
  guiMonitorMDSClusterIcons = Gtk::ListStore::create(icon_columns);
  guiMonitorPGClusterIcons = Gtk::ListStore::create(icon_columns);

  // connect the list stores to the tree views and icon views
  guiMonitorMonitorClusterTreeView->set_model(
    guiMonitorMonitorClusterEntries);
  guiMonitorMonitorClusterTreeView->append_column("Monitor #",
    monitorColumns.key);
  guiMonitorMonitorClusterTreeView->append_column("Address",
    monitorColumns.value);

  guiMonitorOSDClusterIconView->set_model(guiMonitorOSDClusterIcons);
  guiMonitorOSDClusterIconView->set_text_column(icon_columns.caption);
  guiMonitorOSDClusterIconView->set_pixbuf_column(icon_columns.icon);

  guiMonitorMDSClusterIconView->set_model(guiMonitorMDSClusterIcons);
  guiMonitorMDSClusterIconView->set_text_column(icon_columns.caption);
  guiMonitorMDSClusterIconView->set_pixbuf_column(icon_columns.icon);

  guiMonitorPGClusterIconView->set_model(guiMonitorPGClusterIcons);
  guiMonitorPGClusterIconView->set_text_column(icon_columns.caption);
  guiMonitorPGClusterIconView->set_pixbuf_column(icon_columns.icon);

  viewNodeNodeTypeListStore = Gtk::ListStore::create(viewNodeColumns);
  viewNodeTypeComboBox->set_model(viewNodeNodeTypeListStore);
  Gtk::TreeModel::Row currentRow;

  // Set contents of the list store for use in the the combo box in the "View
  // Node..." dialog box
  for (int i = 0; i < NUM_NODE_TYPES; i++) {
      enum NodeType type = (NodeType)i;

      if (type == MON_NODE)
         continue;

      currentRow = *(viewNodeNodeTypeListStore->append());

      currentRow[viewNodeColumns.type] = type;

      currentRow[viewNodeColumns.name] = (type == OSD_NODE) ?
                                  "Object Storage Device" :
                                  (type == PG_NODE) ? "Placement Group" :
                                  (type == MDS_NODE) ? "Metadata Server" :
                                  "Monitor";
  }
}

/*
 * For the OSD, MDS, and PG clusters, the program should limit the amount
 * of cluster icons displayed in their respective cluster view areas in
 * order to ensure that users are not overwhelmed by the amount of
 * clusters in large Ceph installations.  Activating an icon corresponding
 * to a node will open a window that displays information about the node.
 * The number of icons shown in each view should not take up more than the
 * display area of each view.  If the amount of icons cannot be displayed
 * in the view without a heavy amount of scrolling, then some or all icons
 * will correspond to a *range* of nodes (for example, nodes 0 to 9 may be
 * represented by one icon in the view).  Activating an icon corresponding
 * to a range of nodes will update the view to include only information
 * about the nodes in that range; this can be conceputalized as "zooming
 * into" a group of nodes.  Range information corresponding to previous
 * clusters will be preserved, but not updated.  The stats label
 * correspnding to the cluster will state if any updates are available.
 * If the user clicks "View All," then this has the effect of updating the
 * entire cluster view again.
 *
 * For the monitor cluster, a simple list containing information about
 * each monitor cluster is shown.  Activating an entry will open a window
 * that displays more information about the monitor cluster.  Unlike the
 * other clusters, no "zoom in" or "zoom out" functionality is
 * implemented.
 */
void GuiMonitor::update_osd_cluster_view()
{
  int num_osds = g.osdmap.get_max_osd();

  // Do not update the OSD cluster view if the zoom level is not zero.
  if (!osd_cluster_zoom)
    view_osd_nodes(0, num_osds);

  ostringstream oss;
  oss << num_osds << " OSD" << ((num_osds == 1) ? "" : "s")
      << " Total: " << g.osdmap.get_num_up_osds() << " Up, "
      << g.osdmap.get_num_in_osds() << " In";
  guiMonitorOSDClusterStatsLabel->set_label(oss.str());
}

void GuiMonitor::update_mds_cluster_view()
{
  // Do not update the MDS cluster view if the zoom level is not zero.
  if (!mds_cluster_zoom)
    view_mds_nodes();

  ostringstream oss;
  oss << g.mdsmap.get_num_mds() << " In, "
      << g.mdsmap.get_num_failed() << " Failed, "
      << g.mdsmap.get_num_mds(MDSMap::STATE_STOPPED) << " Stopped, "
      << g.mdsmap.get_max_mds() << " Max";
  guiMonitorMDSClusterStatsLabel->set_label(oss.str());
}

void GuiMonitor::update_pg_cluster_view()
{
  // Do not update the PG cluster view if the zoom level is not zero.
  if (!pg_cluster_zoom)
    view_pg_nodes(0, 0, true);

  ostringstream oss;
  oss << g.pgmap.pg_stat.size() << " Placement Groups\n"
      << kb_t(g.pgmap.pg_sum.num_kb) << " Data, "
      << kb_t(g.pgmap.osd_sum.kb_used) << " Used, "
      << kb_t(g.pgmap.osd_sum.kb_avail) << " / "
      << kb_t(g.pgmap.osd_sum.kb) << " Available";
  guiMonitorPGClusterStatsLabel->set_label(oss.str());
}

void GuiMonitor::update_mon_cluster_view()
{
  stringstream input;
  Gtk::TreeModel::Row current_row;
  entity_inst_t currentMonitor;
  string currentAddress;
  unsigned int monitors = g.mc.monmap.size();

  // Clear current contents of the monitor cluster area of the GUI.
  guiMonitorMonitorClusterEntries->clear();

  string label = str(boost::format("%lu Monitors") % monitors);

  guiMonitorMonitorClusterStatsLabel->set_label(label);

  // For each monitor in the monitor map, output its ID and its address.
  for (unsigned int i = 0; i < monitors; i++) {
    currentMonitor = g.mc.monmap.get_inst(i);

    input << currentMonitor.addr;
    currentAddress = input.str();
    input.str("");
    input.flush();
    currentAddress = addr_to_str(currentMonitor.addr);

    current_row = *(guiMonitorMonitorClusterEntries->append());

    current_row[monitorColumns.key] = str(boost::format("%d") % i);
    current_row[monitorColumns.value] = currentAddress;
  }
}

std::string GuiMonitor::
gen_osd_icon_caption(unsigned int begin, unsigned int end) const
{
  boost::format rangeFormatter("OSDs %lu-%lu");
  boost::format singleFormatter("OSD %lu");

  return (end - begin) ?  str(rangeFormatter % begin % end) :
    str(singleFormatter % begin);
}

void GuiMonitor::view_osd_nodes(unsigned int begin, unsigned int end, bool
 viewAll)
{
  unsigned int size = end - begin;
  unsigned int *ranges = new unsigned int[MAX_VIEW_ICONS];
  //unsigned int maxViewIcons = getMaxViewIcons();

  gen_ranges(size, MAX_VIEW_ICONS, ranges);

  // remove old icons
  if (viewAll) {
    while (!old_osd_cluster_zoom_states.empty())
      old_osd_cluster_zoom_states.pop();

    osd_cluster_zoom = 0;
  }

  guiMonitorOSDClusterIcons->clear();

  unsigned int i = 0, j, range_offset = begin, begin_range, end_range;
  int icon_status = CEPH_OSD_UP;
  Gtk::TreeModel::Row row;

  string caption;

  // create and display icons
  for (i = 0; i < MAX_VIEW_ICONS; i++) {
      if (ranges[i]) {
         icon_status = CEPH_OSD_UP;
         row = *(guiMonitorOSDClusterIcons->append());

         begin_range = range_offset;
         end_range = ranges[i] - 1 + range_offset;

         caption = gen_osd_icon_caption(begin_range, end_range);

         for (j = begin_range; j <= end_range; j++) {
            if (g.osdmap.is_out(j)) {
               icon_status = CEPH_OSD_OUT;
               break;
            }
            else if (g.osdmap.is_down(j))
               icon_status = ~CEPH_OSD_UP;
            else
               ;
         }

         row[icon_columns.status] = icon_status;

         switch (icon_status) {
         case CEPH_OSD_OUT:
            row[icon_columns.icon] = outOSDIcon;
            break;
         case ~CEPH_OSD_UP:
            row[icon_columns.icon] = downOSDIcon;
            break;
         default:
            row[icon_columns.icon] = upOSDIcon;
            break;
         }

         row[icon_columns.caption] = caption;
         row[icon_columns.begin_range] = begin_range;
         row[icon_columns.end_range] = end_range;

         range_offset += ranges[i];
      }
  }

  if (viewAll) {
    guiMonitorOSDClusterBackButton->set_sensitive(false);
    guiMonitorOSDClusterViewAllButton->set_sensitive(false);
  }

  delete[] ranges;
}

std::string GuiMonitor::
gen_mds_icon_caption(unsigned int begin, unsigned int end) const
{
  if (end == begin) {
    ostringstream oss;
    oss << "MDS " << current_up_mds.at(begin);
    return oss.str();
  }

  ostringstream oss;
  oss << "MDSes " << current_up_mds.at(begin)
      << "-" << current_up_mds.at(end);
  return oss.str();
}

void GuiMonitor::view_mds_nodes(unsigned int begin, unsigned int end, bool
 viewAll)
{
  unsigned int size;
  unsigned int *ranges = new unsigned int[MAX_VIEW_ICONS];

  // If viewAll is set, then the names set will be filled with all of the names
  // of the MDSes that are up.
  if (viewAll) {
      // Gather all of the names of the up MDSes
      set<int> up_mds;
      g.mdsmap.get_up_mds_set(up_mds);
      current_up_mds.clear();
      for (set<int>::const_iterator mds_id = up_mds.begin();
	   mds_id != up_mds.end();
	   ++mds_id)
      {
         const MDSMap::mds_info_t& info = g.mdsmap.get_mds_info(*mds_id);

         current_up_mds.push_back(info.name);
      }
      size = current_up_mds.size();
  }
  else
      size = end - begin;

  // Remove old icons
  if (viewAll) {
    while (!old_mds_cluster_zoom_states.empty())
      old_mds_cluster_zoom_states.pop();

    mds_cluster_zoom = 0;
  }

  guiMonitorMDSClusterIcons->clear();

  gen_ranges(size, MAX_VIEW_ICONS, ranges);

  // Create and display icons
  Gtk::TreeModel::Row row;
  string caption;

  int i = 0;

  for (unsigned int mdsIndex = viewAll ?  0 : begin;
	i < MAX_VIEW_ICONS;
	i++, mdsIndex += ranges[i])
  {
    if (ranges[i]) {
      row = *(guiMonitorMDSClusterIcons->append());

      row[icon_columns.icon] = MDSIcon;
      row[icon_columns.caption] =
      gen_mds_icon_caption(mdsIndex, mdsIndex + ranges[i] - 1);
      row[icon_columns.begin_range] = mdsIndex;
      row[icon_columns.end_range] = mdsIndex + ranges[i] - 1;
    }
  }

  if (viewAll) {
    guiMonitorMDSClusterBackButton->set_sensitive(false);
    guiMonitorMDSClusterViewAllButton->set_sensitive(false);
  }

  delete[] ranges;
}

std::string GuiMonitor::
gen_pg_icon_caption(unsigned int begin, unsigned int end) const
{
  boost::format rangeFormatter("PGs %lu-%lu");
  boost::format singleFormatter("PG %lu");

  return (end - begin) ?  str(rangeFormatter % begin % end) :
    str(singleFormatter % begin);
}

void GuiMonitor::
view_pg_nodes(unsigned int begin, unsigned int end, bool view_all)
{
  unsigned int size;
  unsigned int *ranges = new unsigned int[MAX_VIEW_ICONS];

  // remove old icons
  if (view_all) {
    while (!old_pg_cluster_zoom_states.empty())
      old_pg_cluster_zoom_states.pop();

    size = g.pgmap.pg_stat.size();
    pg_cluster_zoom = 0;

    current_pgs.clear();
    for (hash_map<pg_t,pg_stat_t>::const_iterator p = g.pgmap.pg_stat.begin();
	 p != g.pgmap.pg_stat.end(); ++p) {
      current_pgs.push_back(p->first);
    }
  }
  else
    size = end - begin;

  guiMonitorPGClusterIcons->clear();

  unsigned int i = 0, range_offset = begin, begin_range, end_range;
  Gtk::TreeModel::Row currentRow;

  string caption;

  gen_ranges(size, MAX_VIEW_ICONS, ranges);

  // create and display icons
  for (i = 0; i < MAX_VIEW_ICONS; i++) {
    if (ranges[i]) {
      currentRow = *(guiMonitorPGClusterIcons->append());

      begin_range = range_offset;
      end_range = ranges[i] - 1 + range_offset;

      caption = gen_pg_icon_caption(begin_range, end_range);

      currentRow[icon_columns.icon] = PGIcon;
      currentRow[icon_columns.caption] = caption;
      currentRow[icon_columns.begin_range] = begin_range;
      currentRow[icon_columns.end_range] = end_range;

      range_offset += ranges[i];
    }
  }

  if (view_all) {
    guiMonitorPGClusterBackButton->set_sensitive(false);
    guiMonitorPGClusterViewAllButton->set_sensitive(false);
  }

  delete[] ranges;
}

void GuiMonitor::check_status()
{
  // Check isCephUpdated to see if the placement groups, OSDs, metadata
  // servers, and the monitors have been updated.  If any of these have been
  // updated, then update the GUI accordingly.

  assert(g.lock.is_locked());
  if (g.updates & OSD_MON_UPDATE)
    update_osd_cluster_view();
  if (g.updates & MDS_MON_UPDATE)
    update_mds_cluster_view();
  if (g.updates & PG_MON_UPDATE)
    update_pg_cluster_view();
  if (g.updates & MON_MON_UPDATE)
    update_mon_cluster_view();
  g.updates = 0;

  // See if the log has been updated. If it has, then
  // update the log text box in the GUI and then clear the log stream.
  string log(g.slog->str());
  if (!log.empty()) {
    if (!guiMonitorCopyImageMenuItem->is_sensitive())
      guiMonitorCopyImageMenuItem->set_sensitive(true);
    Gtk::TextIter end = guiMonitorLogTextBuffer->insert(
			  guiMonitorLogTextBuffer->end(), log);
    end.backward_line();
    guiMonitorLogTextView->scroll_to(
		    guiMonitorLogTextBuffer->create_mark(end));
    g.slog->str("");
    g.slog->flush();
  }
}

// Handler for quitting the GUI via the "x" button on the window.
bool GuiMonitor::quit_gui(GdkEventAny *event)
{
  thread_shutdown();
  guiMonitorWindow->hide();
  return true;
}

// Handler for quitting the GUI via the menu option.
void GuiMonitor::gui_monitor_quit()
{
  thread_shutdown();
  guiMonitorWindow->hide();
}

// Called when the main window is closed.  Ends the program.
bool GuiMonitor::quit_signal_handler()
{
  thread_shutdown();
  Gtk::Main::quit();
  return false; // never executed
}

// Called when the "Copy" menu option in the Edit menu bar is clicked.  Copies
// the contents of the log to the clipboard.
void GuiMonitor::copy_log()
{
  Glib::RefPtr<Gtk::Clipboard> clipboard =
    Gtk::Clipboard::get(GDK_SELECTION_CLIPBOARD);

  clipboard->set_text(guiMonitorLogTextBuffer->get_text());
  clipboard->store();
}

// Called when the "Send Command..." menu option in the Ceph menu is clicked.
// Displays the "Send Command..." dialog box.
void GuiMonitor::open_send_command()
{
  sendCommandPromptEntry->set_text("");
  int result = sendCommandDialog->run();

  if (result == Gtk::RESPONSE_OK) {
    if (send_command_success) {
      send_command_success = false;
      sendCommandDialog->hide();
    }
    else
      open_send_command(); // needs to continue running
  }
  else
    sendCommandDialog->hide();
}

// Called when a button is pressed in the "Send Command..." dialog box.
void GuiMonitor::handle_send_command_response(int response)
{
  send_command_success = false;
  if (response != Gtk::RESPONSE_OK) {
    // todo: show error?
    return;
  }
  string command_str(sendCommandPromptEntry->get_text());
  boost::trim(command_str);

  if (command_str.empty()) {
    dialog_error("Please enter a Ceph command.", Gtk::MESSAGE_INFO);
    return;
  }

  run_command(command_str.c_str());

  send_command_success = true;
}

// Called when the "View Node..." menu option in the Ceph menu is clicked.
// Displays the "View Node..." dialog box.
void GuiMonitor::open_view_mode()
{
  viewNodeTypeComboBox->set_active(-1);
  viewNodeNameEntry->set_text("");
  viewNodeNameLabel->set_label("Node ID:");

  int result = viewNodeDialog->run();

  if (result == Gtk::RESPONSE_OK) {
      if (view_node_success) {
         view_node_success = false;
         viewNodeDialog->hide();
      }
      else
         open_view_mode(); // needs to continue running
  }
  else
      viewNodeDialog->hide();
}

void GuiMonitor::gen_node_info_from_icons(Glib::RefPtr<Gtk::ListStore> iconStore,
					  enum NodeType type, std::vector<NodeInfo >& ret)
{
  Gtk::TreeModel::Children icons = iconStore->children();
  Gtk::TreeModel::iterator iter;
  int status = CEPH_OSD_UP;

  for (iter = icons.begin(); iter != icons.end(); iter++) {
    Gtk::TreeModel::Row cur_row(*iter);

    NodeInfo cur(cur_row[icon_columns.begin_range],
		cur_row[icon_columns.end_range], type, status);
    ret.push_back(cur);
  }
}

void GuiMonitor::gen_icons_from_node_info(const vector<NodeInfo>& node_info) const
{
  Glib::RefPtr<Gtk::ListStore> icons;
  Gtk::TreeModel::Row row;

  switch (node_info[0].type) {
  case OSD_NODE:
      icons = guiMonitorOSDClusterIcons;
      break;
  case MDS_NODE:
      icons = guiMonitorMDSClusterIcons;
      break;
  case PG_NODE:
      icons = guiMonitorPGClusterIcons;
      break;
  default:
      break;
  }

  icons->clear();

  vector<NodeInfo>::const_iterator i = node_info.begin();
  vector<NodeInfo>::const_iterator end = node_info.end();
  for (; i != end; ++i) {
    const NodeInfo &node(*i);
    row = *(icons->append());

    row[icon_columns.begin_range] = node.begin_range;
    row[icon_columns.end_range] = node.end_range;

    switch (node.type) {
    case OSD_NODE:
      row[icon_columns.caption] =
	gen_osd_icon_caption(node.begin_range, node.end_range);
      switch (node.status) {
      case CEPH_OSD_OUT:
	row[icon_columns.icon] = outOSDIcon;
	break;
      case CEPH_OSD_UP:
	row[icon_columns.icon] = upOSDIcon;
	break;
      default: // ~CEPH_OSD_UP
	row[icon_columns.icon] = downOSDIcon;
	break;
      }
      break;

    case MDS_NODE:
      row[icon_columns.caption] = gen_mds_icon_caption(node.begin_range, node.end_range);
      row[icon_columns.icon] = MDSIcon;
      break;

    case PG_NODE:
      row[icon_columns.caption] =
	  gen_pg_icon_caption(node.begin_range, node.end_range);
      row[icon_columns.icon] = PGIcon;
      break;

    default:
      break;
    }
  }
}

// Constructs a StatsWindowInfo object and opens a window containing the stats
// of a node or cluster.
void GuiMonitor::open_stats(enum NodeType type, bool is_cluster, int id)
{
  Glib::RefPtr<Gtk::Builder> builder_file =
    Gtk::Builder::create_from_file(resource_path(STATS_WINDOW_BUILDER_FILE));

  /* note that node_stats will be deleted once the stats window closes */
  StatsWindowInfo *node_stats =
      new StatsWindowInfo(this, builder_file, is_cluster, type, id);
  node_stats->init();
  node_stats->stats_window->show_all();
}

// Called when an icon in the OSD icon view is activated.
void GuiMonitor::osdc_cluster_zoom_in(const Gtk::TreeModel::Path& path)
{
  Gtk::TreeModel::iterator iter = guiMonitorOSDClusterIcons->get_iter(path);

  if (!iter)
    return;

  Gtk::TreeModel::Row row = *iter;

  int begin_range = row[icon_columns.begin_range];
  int end_range = row[icon_columns.end_range];

  if (end_range == begin_range) {
    if (begin_range >= g.osdmap.get_max_osd()) {
      dialog_error("OSD map has changed and the node no longer exists.",
		    Gtk::MESSAGE_ERROR);

      view_osd_nodes(0, g.osdmap.get_max_osd());
    }
    else {
      open_stats(OSD_NODE, false, begin_range);
    }
  }
  else {
    open_stats(OSD_NODE, false, begin_range);
    // Zoom in, since this is a range.  Place the old icons on the stack
    // and then call view_osd_icons() on the narrower range.
    vector<NodeInfo> old_osd_info;
    gen_node_info_from_icons(guiMonitorOSDClusterIcons, OSD_NODE, old_osd_info);

    //Glib::RefPtr<Gtk::ListStore> oldOSDIcons(guiMonitorOSDClusterIcons);

    old_osd_cluster_zoom_states.push(old_osd_info);
    osd_cluster_zoom++;

    guiMonitorOSDClusterBackButton->set_sensitive(true);
    guiMonitorOSDClusterViewAllButton->set_sensitive(true);

    view_osd_nodes(begin_range, end_range + 1, false);
  }
}

// Called when the "Back" button is activated in the OSD cluster view area.
// Displays the previous level of OSDs.  Note that no updates occur to these
// nodes, however.
void GuiMonitor::osdc_cluster_back()
{
  if (osd_cluster_zoom) {
    const vector<NodeInfo> &old_osd_info = old_osd_cluster_zoom_states.top();
    gen_icons_from_node_info(old_osd_info);
    old_osd_cluster_zoom_states.pop();
    osd_cluster_zoom--;

    if (!osd_cluster_zoom) {
      guiMonitorOSDClusterBackButton->set_sensitive(false);
      guiMonitorOSDClusterViewAllButton->set_sensitive(false);
    }
  }
}

// Called when the "View All" button is activated in the OSD cluster view area.
// Displays all OSDs, with recent updates.
void GuiMonitor::osdc_cluster_view_all()
{
  view_osd_nodes(0, g.osdmap.get_max_osd());
}

// Allows user to obtain the statistics of the OSD cluster.  Called by
// guiMonitorOSDClusterStatsButton by signal_clicked.
void GuiMonitor::osdc_cluster_stats()
{
  open_stats(OSD_NODE, true, 0);
}

// Called when an icon in the MDS icon view is activated.
void GuiMonitor::mds_cluster_zoom_in(const Gtk::TreeModel::Path& path)
{
  Gtk::TreeModel::iterator iter =
    guiMonitorMDSClusterIcons->get_iter(path);

  if (!iter)
    return;
  Gtk::TreeModel::Row row = *iter;

  unsigned int begin_range = row[icon_columns.begin_range];
  unsigned int end_range = row[icon_columns.end_range];

  if (end_range - begin_range) {
    // Zoom in, since this is a range.  Place the old icons on the stack
    // and then call viewMDSIcons() on the narrower range.
    vector<NodeInfo> old_mds_info;
    gen_node_info_from_icons(guiMonitorMDSClusterIcons, MDS_NODE, old_mds_info);

    old_mds_cluster_zoom_states.push(old_mds_info);
    mds_cluster_zoom++;

    guiMonitorMDSClusterBackButton->set_sensitive(true);
    guiMonitorMDSClusterViewAllButton->set_sensitive(true);

    view_mds_nodes(begin_range, end_range + 1, false);
  }
  else {
    set<int> up_mds;
    g.mdsmap.get_up_mds_set(up_mds);

    if (begin_range >= up_mds.size()) {
      dialog_error("Metadata server map has changed and the node no "
		    "longer exists.", Gtk::MESSAGE_ERROR);

      view_mds_nodes(0, 0, true);
    }
    else {
      open_stats(MDS_NODE, false, (int)begin_range);
    }
  }
}

// Called when the "Back" button is activated in the MDS cluster view area.
// Displays the previous level of MDSes.  Note that no updates occur to these
// nodes, however.
void GuiMonitor::mds_cluster_back()
{
  if (!mds_cluster_zoom)
    return;
  const vector<NodeInfo> &old_mds_info = old_mds_cluster_zoom_states.top();
  gen_icons_from_node_info(old_mds_info);
  old_mds_cluster_zoom_states.pop();
  mds_cluster_zoom--;

  if (!mds_cluster_zoom) {
    guiMonitorMDSClusterBackButton->set_sensitive(false);
    guiMonitorMDSClusterViewAllButton->set_sensitive(false);
  }
}

// Called when the "View All" button is activated in the MDS cluster view area.
// Displays all MDSes, with recent updates.
void GuiMonitor::mds_cluster_view_all()
{
  view_mds_nodes();
}

// Allows user to obtain the statistics of the MDS cluster.  Called by
// guiMonitorMDSClusterStatsButton by signal_clicked.
void GuiMonitor::mds_cluster_stats()
{
  open_stats(MDS_NODE, true, 0);
}

// Called when an icon in the PG icon view is activated.
void GuiMonitor::pg_cluster_zoom_in(const Gtk::TreeModel::Path& path)
{
  Gtk::TreeModel::iterator iter(guiMonitorPGClusterIcons->get_iter(path));

  if (!iter)
    return;
  Gtk::TreeModel::Row row = *iter;

  unsigned int begin_range = row[icon_columns.begin_range];
  unsigned int end_range = row[icon_columns.end_range];

  if (end_range - begin_range) {
    // Zoom in, since this is a range.  Place the old icons on the stack
    // and then call viewPGIcons() on the narrower range.
    vector<NodeInfo> old_pg_info;
    gen_node_info_from_icons(guiMonitorPGClusterIcons, PG_NODE, old_pg_info);

    old_pg_cluster_zoom_states.push(old_pg_info);
    pg_cluster_zoom++;

    guiMonitorPGClusterBackButton->set_sensitive(true);
    guiMonitorPGClusterViewAllButton->set_sensitive(true);

    view_pg_nodes(begin_range, end_range + 1, false);
  }
  else {
    if (begin_range >= g.pgmap.pg_stat.size()) {
      dialog_error("Placement group map has changed and the node no "
		    "longer exists.", Gtk::MESSAGE_ERROR);

      view_pg_nodes(0, 0, true);
    }
    else
      open_stats(PG_NODE, false, (int)begin_range);
  }
}

// Called when the "Back" button is activated in the PG cluster view area.
// Displays the previous level of PGs.  Note that no updates occur to these
// nodes, however.
void GuiMonitor::pg_cluster_back()
{
  if (!pg_cluster_zoom)
    return;
  const vector<NodeInfo> &old_pg_info = old_pg_cluster_zoom_states.top();
  gen_icons_from_node_info(old_pg_info);
  old_pg_cluster_zoom_states.pop();
  pg_cluster_zoom--;

  if (!pg_cluster_zoom) {
    guiMonitorPGClusterBackButton->set_sensitive(false);
    guiMonitorPGClusterViewAllButton->set_sensitive(false);
  }
}

// Called when the "View All" button is activated in the PG cluster view area.
// Displays all PGs, with recent updates.
void GuiMonitor::pg_cluster_view_all()
{
  view_pg_nodes(0, 0, true);
}

// Allows user to obtain the statistics of the PG cluster.  Called by
// guiMonitorPCClusterStatsButton by signal_clicked.
void GuiMonitor::pg_cluster_stats()
{
  open_stats(PG_NODE, true, 0);
}

// Allows user to obtain the statistics of the monitor cluster.  Called by
// guiMonitorMonitorClusterStatsButton by signal_clicked.
void GuiMonitor::monitor_cluster_stats()
{
  open_stats(MON_NODE, true, 0);
}

void GuiMonitor::dialog_error(const std::string &msg, Gtk::MessageType type)
{
  Gtk::MessageDialog errorMsg(msg, false, type, Gtk::BUTTONS_OK, true);

  errorMsg.run();
  errorMsg.hide();
}

// Finds a metadata server in the metadata server map that is up based on its
// name.  Returns the ID of the metadata server or returns -1 if not found.
int GuiMonitor::find_mds_id(const std::string &name)
{
  set<int> upMDSes;
  g.mdsmap.get_up_mds_set(upMDSes);

  for (set<int>::const_iterator MDSId = upMDSes.begin();
	MDSId != upMDSes.end(); MDSId++)
  {
      const MDSMap::mds_info_t& current_mds_info =
	  g.mdsmap.get_mds_info(*MDSId);
      if (current_mds_info.name == name)
	return *MDSId;
  }

  return -1;
}

// Called when a button is pressed in the "View Node..." dialog box.
void GuiMonitor::handle_view_node_response(int response)
{
  view_node_success = false;
  if (response != Gtk::RESPONSE_OK) {
    // todo: display error?
    return;
  }
  Gtk::TreeModel::iterator selected_type_iter =
      viewNodeTypeComboBox->get_active();

  if (!selected_type_iter) {
    dialog_error("Type of node needs to be selected.",
      Gtk::MESSAGE_INFO);
    return;
  }
  Gtk::TreeRow selected_row = *selected_type_iter;
  enum NodeType selected_type = selected_row[viewNodeColumns.type];
  string id_entry = viewNodeNameEntry->get_text();
  boost::trim(id_entry);

  if (id_entry.empty()) {
    dialog_error("Please enter the node ID.", Gtk::MESSAGE_INFO);
    return;
  }

  switch (selected_type) {
    case OSD_NODE: {
      unsigned int id;

      try {
	id = boost::lexical_cast<unsigned int>(id_entry);
      }
      catch (const boost::bad_lexical_cast &) {
	dialog_error("Node ID must be a number.", Gtk::MESSAGE_ERROR);
	return;
      }
      unsigned int max_osd = g.osdmap.get_max_osd();
      if (id >= max_osd) {
	dialog_error("OSD does not exist.", Gtk::MESSAGE_ERROR);
	return;
      }
      open_stats(selected_type, false, (int)id);
      break;
    }

    case MDS_NODE: {
      int id = find_mds_id(id_entry);
      if (id == -1) {
	string error("Can't find MDS ");
	error += id_entry;
	dialog_error(error, Gtk::MESSAGE_ERROR);
	return;
      }
      open_stats(selected_type, false, id);
      break;
    }

    case PG_NODE: {
      unsigned int id;

      try {
	id = boost::lexical_cast<unsigned int>(id_entry);
      }
      catch (const boost::bad_lexical_cast &) {
	dialog_error("Node ID must be a number.", Gtk::MESSAGE_ERROR);
	return;
      }
      if (id >= g.pgmap.pg_stat.size()) {
	dialog_error("PG does not exist.", Gtk::MESSAGE_ERROR);
	return;
      }
      open_stats(selected_type, false, (int)id);
      break;
    }

    default:
      break;
  }
  view_node_success = true;
}

// Called when a node type is selected in the combo box in the "View Node..."
// dialog box.  Activated by signal_changed().
void GuiMonitor::handle_view_node_change()
{
  if (viewNodeTypeComboBox->get_active_row_number() == -1)
      return;

  Gtk::TreeModel::iterator selected_type_iter =
	viewNodeTypeComboBox->get_active();

  Gtk::TreeRow selected_row = *selected_type_iter;
  enum NodeType selected_type = selected_row[viewNodeColumns.type];
  std::string request_type =
    (selected_type == OSD_NODE || selected_type == PG_NODE ||
    selected_type == MON_NODE) ? "Node ID:" : "MDS Name:";

  viewNodeNameLabel->set_label(request_type);
}

// Called when the "About" menu option in the Help menu is clicked.  Displays
// the "About" dialog box.
void GuiMonitor::open_about_dialog()
{
  guiMonitorAboutDialog->run();
  guiMonitorAboutDialog->hide();
}

/*
 * GuiMonitor Subclass Methods
 */
GuiMonitor::StatsWindowInfo::StatsWindowInfo(GuiMonitor *gui_,
	  Glib::RefPtr<Gtk::Builder> builder,
	  bool is_cluster_, enum NodeType type_, int id_)
  : gui(gui_),
    is_cluster(is_cluster_),
    type(type_),
    id(id_)
{
  builder->get_widget("statsWindow", stats_window);
  builder->get_widget("statsInfoLabel", stats_info_label);
  builder->get_widget("statsInfoTreeView", stats_info_tree_view);
  builder->get_widget("statsCopyButton", stats_copy_button);
  builder->get_widget("statsCloseButton", stats_close_button);

  stats = Gtk::ListStore::create(columns);

  stats_info_tree_view->set_model(stats);
  stats_info_tree_view->append_column("Name", columns.key);
  stats_info_tree_view->append_column("Value", columns.value);

  stats_window->signal_delete_event().connect(sigc::mem_fun(this,
    &GuiMonitor::StatsWindowInfo::closeWindow));

  stats_close_button->signal_activate().connect(sigc::mem_fun(this,
    &GuiMonitor::StatsWindowInfo::close));
  stats_close_button->signal_clicked().connect(sigc::mem_fun(this,
    &GuiMonitor::StatsWindowInfo::close));

  stats_copy_button->signal_activate().connect(sigc::mem_fun(this,
    &GuiMonitor::StatsWindowInfo::copy));
  stats_copy_button->signal_clicked().connect(sigc::mem_fun(this,
    &GuiMonitor::StatsWindowInfo::copy));
}

GuiMonitor::StatsWindowInfo::~StatsWindowInfo()
{
  delete stats_window;
}

void GuiMonitor::StatsWindowInfo::init()
{
  if (is_cluster) {
      if (type == OSD_NODE)
         gen_osd_cluster_columns();
      else if (type == PG_NODE)
         gen_pg_cluster_columns();
      else if (type == MDS_NODE)
         gen_mds_cluster_columns();
      else
         gen_monitor_cluster_columns();
  }
  else {
      if (type == OSD_NODE)
         gen_osd_node_columns();
      else if (type == PG_NODE)
         gen_pg_node_columns();
      else // type == MDS_NODE
         gen_mds_node_columns();
  }
}

void GuiMonitor::StatsWindowInfo::gen_osd_cluster_columns()
{
  string label("OSD Cluster Statistics");

  stats_window->set_title(label);
  stats_info_label->set_label(label);

  {
    ostringstream oss;
    oss << g.osdmap.get_epoch();
    insert_stats("Epoch", oss.str());
  }
  {
    ostringstream oss;
    oss << g.osdmap.get_max_osd();
    insert_stats("Maximum Amount of OSDs", oss.str());
  }
  {
    ostringstream oss;
    oss << g.osdmap.get_num_up_osds();
    insert_stats("Amount of Up OSDs", oss.str());
  }
  {
    ostringstream oss;
    oss << g.osdmap.get_num_in_osds();
    insert_stats("Amount of In OSDs", oss.str());
  }
}

void GuiMonitor::StatsWindowInfo::gen_mds_cluster_columns()
{
  string label("MDS Cluster Statistics");

  stats_window->set_title(label);
  stats_info_label->set_label(label);

  set<int> up_mds;
  set<int> stopped_mds;

  g.mdsmap.get_up_mds_set(up_mds);
  g.mdsmap.get_stopped_mds_set(stopped_mds);

  insert_stats("Epoch", str(boost::format("%llu") % g.mdsmap.get_epoch()));
  insert_stats("Maximum Amount of MDSes", str(boost::format("%u") %
    g.mdsmap.get_max_mds()));
  insert_stats("Amount of Up MDSes", str(boost::format("%d") % up_mds.size()));
  insert_stats("Amount of In MDSes", str(boost::format("%u") %
    g.mdsmap.get_num_mds()));
  insert_stats("Amount of Failed MDSes", str(boost::format("%d") %
    g.mdsmap.get_num_failed()));
  insert_stats("Amount of Stopped MDSes", str(boost::format("%d") %
    stopped_mds.size()));
}

void GuiMonitor::StatsWindowInfo::gen_pg_cluster_columns()
{
  string label("PG Cluster Statistics");

  stats_window->set_title(label);
  stats_info_label->set_label(label);

  {
    ostringstream oss;
    oss << g.pgmap.version;
    insert_stats("Version", oss.str());
  }

  {
    ostringstream oss;
    oss << g.pgmap.pg_stat.size();
    insert_stats("Amount of PGs", oss.str());
  }

  {
    ostringstream oss;
    oss << kb_t(g.pgmap.pg_sum.num_kb);
    insert_stats("Data ", oss.str());
  }

  {
    ostringstream oss;
    oss << kb_t(g.pgmap.osd_sum.kb_used);
    insert_stats("Amount of Storage Used", oss.str());
  }

  {
    ostringstream oss;
    oss << kb_t(g.pgmap.osd_sum.kb_avail);
    insert_stats("Amount of Storage Available", oss.str());
  }

  {
    ostringstream oss;
    oss << kb_t(g.pgmap.osd_sum.kb);
    insert_stats("Total Storage", oss.str());
  }
}

void GuiMonitor::StatsWindowInfo::gen_monitor_cluster_columns()
{
  string label("Monitor Cluster Statistics");
  stats_window->set_title(label);
  stats_info_label->set_label(label);
  {
    ostringstream oss;
    oss << g.mc.monmap.size();
    insert_stats("Amount of Monitors", oss.str());
  }
  {
    ostringstream oss;
    oss << g.mc.monmap.epoch;
    insert_stats("Epoch", oss.str());
  }
  {
    ostringstream oss;
    oss << g.mc.monmap.fsid;
    insert_stats("File System ID", oss.str());
  }
  {
    ostringstream oss;
    oss << g.mc.monmap.last_changed;
    insert_stats("Last Changed", oss.str());
  }
  {
    ostringstream oss;
    oss << g.mc.monmap.created;
    insert_stats("Created", oss.str());
  }
}

void GuiMonitor::StatsWindowInfo::gen_osd_node_columns()
{
  bool isIn = g.osdmap.is_in(id);
  const entity_addr_t& addr = g.osdmap.get_addr(id);
  const osd_info_t& osdInfo = g.osdmap.get_info(id);

  string label(str(boost::format("OSD %lu Statistics:") % id));

  stats_window->set_title(label);
  stats_info_label->set_label(label);

  insert_stats("OSD ID", str(boost::format("%lu") % id));
  insert_stats("Address", isIn ? addr_to_str(addr) : "None");
  insert_stats("Up?", g.osdmap.is_up(id) ? "Up" : "Down");
  insert_stats("In?", isIn ? "In" : "Out");
  insert_stats("Weight", isIn ? str(boost::format("%f") %
      g.osdmap.get_weightf(id)) : "None");
  insert_stats("Up From", str(boost::format("%lu") % osdInfo.up_from));
  insert_stats("Up Through", str(boost::format("%lu") % osdInfo.up_thru));
  insert_stats("Down At", str(boost::format("%lu") % osdInfo.down_at));
  insert_stats("Last Clean", str(boost::format("First: %lu, Last: %lu") %
    osdInfo.last_clean_first % osdInfo.last_clean_last));
}

void GuiMonitor::StatsWindowInfo::gen_mds_node_columns()
{
  const MDSMap::mds_info_t &mdsInfo = g.mdsmap.get_mds_info(id);

  string label(str(boost::format("MDS %s Statistics:") % mdsInfo.name));

  stats_window->set_title(label);
  stats_info_label->set_label(label);

  insert_stats("Name", mdsInfo.name);
  insert_stats("Address", addr_to_str(mdsInfo.addr));
  insert_stats("Rank", str(boost::format("%lu") % mdsInfo.rank));
  insert_stats("Laggy?", mdsInfo.laggy() ? "Yes" : "No");
  insert_stats("State", ceph_mds_state_name(mdsInfo.state));
}

void GuiMonitor::StatsWindowInfo::gen_pg_node_columns()
{
  pg_t pg(gui->current_pgs.at(id));

  hash_map<pg_t,pg_stat_t>::const_iterator s = g.pgmap.pg_stat.find(pg);
  assert(s != g.pgmap.pg_stat.end());

  const pg_stat_t &stat(s->second);
  std::stringstream converter;

  string label(str(boost::format("PG %lu Statistics:") % id));

  stats_window->set_title(label);
  stats_info_label->set_label(label);

  insert_stats("Number of Objects",
    str(boost::format("%llu") % stat.num_objects));
  insert_stats("Number of Objects Missing on Primary",
    str(boost::format("%llu") % stat.num_objects_missing_on_primary));
  insert_stats("Number of Objects Degraded",
    str(boost::format("%llu") % stat.num_objects_degraded));
  insert_stats("Number of Objects Unfound",
    str(boost::format("%llu") % stat.num_objects_unfound));
  insert_stats("KB", str(boost::format("%llu") % stat.num_kb));
  insert_stats("Bytes", str(boost::format("%llu") % stat.num_bytes));
  insert_stats("Log Size", str(boost::format("%llu") % stat.log_size));
  insert_stats("On-Disk Log Size", str(boost::format("%llu") %
  stat.ondisk_log_size));
  insert_stats("State", pg_state_string(stat.state));

  ostringstream vss;
  vss << stat.version;
  insert_stats("Version", vss.str());

  ostringstream rss;
  rss << stat.reported;
  insert_stats("Reported", rss.str());

  converter << stat.up;

  insert_stats("Up", converter.str());

  converter.str("");
  converter.flush();

  converter << stat.acting;

  insert_stats("Acting", converter.str());

  converter.str("");
  converter.flush();

  converter << stat.last_scrub;

  insert_stats("Last Scrub", converter.str());

  converter.str("");
  converter.flush();

  converter << stat.last_scrub_stamp;

  insert_stats("Last Scrub Timestamp", converter.str());

  converter.str("");
  converter.flush();
}

// Copies the information inside of the tree view object that contains the
// information about a node or cluster.  Called by nodeStatsCopyButton by
// signal_clicked.
void GuiMonitor::StatsWindowInfo::copy()
{
  Glib::RefPtr<Gtk::Clipboard> clipboard =
      Gtk::Clipboard::get(GDK_SELECTION_CLIPBOARD);

  clipboard->set_text(stats_string());
  clipboard->store();
}

// Converts the statistics into a string
std::string GuiMonitor::StatsWindowInfo::stats_string()
{
  ostringstream oss;
  Gtk::TreeModel::Children rows = stats->children();
  string delim;

  for (Gtk::TreeModel::iterator iter = rows.begin();
      iter != rows.end(); iter++)
  {
    oss << delim;
    Gtk::TreeModel::Row currentRow(*iter);
    Glib::ustring key(currentRow[columns.key]);
    Glib::ustring value(currentRow[columns.value]);
    oss << key << '\t' << value;
    delim = "\n";
  }

  return oss.str();
}

void GuiMonitor::StatsWindowInfo::insert_stats(const std::string &key,
				const std::string &value)
{
  Gtk::TreeModel::Row currentRow = *(stats->append());
  currentRow[columns.key] = key;
  currentRow[columns.value] = value;
}

int run_gui(int argc, char **argv)
{
  int ret = EXIT_SUCCESS;
  GuiMonitor *gui = NULL;

  ret = calculate_resource_path_prefix(argv[0]);
  if (ret) {
    derr << "Couldn't find GUI resource files!" << dendl;
    goto done;
  }

  try {
    // Now that the monitor map is up and running, initialize the GUI and start
    // sending observe requests.  The observe requests will update the
    // guiMonitorWindow (the main window of the Ceph monitor).  Observe requests
    // will continue to be sent until the application is closed or until the file
    // system is no longer running.
    Gtk::Main kit(argc, argv);

    // read the Gtk::Builder file that contains the layout of the windows.
    Glib::RefPtr<Gtk::Builder> builder =
	  Gtk::Builder::create_from_file(resource_path(GUI_MONITOR_BUILDER_FILE));

    // stores pointers to all GUI elements
    gui = new GuiMonitor(builder);

    if (!gui->init()) {
      derr << "There was a problem with initializing the GUI." << dendl;
      ret = EXIT_FAILURE;
      goto done;
    }

    // The GUI will now enter its main run loop and will
    // not exit until the GUI closes.
    gui->run_main_loop(kit);
  }
  catch(const Gtk::BuilderError& ex) {
    derr << "Gtk BuilderError: " << ex.what() << dendl;
    ret = EXIT_FAILURE;
    goto done;
  }
  catch(const Glib::FileError& ex) {
    derr << "FileError: " << ex.what() << dendl;
    ret = EXIT_FAILURE;
    goto done;
  }
  catch (const Glib::Exception &e) {
    derr << "got Glib exception: " << e.what() << dendl;
    ret = EXIT_FAILURE;
    goto done;
  }
  catch (const std::exception &e) {
    derr << "got exception: " << e.what() << dendl;
    ret = EXIT_FAILURE;
    goto done;
  }

done:
  delete gui;
  return ret;
}
