// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * gui.h
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

#ifndef CEPH_GUI_H
#define CEPH_GUI_H

#include "common/common_init.h"
#include "mds/MDSMap.h"
#include "mon/MonMap.h"
#include "mon/PGMap.h"
#include "msg/tcp.h"
#include "osd/OSDMap.h"

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <gtkmm.h>
#include <iosfwd>
#include <stack>

enum NodeType {
   OSD_NODE = 0,
   PG_NODE,
   MDS_NODE,
   MON_NODE,
   NUM_NODE_TYPES
};

class GuiMonitorThread;

// Contains information about a range of nodes (or a specific node)
class NodeInfo {
public:
   unsigned int begin_range;
   unsigned int end_range;
   enum NodeType type;
   int status;

   NodeInfo(unsigned int begin_range_, unsigned int end_range_,
	    enum NodeType type_, int status_)
     : begin_range(begin_range_),
       end_range(end_range_),
       type(type_),
       status(status_)
   {
   }
};

// Variables, classes, and methods releated to handling the main window and
// dialog boxes are stored in the GuiMonitor class.
class GuiMonitor
{
private:
  // Used for displaying icons of the nodes in a cluster.
  class NodeIconColumns : public Gtk::TreeModel::ColumnRecord {
  public:
    Gtk::TreeModelColumn<Glib::RefPtr<Gdk::Pixbuf> > icon;
    Gtk::TreeModelColumn<Glib::ustring> caption;
    Gtk::TreeModelColumn<unsigned int> begin_range;
    Gtk::TreeModelColumn<unsigned int> end_range;
    Gtk::TreeModelColumn<int> status;

    NodeIconColumns() {
      add(icon);
      add(caption);
      add(begin_range);
      add(end_range);
      add(status);
    }
  };

   // Used for displaying information about either a cluter or a node.
   class KeyValueModelColumns : public Gtk::TreeModel::ColumnRecord {
   public:
      Gtk::TreeModelColumn<Glib::ustring> key;
      Gtk::TreeModelColumn<Glib::ustring> value;

      KeyValueModelColumns() {
         add(key);
         add(value);
      }
   };

   // Used for displaying different types of nodes in the "View Node..." dialog
   // box.
   class ViewNodeOptionColumns : public Gtk::TreeModel::ColumnRecord {
   public:
      Gtk::TreeModelColumn<enum NodeType> type;
      Gtk::TreeModelColumn<Glib::ustring> name;

      ViewNodeOptionColumns() {
         add(type);
         add(name);
      }
   };

   // Variables, classes, and methods related to the node statistics and cluster
   // statistics windows.
   class StatsWindowInfo {
    public:
      StatsWindowInfo(GuiMonitor *gui_,
	  Glib::RefPtr<Gtk::Builder> builder,
	  bool is_cluster_, enum NodeType type_, int id_);

      ~StatsWindowInfo();
      void init();

   public:
      /*
       * GUI Elements
       */
      GuiMonitor *gui;
      Gtk::Window *stats_window;
      Gtk::Label *stats_info_label;
      Gtk::TreeView *stats_info_tree_view;
      Gtk::Button *stats_copy_button;
      Gtk::Button *stats_save_button;
      Gtk::Button *stats_close_button;

    private:
      KeyValueModelColumns columns;
      Glib::RefPtr<Gtk::ListStore> stats;

      bool is_cluster;
      enum NodeType type;
      int id;

      CephToolCtx *ctx;

      /*
       * Private Functions
       */
   private:
      // Closes the window that contains the statistics of a node or cluster.
      // Called by nodeStatsWindow by signal_delete_event.
      bool closeWindow(GdkEventAny *event) {
         delete this;
	 return true;
      }

      void copy();

      // Closes the window that contains the statistics of a node or cluster.
      // Called by nodeStatsCloseButton by signal_clicked.
      void close() {
         delete this;
      }

      void gen_osd_cluster_columns();
      void gen_mds_cluster_columns();
      void gen_pg_cluster_columns();
      void gen_monitor_cluster_columns();

      void gen_osd_node_columns();
      void gen_mds_node_columns();
      void gen_pg_node_columns();

      std::string stats_string();

      void insert_stats(const std::string &key, const std::string &value);
   };

   friend class StatsWindowInfo;

public:
  GuiMonitor(Glib::RefPtr<Gtk::Builder> builder, CephToolCtx *ctx_);
  ~GuiMonitor();

  bool init();
  void run_main_loop(Gtk::Main &kit);
  void thread_shutdown();

  void check_status();

private:
  /*
   * Private Functions
   */
  bool open_icon(Glib::RefPtr<Gdk::Pixbuf> &icon, const std::string &path);
  void connect_signals();
  void link_elements();
  void update_osd_cluster_view();
  void update_mds_cluster_view();
  void update_pg_cluster_view();
  void update_mon_cluster_view();

  std::string gen_osd_icon_caption(unsigned int begin, unsigned int end) const;
  std::string gen_mds_icon_caption(unsigned int begin, unsigned int end) const;
  std::string gen_pg_icon_caption(unsigned int begin, unsigned int end) const;

  void gen_node_info_from_icons(Glib::RefPtr<Gtk::ListStore> iconStore,
				enum NodeType type, std::vector<NodeInfo >& ret);
  void gen_icons_from_node_info(const std::vector<NodeInfo>& node_info) const;

  void view_osd_nodes(unsigned int begin, unsigned int end,
		      bool view_all = true);
  void view_mds_nodes(unsigned int begin = 0, unsigned int end = 0,
		      bool view_all = true);
  void view_pg_nodes(unsigned int begin, unsigned int end,
		     bool view_all);
  int find_mds_id(const std::string &name);
  void open_stats(enum NodeType type, bool is_cluster, int id);
  void dialog_error(const std::string &msg, Gtk::MessageType type);

  /*
   * Signal handlers
   */
  // Exits the program.  Called by Gtk::Main by signal_quit
  bool quit_signal_handler();

  // Quits the GUI.  Called by guiMonitorWindow by signal_delete_event.
  bool quit_gui(GdkEventAny *event);

  // Quits the GUI.  Called by guiMonitorQuitImageMenuItem by signal_activate.
  void gui_monitor_quit();

  // Copies the text in guiMonitorLogTextView onto the clipboard.  Called by
  // guiMonitorCopyImageMenuItem by signal_activate.
  void copy_log();

  // Opens the "Send Command...." window.  Called by
  // guiMonitorSendCommandMenuItem for signal_activate.
  void open_send_command();

  // Opens the "View Node..." window.  Called by guiMonitorViewNodeMenuItem by
  // signal_activate.
  void open_view_mode();

  // Opens the "About" dialog box.  Called by guiMonitorAboutImageMenuItem by
  // signal_activate.
  void open_about_dialog();

  // Performs "zoom in" option for the PG cluster icon view area.  Allows the
  // user to "zoom in" on a narrower range of nodes until he or she selects a
  // specific node of interest.  Called by guiMonitorPGClusterIconView by
  // signal_item_activated.
  void pg_cluster_zoom_in(const Gtk::TreeModel::Path& path);

  // Allows user to "zoom out" one level of the PG cluster view area to obtain
  // a broader range of nodes until he or she views the overall amount of
  // nodes.  Called by guiMonitorPGClusterBackButton by signal_clicked
  void pg_cluster_back();

  // Allows user to view the broadest range of nodes in the PG cluster.  Called
  // by guiMonitorPGClusterViewAllButton by signal_clicked.
  void pg_cluster_view_all();

  // Allows user to obtain the statistics of the PG cluster.  Called by
  // guiMonitorPCClusterStatsButton by signal_clicked.
  void pg_cluster_stats();

  // Allows user to obtain the statistics of the monitor cluster.  Called by
  // guiMonitorMonitorClusterStatsButton by signal_clicked.
  void monitor_cluster_stats();

  // Performs "zoom in" option for the OSD cluster icon view area.  Allows the
  // user to "zoom in" on a narrower range of nodes until he or she selects a
  // specific node of interest.  Called by guiMonitorOSDClusterIconView by
  // signal_item_activated.
  void osdc_cluster_zoom_in(const Gtk::TreeModel::Path& path);

  // Allows user to "zoom out" one level of the OSD cluster view area to obtain
  // a broader range of nodes until he or she views the overall amount of
  // nodes.  Called by guiMonitorOSDClusterBackButton by signal_clicked
  void osdc_cluster_back();

  // Allows user to view the broadest range of nodes in the PG cluster.  Called
  // by guiMonitorOSDClusterViewAllButton by signal_clicked.
  void osdc_cluster_view_all();

  // Allows user to obtain the statistics of the OSD cluster.  Called by
  // guiMonitorOSDClusterStatsButton by signal_clicked.
  void osdc_cluster_stats();

  // Performs "zoom in" option for the MDS cluster icon view area.  Allows the
  // user to "zoom in" on a narrower range of nodes until he or she selects a
  // specific node of interest.  Called by guiMonitorMDSClusterIconView by
  // signal_item_activated.
  void mds_cluster_zoom_in(const Gtk::TreeModel::Path& path);

  // Allows user to "zoom out" one level of the MDS cluster view area to obtain
  // a broader range of nodes until he or she views the overall amount of
  // nodes.  Called by guiMonitorMDSClusterBackButton by signal_clicked.
  void mds_cluster_back();

  // Allows user to view the broadest range of nodes in the MDS cluster.
  // Called by guiMonitorMDSClusterViewAllButton by signal_clicked.
  void mds_cluster_view_all();

  // Allows user to obtain the statistics of the MDS cluster.  Called by
  // guiMonitorMDSClusterStatsButton by signal_clicked.
  void mds_cluster_stats();

  // Called when a button is pressed in the "View Node..." dialog box.
  void handle_view_node_response(int response);

  // Called when a node type is selected in the "View Node..." dialog box.
  void handle_view_node_change();

  // Called when a button is pressed in the "Send Command..." dialog box.
  void handle_send_command_response(int response);

  unsigned int pg_cluster_zoom;  // current zoom level of the PG cluster view
  unsigned int osd_cluster_zoom; // current zoom level of the OSD cluster view
  unsigned int mds_cluster_zoom; // current zoom level of the MDS cluster view

  stack<vector<NodeInfo> > old_pg_cluster_zoom_states;
  stack<vector<NodeInfo> > old_osd_cluster_zoom_states;
  stack<vector<NodeInfo> > old_mds_cluster_zoom_states;

  std::vector <std::string> current_up_mds;
  std::vector <pg_t> current_pgs;

  bool send_command_success; // did "Send Command..." end without errors?
  bool view_node_success;    // did "View Node..." end without errors?"

  GuiMonitorThread *thread;

  ///// GUI Elements /////
  // Main Window
  Gtk::Window *guiMonitorWindow;

  Gtk::ImageMenuItem *guiMonitorQuitImageMenuItem;
  Gtk::ImageMenuItem *guiMonitorCopyImageMenuItem;
  Gtk::MenuItem *guiMonitorSendCommandMenuItem;
  Gtk::MenuItem *guiMonitorViewNodeMenuItem;
  Gtk::ImageMenuItem *guiMonitorAboutImageMenuItem;
  Gtk::TextView *guiMonitorLogTextView;
  Glib::RefPtr<Gtk::TextBuffer> guiMonitorLogTextBuffer;
  Gtk::Statusbar *guiMonitorStatusbar;

  // Main Window -- Placement Groups Section
  Gtk::Label *guiMonitorPGClusterStatsLabel;
  Gtk::IconView *guiMonitorPGClusterIconView;
  Glib::RefPtr<Gtk::ListStore> guiMonitorPGClusterIcons;
  Gtk::Button *guiMonitorPGClusterBackButton;
  Gtk::Button *guiMonitorPGClusterViewAllButton;
  Gtk::Button *guiMonitorPGClusterStatsButton;

  // Main Window -- Monitor Cluster Section
  Gtk::Label *guiMonitorMonitorClusterStatsLabel;
  Gtk::Button *guiMonitorMonitorClusterStatsButton;
  Gtk::TreeView *guiMonitorMonitorClusterTreeView;
  Glib::RefPtr<Gtk::ListStore> guiMonitorMonitorClusterEntries;
  KeyValueModelColumns monitorColumns;

  // Main Window -- OSD Cluster Section
  Gtk::Label *guiMonitorOSDClusterStatsLabel;
  Gtk::IconView *guiMonitorOSDClusterIconView;
  Glib::RefPtr<Gtk::ListStore> guiMonitorOSDClusterIcons;
  Gtk::Button *guiMonitorOSDClusterBackButton;
  Gtk::Button *guiMonitorOSDClusterViewAllButton;
  Gtk::Button *guiMonitorOSDClusterStatsButton;

  // Main Window -- Metadata Server Section
  Gtk::Label *guiMonitorMDSClusterStatsLabel;
  Gtk::IconView *guiMonitorMDSClusterIconView;
  Glib::RefPtr<Gtk::ListStore> guiMonitorMDSClusterIcons;
  Gtk::Button *guiMonitorMDSClusterBackButton;
  Gtk::Button *guiMonitorMDSClusterViewAllButton;
  Gtk::Button *guiMonitorMDSClusterStatsButton;

  // View Node Dialog
  Gtk::Dialog *viewNodeDialog;
  Gtk::Entry *viewNodeNameEntry;
  Gtk::ComboBox *viewNodeTypeComboBox;
  Glib::RefPtr<Gtk::ListStore> viewNodeNodeTypeListStore;
  Gtk::Label *viewNodeNameLabel;
  ViewNodeOptionColumns viewNodeColumns;

  // Send Command Window
  Gtk::Dialog *sendCommandDialog;
  Gtk::Entry *sendCommandPromptEntry;

  // About Dialog
  Gtk::AboutDialog *guiMonitorAboutDialog;

  // Icons
  Glib::RefPtr<Gdk::Pixbuf> blacklistIcon;
  Glib::RefPtr<Gdk::Pixbuf> clientIcon;
  //Glib::RefPtr<Gdk::Pixbuf> failedMDSIcon;
  Glib::RefPtr<Gdk::Pixbuf> MDSIcon;
  Glib::RefPtr<Gdk::Pixbuf> monitorIcon;
  Glib::RefPtr<Gdk::Pixbuf> upOSDIcon;
  Glib::RefPtr<Gdk::Pixbuf> downOSDIcon;
  Glib::RefPtr<Gdk::Pixbuf> outOSDIcon;
  Glib::RefPtr<Gdk::Pixbuf> PGIcon;
  //Glib::RefPtr<Gdk::Pixbuf> stoppedMDSIcon;

  NodeIconColumns icon_columns;
public:
  CephToolCtx *ctx;
};

#endif
