#!env python3
"""
This script expects as input a list of .out files produced from a client perf_crimson_msgr:

(We might consider to refact this to use as input a .json config file to describe the list of input .out files,
potentially with the option to compare against another run)

for x in *.zip; do 
  y=${x/.zip/_client.out};
  echo "== $y ==";
  unzip -c $x $y | tail -8; 
done > msgr_crimson_bal_vs_sep_client.out

It produces a .json file with the following layout: main key is the CPU balance
value, each of which can be converted to a pandas dataframe

{
 "balanced":{
 "smp": [ 2, 4, 8, 14, 28 ],
 "clients": [ 2, 4, 8, 14, 28 ],
 "latency": [ 7.694677, 7.694677, 7.694677, 7.694677, 7.694677 ],
 "throughput": [ 1039.688596, 1039.688596, 1039.688596, 1039.688596, 1039.688596 ],
 },
 "separated":{
 # same layout as above
 }
}

python pp_perf_msgr.py -d $MYDIR -i msgr_crimson_bal_vs_sep_client.out -v

Produces combined .json (table/dataframe) with the Latency vs Throughput results (and CPU util)

Example:
_b1e4a2b_round_02/msgr_crimson_07/msgr_crimson_bal_vs_sep_clientdes.json
"""

import argparse
import logging
import os
import sys
import re
import json
import tempfile
from matplotlib import legend
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Any
from pprint import pformat
#from gnuplot_plate import GnuplotTemplate
from common import load_json, save_json

__author__ = "Jose J Palacios-Perez"

logger = logging.getLogger(__name__)


class MsgrStatEntry(object):
    """
    Simply capture the following summary from the .out file:

    == msgr_crimson_2smp_2clients_balanced_client.out ==
    all(depth=2048):
      connect time: 0.023088s
      messages received: 47921197
      messaging time: 180.046387s
      latency: 7.694677ms
      IOPS: 266160.280455
      out throughput: 1039.688596MB/s
    """

    def __init__(self, iflname: str, out_json: str, directory: str, plot: str = ""):
        """
        This class expects an input list of messenger result files to process
        The result is a dict with keys the number of reactors/SMP, number of clients,
        the CPU balance strategy (separated, NUMA balanced)
        """
        self.iflname = iflname
        self.plot = plot
        if iflname:
            if out_json:
                self.oflname = out_json
            else:
                self.oflname = iflname.replace(".out", ".json")
                self.dflname = iflname.replace(".out", "_des.json")
        else:
            self.oflname = plot
        # Gneric key:value regex
        self._gen_key_value = [
            re.compile(r"^([^:]+): (.*)$"),  # , re.DEBUG)
        ]
        self.descriptions = {
            "smp": [],
            "clients": [],
            "balance": [],
        }
        # Current description is the latest
        self.description = {
            "regex": re.compile(
                r"^== msgr_(crimson|async)_(\d+)smp_(\d+)clients_(balanced|separated)_client.out =="
            ),
        }
        self.regex_start = re.compile(r"all\(depth=\d+\):")
        self.measurements = {
            "connect_time": {
                "regex": re.compile(r"connect time: (.*)s"),
                "unit": "s",
                "value": None,
            },
            "messages_received": {
                "regex": re.compile(r"messages received: (.*)"),
                "unit": "",
                "value": None,
            },
            "messaging_time": {
                "regex": re.compile(r"messaging time: (.*)s"),
                "unit": "s",
                "value": None,
            },
            "latency": {
                "regex": re.compile(r"latency: (.*)ms"),
                "unit": "ms",
                "value": None,
            },
            "IOPS": {
                "regex": re.compile(r"IOPS: (.*)"),
                "unit": "",
                "value": None,
            },
            "out_throughput": {
                "regex": re.compile(r"out throughput: (.*)MB/s"),
                "unit": "MB/s",
                "value": None,
            },
        }

        self.directory = directory
        self.entry: Dict[str, Dict[str, List[Any]]] = {}

    def parse_entry(self, line):
        """
        Parse an entry and update the measurements
        """
        match = self.description["regex"].search(line)
        logger.debug(f"Line: {line}, Match: {match}")
        if match:
            self.descriptions["smp"].append(int(match.group(2)))
            self.descriptions["clients"].append(int(match.group(3)))
            self.descriptions["balance"].append(match.group(4))
            _bal = self.descriptions["balance"][-1]
            if _bal not in self.entry:
                self.entry.update({_bal: {}})
            return True
        else:
            return False

    def update_descriptions(self, metric, value):
        """
        Update the descriptions with the metric and value
        This is a flat table, with columns each metric, including the smp and num clients
        """
        if metric not in self.descriptions:
            self.descriptions.update({metric: []})
        self.descriptions[metric].append(value)

    def parse_measurements(self, line, it_lines):
        """
        Parse a line from the iterator and update the measurements
        """
        match = self.regex_start.search(line)
        logger.debug(f"Line: {line}, Match: {match}")
        if match:
            line = next(it_lines, None)
            while line:
                for key, value in self.measurements.items():
                    match = value["regex"].search(line)
                    logger.debug(f"Line: {line}, Match: {match}")
                    if match:
                        _bal = self.descriptions["balance"][-1]
                        _d = self.entry[_bal]
                        if key not in _d:
                            _d.update({key: []})
                        _d[key].append(match.group(1))
                        self.update_descriptions(key, match.group(1))
                        break
                line = next(it_lines, None)

    def load_files(self, fname):
        """
        Load a file containing summary perf_crimson_msgr metrics
        Returns a pandas dataframe
        TBC. we might expect a generic config .json file with the list of files to process
        """
        lines = []
        try:
            with open(fname, "r") as data:
                f_info = os.fstat(data.fileno())
                if f_info.st_size == 0:
                    logger.error(f"input file {fname} is empty")
                    return []
                lines = data.read().splitlines()
                data.close()
        except IOError as e:
            raise argparse.ArgumentTypeError(str(e))
        it_lines = iter(lines)
        for line in it_lines:
            if self.parse_entry(line):
                self.parse_measurements(next(it_lines, None), it_lines)

        logger.debug(
            f"Entry: {self.entry}\n Descriptions: {pformat(self.descriptions)}"
        )
        save_json(self.oflname, self.entry)
        save_json(self.dflname, self.descriptions)
        # Circular reference
        # self.save_json(self.oflname, { "entries": self.entry, "descriptions": self.descriptions })
        return pd.DataFrame(self.entry)

    def make_response_chart(self, df, title):
        """
        Plot a response graph (IOPs vs Latency) of the dataframe

        #print(f"Title:{title} df: {df}")
        sns.set_theme()
        # Using "latency" and "out_throughput" for the x,y axis, resp
        #sns.relplot(data=df, x="latency", y="out_throughput", hue="balance", kind="line") #, title=title)
        sns.lineplot(data=df, x="latency", y="out_throughput", hue="balance")
        #plt.show()
        plt.savefig(self.oflname.replace(".json", "_rc.png"), dpi=300, bbox_inches='tight')

        sns.heatmap(flights, cmap='Blues', annot=True, fmt='d')
        plt.title('Passengers per month')
        plt.xlabel('Year')
        plt.ylabel('Month')
        # Show the plot
        plt.show()

        #df.rename(columns={"out_throughput": "out_throughput (K)"}, inplace=True) #.astype(float)
        x_data = df["latency"]
        # Needs scaling out_throughput to thousands
        y_data =  df["out_throughput"].astype(float) / 1000.0
        # plt.scatter(x_data, y_data, c=df["smp"], cmap='viridis')
        plt.plot(x_data, y_data, "+-", label=title)

        TODO: filter out data from the dataframe if the corresponding latency is higher than 10ms
        """
        # Format column latency to float .2f
        df["latency"] = df["latency"].astype(float).map("{:.2f}".format)
        # Using "latency" and "out_throughput" for the x,y axis, resp
        # sns.relplot(data=df, x="latency", y="out_throughput", hue="balance", kind="line") #, title=title)
        g = sns.relplot(
            data=df,
            x="latency",
            y="IOPS",
            hue="balance",
            style="balance",
            kind="line",
            markers=True,
        ).set(title=title)
        g.set_axis_labels("Latency(ms)", "IOPS (K)")
        g.set(xticks=df["latency"].unique())
        #df.dataframe(df.style.format(subset=['Position', 'Marks'], formatter="{:.2f}"))
        g.set_xticklabels(rotation=45)
        g.legend.remove()
        plt.legend(title="CPU Balance", loc="center right")
        # plt.show()
        out_name = self.oflname.replace(".json", "_rc.png")
        logger.debug(f"Saving chart: {out_name}")
        plt.savefig(out_name, dpi=100, bbox_inches="tight")

    def save_table(self, name, df):
        """
        Save the df in latex format
        """
        if name:
            with open(name, "w", encoding="utf-8") as f:
                print(df.to_latex(), file=f)
                f.close()

    def make_simple_chart(self, df, title):
        """
        Plot a simple chart of the dataframe
        """
        print(f"DF:\n {df}")
        df.sort_values(by=["smp", "clients"], inplace=True)
        print(f"DF sorted:\n {df}")
        # Apply conversion to thousands to IOPS columns
        df["IOPS"] = df["IOPS"].astype(float) / 1000.0
        # df.rename(columns={"IOPS": "IOPS (K)"}, inplace=True) #.astype(float)
        print(f"DF sorted and converted to thousands:\n {df}")
        sns.set_theme(
            style="darkgrid", rc={"figure.figsize": (6.5, 4.2)}
        )  # width=8, height=4
        g = sns.relplot(
            data=df,
            x="clients",
            y="IOPS",
            hue="balance",
            style="balance",
            kind="line",
            markers=True,
        ).set(title=title)
        g.set_axis_labels("Clients", "IOPS (K)")
        g.set(xticks=df["clients"].unique())
        g.legend.remove()
        plt.legend(title="CPU Balance", loc="center right")
        # plt.show()
        out_name = self.oflname.replace(".json", "_table.tex")
        logger.debug(f"Saving table: {out_name}")
        self.save_table(out_name, df)

        out_name = self.oflname.replace(".json", "_iops.png")
        logger.debug(f"Saving chart: {out_name}")
        plt.savefig(out_name, dpi=100, bbox_inches="tight")
        # plt.clf()

    def prep_response_charts(self):
        """
        Traverse the entry dict to produce response charts
        z = self.entry | self.descriptions # don't work in 3.09
        # z = self.entry.copy()
        # z.update(self.descriptions)
        print(f"descriptions: {self.descriptions}")
        df = pd.DataFrame(self.descriptions)
        print(f"DF: {df}")
        df.sort_values(by=["smp", "clients"], inplace=True)
        print(f"DF: {df}")

                for key, value in self.entry.items():  # balanced, separated
                    df = pd.DataFrame(value)
                    self.make_response_chart(df, key)
                    #self.make_response_chart(value, key)
        """
        if self.plot:
            regex = re.compile(r"des.json")
            if regex.search(self.plot):
                df = pd.DataFrame(self.entry)
                self.make_simple_chart(df, self.plot)
                self.make_response_chart(df, "Response Latency vs Throughput")

    def run(self):
        """
        Entry point: produces a dataframe  from the .json file
        """
        os.chdir(self.directory)
        if self.plot:
            self.entry = load_json(self.plot).pop()
            self.prep_response_charts()
            # self.make_response_chart(pd.read_json(self.plot), "Response Chart")
            return
        self.load_files(self.iflname)
        self.prep_response_charts()


def main(argv):
    examples = """
    Examples:
    # Produce a dataframe index by smp/clients and a response chart Throughput vs Latency:
    #  %prog -i msgr_crimson_bal_vs_sep_client.out -o msgr_crimson_bal_vs_sep_client.json
    """
    parser = argparse.ArgumentParser(
        description="""This tool is used to post-process perf-crimson-msgr measurements""",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cmd_grp = parser.add_mutually_exclusive_group()
    cmd_grp.add_argument(
        "-i",
        type=str,
        required=False,
        help="Input .out file from perf-crimson-msgr",
        default=None,
    )
    cmd_grp.add_argument(
        # parser.add_argument(
        "-p",
        "--plot",
        type=str,
        required=False,
        default=None,
        help="Just plot the heatmap of the given .json file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=False,
        help="Output .json file",
        default=None,
    )
    parser.add_argument(
        "-d", "--directory", type=str, help="Directory to examine", default="./"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="True to enable verbose logging mode",
        default=False,
    )

    options = parser.parse_args(argv)

    if options.verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO

    with tempfile.NamedTemporaryFile(dir="/tmp", delete=False) as tmpfile:
        logging.basicConfig(filename=tmpfile.name, encoding="utf-8", level=logLevel)

    logger.debug(f"Got options: {options}")

    msgSt = MsgrStatEntry(options.i, options.output, options.directory, options.plot)
    msgSt.run()


if __name__ == "__main__":
    main(sys.argv[1:])
