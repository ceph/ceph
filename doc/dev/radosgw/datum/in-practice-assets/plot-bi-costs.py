from pathlib import Path
import json
import os

root = Path(__file__).resolve().parent
os.environ['MPLCONFIGDIR'] = str(root / '.matplotlib-cache')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

data = json.loads((root / 'bi-costs.json').read_text())
cpu = data['results'][0]['thread_cpu_ns']
cpu_ratio = 100 + cpu['change_percent']
rows = [
    ('CPU per reply', '41.91 µs', '13.08 µs', cpu_ratio, '69% less CPU'),
    ('Encoded bytes', '29,901', '23,901', 100 * 23901 / 29901, '20% fewer bytes'),
    ('Allocation requests', '10', '6', 60, '40% fewer requests')
]
plt.rcParams.update({'font.family': 'Liberation Sans', 'font.size': 12,
                     'svg.fonttype': 'none', 'pdf.fonttype': 42})
fig, axes = plt.subplots(3, 1, figsize=(11.5, 3.6))
fig.subplots_adjust(left=.025, right=.99, top=.84, bottom=.04, hspace=.32)
ink, green, gray = '#193441', '#217f67', '#c8c7bb'
for index, (ax, (label, old, new, ratio, gain)) in enumerate(zip(axes, rows)):
    ax.barh([1, 0], [100, ratio], height=.48, color=[gray, green], zorder=2)
    if 0 == index:
        low, high = 100 + cpu['ci_low_percent'], 100 + cpu['ci_high_percent']
        ax.errorbar(ratio, 0, xerr=[[ratio-low], [high-ratio]], fmt='none',
                    capsize=3, ecolor=ink, linewidth=1, zorder=3)
    ax.text(-54, .5, label, ha='left', va='center', weight='bold', color=ink, fontsize=13)
    ax.text(103, 1, old, ha='left', va='center', color=ink)
    ax.text(ratio + 3, 0, new, ha='left', va='center', color=ink)
    ax.text(141, .5, gain, ha='left', va='center', weight='bold', color=green, fontsize=13)
    ax.set(xlim=(-55, 187), ylim=(-.5, 1.5))
    ax.axis('off')
fig.legend(handles=[Patch(color=gray, label='Native framed reply'),
                    Patch(color=green, label='Compact BI reply')],
           loc='upper center', ncol=2, frameon=False)
fig.savefig(root / 'bi-costs.svg', facecolor='white')
