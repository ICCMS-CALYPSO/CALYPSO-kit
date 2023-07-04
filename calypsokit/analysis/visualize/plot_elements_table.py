import matplotlib as mpl  
import matplotlib.cm as cm  
import matplotlib.patches as patches
import matplotlib.pyplot as plt  
import mendeleev
import json


# plot_data = {'O': 9, 'Te': 9, 'F': 9, 'S': 9, 'Na': 9, 'K': 9, 'N': 8, 'Li': 8,
#              'I': 8, 'Rb': 8, 'Si': 7, 'Cd': 7, 'Cl': 7, 'Zn': 7, 'H': 7,
#              'Bi': 7, 'Br': 7, 'P': 6, 'Sn': 6, 'Ca': 6, 'Au': 6, 'Al': 5,
#              'As': 5, 'Ga': 5, 'C': 5, 'Ge': 5, 'Sr': 5, 'Se': 5, 'Be': 5,
#              'B': 5, 'Cs': 5, 'Mg': 5, 'Ag': 5, 'Pb': 4, 'In': 4, 'Ti': 4,
#              'Cu': 4, 'Zr': 4, 'Sb': 4, 'Tl': 4, 'Sc': 4, 'Y': 4, 'Hg': 4,
#              'Ba': 4, 'La': 4, 'Hf': 4, 'Og': 1}

with open('../element_cnt.json', 'r') as f:
    plot_data = json.load(f)

cell_length = 1
cell_gap = 0.1
cell_edge_width = 0.5

elements = []
for i in range(1, 119):
    ele = mendeleev.element(i)
    ele_group, ele_period = ele.group_id, ele.period

    if 57 <= i <= 71:
        ele_group = i - 57 + 3
        ele_period = 8

    if 89 <= i <= 103:
        ele_group = i - 89 + 3
        ele_period = 9

    elements.append([i, ele.symbol, ele_group, ele_period,
                     plot_data.setdefault(ele.symbol, 0)])

elements.append([None, 'LA', 3, 6, None])
elements.append([None, 'AC', 3, 7, None])
elements.append([None, 'LA', 2, 8, None])
elements.append([None, 'AC', 2, 9, None])

fig = plt.figure(figsize=(10, 5), dpi=350)

xy_length = (20, 11)

# my_cmap = cm.get_cmap('YlOrRd')
my_cmap = cm.get_cmap('plasma_r')

norm = mpl.colors.Normalize(1, 260000)

my_cmap.set_under('None')

cmmapable = cm.ScalarMappable(norm, my_cmap)

plt.colorbar(cmmapable, drawedges=False)

for e in elements:
    ele_number, ele_symbol, ele_group, ele_period, ele_count = e
    print(ele_number, ele_symbol, ele_group, ele_period, ele_count)

    if ele_group is None:
        continue

    x = (cell_length + cell_gap) * (ele_group - 1)
    y = xy_length[1] - ((cell_length + cell_gap) * ele_period)

    if ele_period >= 8:
        y -= cell_length * 0.5

    if ele_number:
        fill_color = my_cmap(norm(ele_count))
        rect = patches.Rectangle(xy=(x, y),
                                 width=cell_length, height=cell_length,
                                 linewidth=cell_edge_width,
                                 edgecolor='k',
                                 facecolor=fill_color)
        plt.gca().add_patch(rect)

    plt.text(x + 0.04, y + 0.8,
             ele_number,
             va='center', ha='left',
             fontdict={'size': 6, 'color': 'black', 'family': 'Helvetica'})

    plt.text(x + 0.5, y + 0.5,
             ele_symbol,
             va='center', ha='center',
             fontdict={'size': 9, 'color': 'black', 'family': 'Helvetica', 'weight': 'bold'})

    plt.text(x + 0.5, y + 0.12,
             ele_count,
             va='center', ha='center',
             fontdict={'size': 6, 'color': 'black', 'family': 'Helvetica'})


plt.axis('equal')

plt.axis('off')

plt.tight_layout()

plt.ylim(0, xy_length[1])
plt.xlim(0, xy_length[0])

plt.savefig('./periodic_table.png')
plt.show()

