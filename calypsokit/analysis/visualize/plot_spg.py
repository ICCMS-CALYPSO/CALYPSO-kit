import json
import pandas as pd
import matplotlib.pyplot as plt  
import plotly.express as px
import numpy as np

with open('../spg_cnt.json', 'r') as f:
    spg = json.load(f)

triclinic = ['triclinic'] * 2 
monoclinic = ['monoclinic'] * 13
orthorhombic = ['orthorhombic'] *  (75 - 16)
teragonal = ['teragonal'] * (143 - 75)
trigonal = ['trigonal'] * (168 - 143)
hexagonal = ['hexagonal'] * (195 - 168)
cubic = ['cubic'] * (231 - 195 - 1)

crystal_system = []
spg_num = []
value = []

for cs in [triclinic, monoclinic, orthorhombic, teragonal, trigonal, hexagonal, cubic]:
        crystal_system.extend(cs)

tt = sorted(spg.items(), key=lambda x: int(x[0]))
for t in tt:
    spg_num.append(int(t[0]))
    value.append(int(t[1]))

print(len(spg_num), len(crystal_system), len(value))
df = pd.DataFrame({'spg_num':spg_num[1:], 'crystal_system':crystal_system, 'value':value[1:]})
print(df)

fig = px.sunburst(df, path=['crystal_system', 'spg_num'], values='value')

fig.write_image('spg.png', scale=2)

