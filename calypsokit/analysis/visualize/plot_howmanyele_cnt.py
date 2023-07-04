import json
import pandas as pd
import matplotlib.pyplot as plt  
import plotly.express as px
import numpy as np


pressure_dict = {"ternary": 294751, "binary": 390113, "element": 9478, "quaternary": 3509}

pressure = []
value = []


for t in pressure_dict.items():
    pressure.append(t[0])
    value.append(t[1])

df = pd.DataFrame({'pressure':pressure, 'value':value})
print(df)

fig = px.sunburst(df, path=['pressure', ], values='value')

fig.write_image('howmanyele_cnt.png', scale=2)

