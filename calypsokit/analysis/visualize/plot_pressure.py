import json
import pandas as pd
import matplotlib.pyplot as plt  
import plotly.express as px
import numpy as np


pressure_data = {"80.0": 1566, "200.0": 147054, "300.0": 67059, "100.0": 179599, "50.0": 106451, "150.0": 10573, "135.0": 2136, "0.001": 133757, "20.0": 5017, "400.0": 1206, "40.0": 4554, "10.0": 12390, "15.0": 870, "30.0": 4354, "5.0": 12700, "500.0": 10496, "130.0": 1065, "25.0": 43837, "700.0": 2681, "800.0": 2285, "360.0": 4110, "250.0": 638, "1000.0": 4796, "1.0": 30, "2000.0": 2959, "60.0": 5629, "1500.0": 309,  "600.0": 385, "350.0": 235, "1000.0": 195 } 


pressure = []
value = []


tt = sorted(pressure_data.items(), key=lambda x: float(x[0]))
for t in tt:
    pressure.append(float(t[0]))
    value.append(int(t[1]))

df = pd.DataFrame({'pressure':pressure, 'value':value})
print(df)

fig = px.sunburst(df, path=['pressure', ], values='value')

fig.write_image('pressure.png', scale=2)

