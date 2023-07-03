import os
import numpy as np
from itertools import product

titels = os.popen("grep 'TITEL  = PAW_PBE' POTCAR").read().split("\n")

element_name = []
for titel in titels:
    if 'TITEL' in titel:
        element_name.append(titel.strip().split()[-2])

rcores = os.popen("grep 'RCORE' POTCAR").read().split("\n")

element_rcore = []
for rcore in rcores:
    if 'RCORE' in rcore:
        element_rcore.append(float(rcore.strip().split()[2]))


def calc_dis(a, b):
    return (a + b) * 0.529177

def calc_indice(element_a, element_b):
    return (element_name.index(element_a), element_name.index(element_b),)

distance_mtx = np.ones((len(element_name), len(element_name)))

referance_dict = dict(zip(element_name, element_rcore))

possible_combinations = product(element_name, repeat=2 )

for combination in possible_combinations:
    a, b = combination
    distance = calc_dis(referance_dict[a], referance_dict[combination[1]])
    indice = calc_indice(a, b)
    distance_mtx[indice] = distance

print(element_name)

print(distance_mtx)

print(0.9*distance_mtx)

print(0.8*distance_mtx)

print(0.7*distance_mtx)

