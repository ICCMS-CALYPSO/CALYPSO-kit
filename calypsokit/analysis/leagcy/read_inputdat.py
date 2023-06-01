#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Read input.dat
#
# To add new keys:
#   1. add {key: (func, default_value)} to `key_func_map`
#   2. if adding multi-line key-value which starts with '@',
#      add how to parse in `SplitBlock` class
#   3. run unittest in Test/test-io-utils/test_read_input.py
#
# Pass test on:
#   python >= 2.7, >= 3.9
#   ase >= 3.15
#   numpy >= 1.10
#
'''
@Author   : Xiaoshan Luo <luoxs@calypso.cn>
@Date     : 2021/12/19
'''

import numpy as np
from ase.data import atomic_names, atomic_numbers


# ----------------------------------------------------------------
# Split key and value(if singleline)
def singleline(line):
    '''Parse single line parameter, ignore comment and split by '='.
    e.g.    PopSize = 30  # comment
    '''
    key, val = line.partition('#')[0].split('=')
    return key.strip().lower(), val.strip()


def multiline(line):
    '''Parse multiline parameter, ignore comment and get key.
    e.g.    @DistanceOfIon  # comment
    '''
    key = line.partition('#')[0][1:].strip().lower()
    return key


# ----------------------------------------------------------------
# Parse value
class SplitLine:
    '''Split value which has multi-element into list.
    e.g.    NameOfAtoms = B N  # comment
    listtype <- str
    '''

    def __init__(self, listtype):
        self.listtype = listtype

    def __call__(self, string):
        return list(map(lambda x: self.listtype(x.strip()), string.split()))


class LogicLine:
    '''Parse logical value'''

    def __call__(self, string):
        if string.strip().lower().startswith('t'):
            return True
        elif string.strip().lower().startswith('f'):
            return False
        else:
            raise ValueError(
                'Unknown value: %s, please use `True` or `False`' % (string)
            )


def SplitBlock(key, block, parms):
    '''Split block which is matrix-like'''

    if key == 'distanceofion':
        # {(atom_number, atom_number): distance, ...}
        # e.g.  {(1, 1): 0.8, (1, 8): 1.5, (8, 1): 1.5, (8, 8): 2.0}
        #       H  H         H  O         O  H         O  O
        try:
            block = np.asarray([line.split() for line in block]).astype(float)
            return block
            dist_of_ion = {}
            for i, elemi in enumerate(parms['nameofatoms']):
                for j, elemj in enumerate(parms['nameofatoms']):
                    dist_of_ion[(atomic_numbers[elemi], atomic_numbers[elemj])] = block[
                        i, j
                    ]
            return dist_of_ion
        except ValueError as e:
            raise ValueError('DistanceOfIon Error: %s' % (e))

    elif key == 'layertype':
        # [{atom_name: atom_count, ...}, <next layer>]
        # e.g.  [{'B': 1, 'N': 1}, {'C': 2}]
        try:
            block = np.asarray([line.split() for line in block]).astype(int)
            layertype = [
                {u: v for u, v in zip(parms['nameofatoms'], layer)}
                for n_layer, layer in zip(range(parms['multilayer']), block)
            ]
            return layertype
        except ValueError as e:
            raise ValueError('LayerType Error: %s' % (e))

    elif key == 'ctrlrange':
        # {atom_name: (min, max), ...}
        # e.g.  {'Li': (1, 4), 'Mg': (1, 4)}
        try:
            block = np.asarray([line.split() for line in block]).astype(int)
            ctrlrange = {u: (v[0], v[1]) for u, v in zip(parms['nameofatoms'], block)}
            return ctrlrange
        except ValueError as e:
            raise ValueError('CtrlRange Error: %s' % (e))

    elif key == 'surface_atoms':
        # {atom_name: atom_count, ...}
        # e.g.  {'Ti': 2, 'O': 4}
        try:
            surface_atoms = {
                line.split()[0].strip(): int(line.split()[1]) for line in block
            }
            return surface_atoms
        except ValueError as e:
            raise ValueError('SurfaceAtoms Error: %s' % (e))

    elif key == 'matrixnotation':
        # 2x2 ndarray
        try:
            block = np.asarray([line.split() for line in block]).astype(int)
            return block
        except ValueError as e:
            raise ValueError('Matrixnotation Error: %s' % (e))

    elif key == 'interface_atoms':
        # {atom_name: atom_count, ...}
        # e.g.  {'Ti': 2, 'O': 4}
        try:
            interface_atoms = {
                line.split()[0].strip(): int(line.split()[1]) for line in block
            }
            return interface_atoms
        except ValueError as e:
            raise ValueError('Interface_Atoms Error: %s' % (e))

    elif key == 'coordinate_number':
        # {
        # (center_atom, neighbor_atom):
        #   (ini_coord_num, max_coord_num, min_radius, max_radius)
        # e.g.  {(1, 1): (0, 2, 2.5, 2.5), (1, 8): (2, 0, 1.5, 1.5)}
        #       H  H                      H  O
        # }
        try:
            block = [
                [
                    line.split()[0].strip(),
                    line.split()[1].strip(),
                    int(line.split()[2]),
                    int(line.split()[3]),
                    float(line.split()[4]),
                    float(line.split()[5]),
                ]
                for line in block
            ]
            coordinate_number = {
                (atomic_numbers[center_atom], atomic_numbers[neighbor_atom]): (
                    ini_coord_num,
                    max_coord_num,
                    min_radius,
                    max_radius,
                )
                for (
                    center_atom,
                    neighbor_atom,
                    ini_coord_num,
                    max_coord_num,
                    min_radius,
                    max_radius,
                ) in block
            }
            return coordinate_number
        except ValueError as e:
            raise ValueError('Coordinate_Number Error: %s' % (e))

    else:
        return list(block)


# type transform and default value
key_func_map = {
    # basic keys
    'systemname': (str, 'CALYPSO'),
    'numberofspecies': (int, []),
    'nameofatoms': (SplitLine(str), []),
    'numberofatoms': (SplitLine(int), []),
    'numberofformula': (SplitLine(int), []),
    'volume': (SplitLine(float), []),
    'distanceofion': (SplitBlock, 0.7),  # !!!!
    'ialgo': (int, 2),
    'icode': (int, 1),
    'numberoflocaloptim': (int, 4),
    'psoratio': (float, 0.6),
    'popsize': (int, 30),
    'kgrid': (SplitLine(float), (0.12, 0.06)),
    'command': (str, 'submit.sh'),
    'maxstep': (int, 50),
    'pickup': (LogicLine(), False),
    'pickupstep': (int, None),
    'maxtime': (float, 7200.0),
    'lmc': (LogicLine(), False),
    # 2d layers
    '2d': (LogicLine(), False),
    'lfilm': (LogicLine(), False),
    'thickness': (float, None),
    'area': (float, 0.0),
    'multilayer': (int, 1),
    'deltaz': (float, 0.2),
    'layergap': (float, 5.0),
    'vacuumgap': (float, 10.0),
    'layertype': (SplitBlock, None),  # !!!!
    'latom_dis': (float, 1.0),
    # cluster
    'cluster': (LogicLine(), False),
    'vacancy': (SplitLine(float), (10.0, 10.0, 10.0)),
    # rigid molecular
    'mol': (LogicLine(), False),
    'numberoftypemolecule': (int, None),
    'numberofmolecule': (SplitLine(int), None),
    'distofmol': (float, 1.5),
    # variational stoichiometry
    'vsc': (LogicLine(), False),
    'maxnumatom': (int, 20),
    'ctrlrange': (SplitBlock, None),  # !!!!
    # surface reconstruction
    'lsurface': (LogicLine(), False),
    'surfacethickness': (float, 3.0),
    'substrate': (str, 'SUBSTRATE.surf'),
    'surface_atoms': (SplitBlock, None),  # !!!!
    'spacesaving': (LogicLine(), True),
    'ecr': (LogicLine(), False),
    'ciffilepath': (str, None),
    'millerindex': (SplitLine(int), None),
    'matrixnotation': (SplitBlock, None),  # !!!!
    'slabvaccumthick': (float, 10),
    'slabtopmost': (SplitLine(str), ['CALYPSO']),
    'slabnumlayers': (int, 6),
    'numrelaxedlayers': (int, 2),
    'capbondswithh': (LogicLine(), True),
    # solid-solid interface
    'linterface': (LogicLine(), False),
    'interfacetranslation': (str, False),
    'translationlimita': (float, 0.0),
    'translationlimitb': (float, 0.0),
    'rand_scheme': (int, 1),
    'interface_thickness': (float, None),
    'forbi_thickness': (float, 0.2),
    'interface_atoms': (SplitBlock, None),  # !!!!
    'coordinate_number': (SplitBlock, None),  # !!!!
    # substrate
    'substrate2': (str, 'SUBSTRATE2.surf'),
    'twin_interface': (int, 0),
    # inverse design of superhard materials
    # ----------------------------------------------------------------
    # atom or molecule adsorption of 2d layer
    # ----------------------------------------------------------------
    # optical materials with desirable band gap
    # ----------------------------------------------------------------
    # Special Constraints
    # ----------------------------------------------------------------
    # transition states in solids
    # ----------------------------------------------------------------
}


def readinput(input='input.dat'):
    parms = {u: v[1] for u, v in key_func_map.items()}
    parms_number = {}

    with open(input, 'r') as f:
        ini_file = f.readlines()
    lines = list(map(lambda line: line.strip(), ini_file))
    for num, line in enumerate(lines):
        # empty line
        if len(line) == 0:
            continue
        # comment line
        elif line[0] == '#':
            continue
        # single line parameter
        elif '=' in line:
            key, val = singleline(line)
            try:
                func = key_func_map[key][0]
            except KeyError:  # Not a single line parameter or unknown key
                continue
            try:  # Indeed a single line parameter
                val = func(val)
            except ValueError as e:
                raise ValueError('Line %i of %s: %s' % (num, input, e))
            parms[key] = val
            parms_number[key] = num

        # multiline parameter
        elif line[0] == '@':
            key = multiline(line)
            if key in key_func_map.keys():
                idx = num + 1
                while multiline(lines[idx]) != 'end':
                    idx += 1
                block = lines[num + 1 : idx]
                parms[key] = SplitBlock(key, block, parms)
        else:
            continue

    return parms
