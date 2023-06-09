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
from io import StringIO

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
                {u: int(v) for u, v in zip(parms['nameofatoms'], layer)}
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
            ctrlrange = {
                u: [int(v[0]), int(v[1])] for u, v in zip(parms['nameofatoms'], block)
            }
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
                (atomic_numbers[center_atom], atomic_numbers[neighbor_atom]): [
                    int(ini_coord_num),
                    int(max_coord_num),
                    float(min_radius),
                    float(max_radius),
                ]
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


def old_distanceofion_patch(val):
    # 11
    # 11 22 12
    # 11 22 33 12 13 23
    val = val.strip().split()
    if len(val) == 1:
        val = np.array([[float(val)]])
    elif len(val) == 3:
        val = np.array(
            [
                [float(val[0]), float(val[2])],
                [float(val[2]), float(val[1])],
            ]
        )
    elif len(val) == 6:
        val = np.array(
            [
                [float(val[0]), float(val[3]), float(val[4])],
                [float(val[3]), float(val[1]), float(val[5])],
                [float(val[4]), float(val[5]), float(val[2])],
            ]
        )
    else:
        raise ValueError("DistanceOfIon Error: more than three element in old version")
    return val


# type transform and default value
key_func_map = {
    # basic keys
    'command': (str, 'submit.sh'),
    'distanceofion': (SplitBlock, 0.7),  # !!!!
    'ialgo': (int, 2),
    'icode': (int, 1),
    'kgrid': (SplitLine(float), (0.12, 0.06)),
    'lmc': (LogicLine(), False),
    'maxstep': (int, 50),
    'maxtime': (float, 7200.0),
    'nameofatoms': (SplitLine(str), []),
    'numberofatoms': (SplitLine(int), []),
    'numberofformula': (SplitLine(int), []),
    'numberoflocaloptim': (int, 4),
    'numberofspecies': (int, []),
    'pickup': (LogicLine(), False),
    'pickupstep': (int, None),
    'popsize': (int, 30),
    'psoratio': (float, 0.6),
    'systemname': (str, 'CALYPSO'),
    'volume': (SplitLine(float), []),
    # 2d layers
    '2d': (LogicLine(), False),
    'area': (float, 0.0),
    'deltaz': (float, 0.2),
    'latom_dis': (float, 1.0),
    'layergap': (float, 5.0),
    'layertype': (SplitBlock, None),  # !!!!
    'lfilm': (LogicLine(), False),
    'multilayer': (int, 1),
    'thickness': (float, None),
    'vacuumgap': (float, 10.0),
    # cluster
    'cluster': (LogicLine(), False),
    'vacancy': (SplitLine(float), (10.0, 10.0, 10.0)),
    # rigid molecular
    'distofmol': (float, 1.5),
    'mol': (LogicLine(), False),
    'numberofmolecule': (SplitLine(int), None),
    'numberoftypemolecule': (int, None),
    # variational stoichiometry
    'ctrlrange': (SplitBlock, None),  # !!!!
    'maxnumatom': (int, 20),
    'vsc': (LogicLine(), False),
    'vscenergy': (SplitLine(float), []),
    # surface reconstruction
    'capbondswithh': (LogicLine(), True),
    'ciffilepath': (str, None),
    'ecr': (LogicLine(), False),
    'lsurface': (LogicLine(), False),
    'matrixnotation': (SplitBlock, None),  # !!!!
    'millerindex': (SplitLine(int), None),
    'numrelaxedlayers': (int, 2),
    'slabnumlayers': (int, 6),
    'slabtopmost': (SplitLine(str), ['CALYPSO']),
    'slabvaccumthick': (float, 10),
    'spacesaving': (LogicLine(), True),
    'substrate': (str, 'SUBSTRATE.surf'),
    'surface_atoms': (SplitBlock, None),  # !!!!
    'surfacethickness': (float, 3.0),
    # solid-solid interface
    'coordinate_number': (SplitBlock, None),  # !!!!
    'forbi_thickness': (float, 0.2),
    'interface_atoms': (SplitBlock, None),  # !!!!
    'interface_thickness': (float, None),
    'interfacetranslation': (str, False),
    'linterface': (LogicLine(), False),
    'rand_scheme': (int, 1),
    'translationlimita': (float, 0.0),
    'translationlimitb': (float, 0.0),
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


def readinput(input='input.dat', input_str=None):
    parms = {u: v[1] for u, v in key_func_map.items()}
    parms_number = {}

    if input_str is None:
        with open(input, 'r') as f:
            ini_file = f.readlines()
    else:
        ini_file = StringIO(input_str).readlines()
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
            if key == "distanceofion":
                val = old_distanceofion_patch(val)
            else:
                try:
                    func = key_func_map[key][0]
                except KeyError:  # Not a single line parameter or unknown key
                    continue
                try:  # Indeed a single line parameter
                    val = func(val)
                except ValueError as e:
                    raise ValueError('Line %i of %s: %s' % (num, input, e))
                except Exception:
                    # Other error line
                    continue
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
