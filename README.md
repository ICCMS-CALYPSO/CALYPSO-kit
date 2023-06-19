# CALYPSO kit

This repository maintains toolkit for CALYPSO structure prediction software.

## How does this repo organized?

    /calypsokit
      |- analysis             # structure analysis
         |- validity.py       # structure validity
      |- calydb               # calypso structure database
         |- calydb.py
         |- query.py          # customized query methods

## CALYPSO Database

### Connect to the database

To connect it with pymongo, the database address, username, password are read from
the environment variables. You can either put them in you environment variables or
write a `.env` file:

```properties
MONGODB_ADDR=<ip>:<port>
MONGODB_USER=<username>
MONGODB_PWD=<password>
MONGODB_DATABASE=<database name>
```

### How does the DataBase organized?

The database is stored by MongoDB.

    /calydb            # database
      |- raw           # raw data collection
      |- uniq          # unique _id collection
      |- *             # other selected collection
      |- debugcol      # debug collection

### What are recorded in `rawcol`?


    "_id":                      ObjectId(***),    # bson ObjectId
    "material_id":              "caly-*****",     # str
    "source": {
        "name":         "calypso",      # calypso or materials project, etc.
        "index":        *****,          # int index in this source, or None
    },
    "elements":                 ['La', 'H'],     # list of elements str
    "nelements":                2,               # number of elements
    "elemcount":                [2, 20],         # list of int, count of each element
    "species":                  ['La', ...],     # list of str, species of each atom
    "formula":                  "La2H20",        # formula str, metal and alphabet order
    "reduced_formula":          "LaH10",         # formula str, metal and alphabet order
    "natoms":                   22,              # int
    "cell":                     3x3 ndarray,     # cell matrix in angstrom
    "cell_abc":                 [a, b, c],       # cell length, angstrom
    "cell_angles":              [α, β, γ],       # cell angles, angle
    "positions":                22x3 ndarray,    # atom cartesian coordinates in angstrom
    "scaled_positions":         22x3 ndarray,    # atom fractional coordinates in angstrom
    "forces":                   22x3 ndarray,    # force of each atom in eV/A
    "enthalpy":                 *.*,             # eV
    "enthalpy_per_atom":        *.*,             # eV/atom
    "volume":                   *.*,             # A^3
    "volume_per_atom":          *.*,             # A^3/atom
    "density":                  *.*,             # g/cm^3
    "clospack_volume":          *.*,             # float, A^3, close-pack volume of covalent radii
    "clospack_volume_per_atom": *.*,             # float, A^3
    "clospack_density":         *.*,             # float, g/cm^3
    "volume_rate":              *,      # float, volume/clospack_volume
    "pressure":                 *.*,    # float, GPa
    "pressure_range": {                         # each pressure is set to a bin
        "mid":                     "*.*",       # starts=-0.1, width=0.2
        "length":                  "0.2",       # e.g. 10 -> (-9.9, 10.1]
        "closed":                  "right"      # default left-open right-closed
    },                                          # for group structures
    "min_distance":             *.*,    # min distance between atoms
    "dim_larsen":               *,      # dimension in larsen algorithm, -1 if no neighboor if found
    "symmetry": {  # symprec
        # (str %.0e) int[1, 230]   "F m -3 m"         '
        "1e-1": {"number": 0, "symbol": "", "crystal_system": ""},
        "1e-2": {"number": 0, "symbol": "", "crystal_system": ""},
        "1e-5": {"number": 0, "symbol": "", "crystal_system": ""},
    },
    "cif":                      "*",     # cif string
    "poscar"                    "*",     # VASP POSCAR string

    "trajectory": {
        "nframes":                  2,          # int
        "cell":                     2x3x3,      # np.ndarray, angstrom
        "positions":                2x3x3,      # np.ndarray, angstrom
        "scaled_positions":         2x3x3,      # np.ndarray
        "forces":                   2x3x3,      # np.ndarray
        "volume":                   [*.*, *.*], # volume of each frame
        "enthalpy":                 [*.*, *.*], # enthalpy of each frame, np.nan if None, eV
        "enthalpy_per_atom":        [*.*, *.*], # enthalpy per atom of each frame, np.nan if None, eV
        "source_file":              [*.*, *.*], # source path of each frame
        "source_idx":               [*.*, *.*], # index in their source of each frame
        "source_dir":               "*",       # str, source dir
        "kabsch": {
            "kabsch_rot":           3x3 ndarray,     # rotation mat from 'r' to 'i' in kabsch algorithm
            "max_d_cell_i_r":       float,           # max abs delta between cell_r and cell_i
            "avg_d_cell_i_r":       float,           # mean abs delta between cell_r and cell_i
            "max_d_cell_roti_r":    float,           # max abs delta between rotated cell_i and cell_r
            "avg_d_cell_roti_r":    float,           # mean abs delta between rotated cell_i and cell_r
        }
        "shifted_d_frac":           22x3 ndarray,    # delta fractional coordinates 'r' minus 'i' shifted between [-0.5, 0.5]
        "strain": {
            "rot":                  3x3 ndarray,     # U of polar decomposition <r>=UP<i> 
            "strain":               3x3 ndarray,     # strain matrix, strain=P-I
            "avg_strain":           *.*,             # float, average strain
        }
    },

    "calyconfig": {
        "version": "",
        "icode": 1,
        ...
    },
    "dftconfig":                [<INCAR_3>],   # list of str，不要替换换行符
    "pseudopotential":          [<La>, <H>],   # list of str，与elements对应
    "donator": {"name": "", "email": ""},
    "deprecated":               False,  # bool
    "deprecated_reason":        "",     # str
    "last_updated_utc": datetime.utcnow(),

| Possible deprecated reason | comment|
| :-                         | :-     |
| `error enthalpy : optimization fail` | `enthalpy_per_atom` > 610612508(9) |
| `number of extracted structure <= <lte> in this prediction task` | Too less structures (`lte`) in one prediction |
| `error enthalpy : solitary and too small` | Solitary energy structure in each prediction (low energy side) |
| `error structure : isolated atoms`| atoms too discrate in crystal prediction |
| `error structure : unreasonable` |  |
