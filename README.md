# CALYPSO kit

This repository maintains toolkit for CALYPSO structure prediction software.

## How does this repo organized?

    /calypsokit
      |- analysis             # structure analysis
         |- validity.py       # structure validity
      |- calydb               # calypso structure database
         |- calydb.py
         |- query.py          # customized query methods

## How does the DataBase organized?

The database is stored by MongoDB.

    /calydb            # database
      |- rawcol        # raw data collection
      |- *col          # other selected collection
      |- debugcol      # debug collection

## What is recorded in rawcol?


    "material_id":              "",     # str
    "source": {
        "name":         "",             # calypso or materials project, etc.
        "index":        0,              # int index in this source, or None
    },
    "elements":                 [],     # list of str
    "nelements":                <Ne>,   # int
    "elemcount":                [],     # list of int, count of each element
    "species":                  [],     # list of str, species of each atom
    "formula":                  "",     # str, metal and alphabet order
    "reduced_formula":          "",     # str, metal and alphabet order
    "natoms":                   <Na>,   # int
    "cell":                     3x3 ndarray,       # np.ndarray, angstrom
    "cell_abc":                 [a, b, c],         # cell length, angstrom
    "cell_angles":              [α, β, γ],         # cell angles, angle
    "positions":                <Na>x3,            # np.ndarray, angstrom
    "scaled_positions":         <Na>x3,            # np.ndarray
    "forces":                   <Na>x3,            # np.ndarray
    "enthalpy":                 0.0,
    "enthalpy_per_atom":        0.0,
    "volume":                   0.0,
    "volume_per_atom":          0.0,
    "density":                  0.0,
    "clospack_volume":          0.0,    # float, A^3, close-pack volume of covalent radii
    "clospack_volume_per_atom": 0.0,    # float, A^3
    "clospack_density":         0.0,    # float, g/cm^3
    "volume_rate":              1.0,    # float, volume/clospack_volume
    "pressure":                 0.0,    # float, GPa
    "pressure_range": {                         # each pressure is set to a bin
        "mid":                     "0.0",       # starts=-0.1, width=0.2
        "length":                  "0.2",       # e.g. 10 -> (-9.9, 10.1]
        "closed":                  "right"      # default left-open right-closed
    },                                          # for group structures
    "min_distance":             0.0,    # min distance between atoms
    "dim_larsen":               3,      # dimension in larsen algorithm, -1 if no neighboor if found

    "trajectory": {
        "nframes":                  <F>,      # int
        "cell":                     <F>x3x3,  # np.ndarray, angstrom
        "positions":                <F>x3x3,  # np.ndarray, angstrom
        "scaled_positions":         <F>x3x3,  # np.ndarray
        "forces":                   <F>x3x3,  # np.ndarray
        "volume":                   [],       # volume of each frame
        "enthalpy":                 [],       # enthalpy of each frame, np.nan if None, eV
        "enthalpy_per_atom":        [],       # enthalpy per atom of each frame, np.nan if None, eV
        "source_file":              [],       # source path of each frame
        "source_idx":               [],       # index in their source of each frame
        "source_dir":               "",       # str, source dir
        "kabsch": {
            "kabsch_rot":           3x3 ndarray,     # rotation mat from 'r' to 'i' in kabsch algorithm
            "max_d_cell_i_r":       float,           # max abs delta between cell_r and cell_i
            "avg_d_cell_i_r":       float,           # mean abs delta between cell_r and cell_i
            "max_d_cell_roti_r":    float,           # max abs delta between rotated cell_i and cell_r
            "avg_d_cell_roti_r":    float,           # mean abs delta between rotated cell_i and cell_r
        }
        "shifted_d_frac":           <Na>x3 ndarray,  # delta fractional coordinates 'r' minus 'i' shifted in [-0.5, 0.5]
        "strain": {
            "rot":                  3x3 ndarray,     # U of polar decomposition <r>=UP<i> 
            "strain":               3x3 ndarray,     # strain matrix, strain=P-I
            "avg_strain":           0,               # float, average strain
        }
    },

    "calyconfig": {
        "version": "",
        "icode": 0,
    },
    "dftconfig":                [],   # list of str，不要替换换行符
    "pseudopotential":          [],   # list of str，与elements对应
    "symmetry": {
        # symprec(str %.0e)
        #        int[1, 230] str ("F m -3 m")
        "1e-1": {"number": 0, "symbol": ""},
        "1e-2": {"number": 0, "symbol": ""},
        "1e-5": {"number": 0, "symbol": ""},
    },
    "donator": {"name": "", "email": ""},
    "deprecated":               False,  # bool
    "deprecated_reason":        "",     # str
    "last_updated_utc": datetime.utcnow(),
