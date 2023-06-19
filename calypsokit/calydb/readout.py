import io
import logging
from contextlib import redirect_stdout

import pandas as pd
from ase.io import write

from calypsokit.calydb.queries import Pipes

logger = logging.getLogger(__name__)


class ReadOut:
    def unique2cdvae(self, db, rawcol: str, uniqcol: str, *, debug=-1):
        """output filtered records as cdvae dataset

        Examples
        --------
        >>> from calypsokit.calydb.login import login
        >>> from calyposkit.calydb.readout import ReadOut
        >>> db = login()
        >>> df = ReadOut().unique2cdvae(db, debug=10)
        >>> print(df)
        material_id   formula  pressure natoms  ...
        <id>

        Parameters
        ----------
        db : _type_
            _description_
        rawcol: str, optional
            collection name of raw data
        uniqcol : str, optional
            collection name of unique data
        debug : int, optional
            limit number of output records, by default -1

        Returns
        -------
        pd.DataFrame
        """
        pipeline = [
            {
                "$match": {
                    "min_distance": {"$gt": 0.5, "$lt": 5.2},
                    "volume_rate": {"$lt": 4},
                    'deprecated': False,
                }
            },
        ] + Pipes.unique_records(uniqcol)
        if debug > 0:
            pipeline.append({"$limit": debug})
        projection = {
            "_id": 0,
            "material_id": 1,
            "formula": 1,
            "pressure": 1,
            "natoms": 1,
            "volume_rate": 1,
            "enthalpy_per_atom": 1,
            "volume_per_atom": 1,
            "symmetry.1e-1.number": 1,
            "cif": 1,
        }
        pipeline.append({"$project": projection})

        logger.info("Start finding")
        records = list(db.get_collection(rawcol).aggregate(pipeline))
        logger.info("Parsing to DataFrame")
        ser_list = []
        for record in records:
            sym = record.pop("symmetry")
            record['spgno'] = sym["1e-1"]["number"]
            series = pd.Series(record)
            ser_list.append(series)
        df = pd.DataFrame(ser_list)
        logger.info("Successfully to DataFrame")
        return df
