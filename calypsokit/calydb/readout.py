import io
from contextlib import redirect_stdout

import pandas as pd
from ase.io import write

from calypsokit.calydb.queries import Pipes, QueryStructure


class ReadOut:
    def unique2cdvae(self, rawcol, uniqcol, *, debug=-1):
        pipeline = Pipes.unique_records(fromcol="rawcol") + [{"$project": {"_id": 1}}]
        if debug > 0:
            pipeline.append({"$limit": debug})
        records = uniqcol.aggregate(pipeline)
        ids = [record["_id"] for record in records]
        projection = {"pressure": 1, "material_id": 1, "formula": 1}
        qs = QueryStructure(rawcol, projection=projection, type="ase")

        ser_list = []
        for item in qs.find({"_id": {"$in": ids}}):
            atoms = item[0]
            properties = item[1]
            with io.BytesIO() as buffer, redirect_stdout(buffer):
                write('-', atoms, format='cif')
                cif = buffer.getvalue().decode()
            data = {key: properties[key] for key in projection.keys()}
            data["cif"] = cif
            series = pd.Series(data)
            ser_list.append(series)

        df = pd.DataFrame(ser_list)
        return df
