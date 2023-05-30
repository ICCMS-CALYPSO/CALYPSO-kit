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
