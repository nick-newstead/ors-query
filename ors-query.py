import ors_query as query
import numpy as np
import pandas as pd
import itertools
import multiprocessing as mp
import argparse
import functools
import sys
from datetime import datetime
from pathlib import Path

parser = argparse.ArgumentParser(description="Query the ORS API")
parser.add_argument("-if", "--in", help="Name of input file", required=False)
parser.add_argument("-t", "--table", help="Name of table in input file", required=False)
parser.add_argument("-of", "--out", help="Name of output file", required=False)
parser.add_argument("-fp", "--path", help="Set path", required=False)
parser.add_argument("-i", "--iteration", help="Iteration to start on", required=False)
parser.add_argument("-o", "--optimised", help="Optimise path-routing", required=False)
parser.add_argument("-m", "--metric", help="Distance or duration", required=False)
parser.add_argument("-wm", "--working-memory", help="Max memory for chunks", required=False)
parser.add_argument("-u", "--units", help="Units returned in distance matrix", required=False)
parser.add_argument("-p", "--profile", help="Mode of transportation", required=False)
parser.add_argument("-d", "--distance", help="buffer size", required=False)
args = vars(parser.parse_args())

if args["path"] is None:
  data = Path().resolve().joinpath("data")
else:
  data = Path(args["path"]).absolute()

if args["working_memory"] is None:
  wm = 0.1
else:
  wm = float(args["working_memory"])

if args["units"] is None:
  units = "km"
else:
  units = str(args["units"])

if args["profile"] is None:
  profile = "driving-car"  # foot-walking
else:
  profile = str(args["profile"])

if args["optimised"] is None:
  optimised = True
else:
  optimised = bool(args["optimised"])

if args["metric"] is None:
  metric = "distance"  # distance duration
else:
  metric = str(args["metric"])

if args["distance"] is None:
  distance = 30
else:
  distance = int(args["distance"])

if args["table"] is None:
  table = "geodesic"
else:
  table = str(args["table"])

params = {"geo_class": "db",
          "loc_name": "canada",
          "distance": distance,
          "units": units}

if args["iteration"] is None:
  iteration = 0
else:
  iteration = int(args["iteration"])

if args["in"] is None:
  h5_in = data.joinpath("{geo_class}-{loc_name}-{distance}{units}-geodesic.h5".format(**params))
else:
  h5_in = data.joinpath(str(args["in"]))

if args["out"] is None:
  h5_out = data.joinpath("{geo_class}-{loc_name}-{distance}{units}-network.h5".format(**params))
else:
  h5_out = data.joinpath(str(args["out"]))

def get_chunksize(wm, conv=1024**3, size=30):
  return int((wm * conv)/size)


chunksize = get_chunksize(wm)

if __name__ == "__main__":

  store = pd.HDFStore(h5_in)
  nrows = store.get_storer(table).nrows
  count = 0
  all_start = datetime.now()
  for (i, chunk) in enumerate(range(iteration, nrows//chunksize + 1)):
    chunk = store.select(table, start=i * chunksize, stop=(i + 1) * chunksize)
    #chunk = chunk.round({"lon_src": 6, "lat_src": 6, "lon_dest": 6, "lat_dest": 6})
    chunk["coord_src"] = list(zip(chunk["lon_src"], chunk["lat_src"]))
    chunk["coord_dest"] = list(zip(chunk["lon_dest"], chunk["lat_dest"]))
    chunk["locations"] = list(zip(chunk["coord_src"], chunk["coord_dest"]))
    chunk.drop(columns=["lon_src", "lat_src", "lon_dest", "lat_dest", "coord_src", "coord_dest"], inplace=True)
    count += chunk.shape[0]

    # Parallel processing.
    start = datetime.now()
    print(f"Start time: {start}")
    # Get maximum amount of processors.
    nprocs = mp.cpu_count() if mp.cpu_count() - chunk.shape[0] < 0 else chunk.shape[0]
    # Chunk into as many groups as there are processors.
    mp_chunksize = int(chunk.shape[0]/nprocs)
    # Chunk data set.
    #chunk = [chunk.loc[chunk.index[i:i + mp_chunksize]] for i in range(0, chunk.shape[0], mp_chunksize)]
    chunk = [chunk.loc[chunk.index[i:i + mp_chunksize]] for i in range(0, chunk.shape[0], mp_chunksize)]
    # Run request data function on each chunk in parallel.
    func = functools.partial(query.request_data_ors_safe, profile=profile,
                             metric=metric, units=units, optimized=optimised)
    with mp.Pool(processes=nprocs) as pool:
      result = pool.map(func, chunk)

    #result = list(itertools.chain.from_iterable(itertools.chain.from_iterable(result)))
    result = list(itertools.chain.from_iterable(result))
    result = pd.DataFrame(result, columns=["row_src", "row_dest", "src2dest_dist", "dest2src_dist"])
    result.drop_duplicates(subset=["row_src", "row_dest"], inplace=True)
    result.set_index(["row_src", "row_dest"], inplace=True)
    result.sort_index(inplace=True)
    end = datetime.now()
    print(f"End time: {end}\nParallel processing with {nprocs} threads: {end - start}")
    result.to_hdf(h5_out, key="network", mode="a", format="table",
                  append=True, data_columns=True, complevel=9, complib="zlib")
    print(f"Iteration: {i}; Rows completed: {count}; Progress: {count/nrows * 100}%")
  all_end = datetime.now()
  print(f"End time: {all_end}\nTotal run time: {all_end - all_start}")

  store.close()
