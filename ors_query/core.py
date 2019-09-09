# -*- coding: UTF-8 -*-
import numpy as np
import pandas as pd
import itertools
import openrouteservice as ors
import requests
import numba

earth_radius = 6371
# URL to ORS API.
# localhost / IP of machine
client = ors.Client(key="", base_url="http://localhost:8080/ors", retry_over_query_limit=True)

def request_data_ors_unsafe(df):
  out_list = []
  app = out_list.append
  for row in df.itertuples():
    dist = client.distance_matrix(locations=row.locations,
                                  profile="driving-car",
                                  metrics=["distance"],
                                  units="km",
                                  dry_run=False)["distances"]
    out = [[row.Index[0], row.Index[1], dist[0][1]], [row.Index[1], row.Index[0], dist[1][0]]]
    app(out)
  return out_list


def request_data_ors_safe(df, profile="driving-car", metric="distance", units="km", optimized=True):
  out_list = []
  app = out_list.append
  for row in df.itertuples():
    try:
      dist = client.distance_matrix(locations=row.locations,
                                    profile=profile,  # driving-car, foot-walking
                                    metrics=[metric],
                                    units=units,
                                    optimized=optimized,
                                    dry_run=False)[metric+"s"]
      #out = [[row.Index[0], row.Index[1], dist[0][1]], [row.Index[1], row.Index[0], dist[1][0]]]
      out = [row.Index[0], row.Index[1], dist[0][1], dist[1][0]]
      app(out)
    except (requests.exceptions.ConnectionError, ors.exceptions.ApiError, ors.exceptions.HTTPError,
            ors.exceptions.Timeout, ors.exceptions.ValidationError) as error:
      print(f"{row.Index}: {error}")
  return out_list


@numba.jit()
def haversine(lon1, lat1, lon2, lat2):
  dlon = lon2 - lon1
  dlat = lat2 - lat1

  a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
  c = 2 * np.arcsin(np.sqrt(a))
  return earth_radius * c
