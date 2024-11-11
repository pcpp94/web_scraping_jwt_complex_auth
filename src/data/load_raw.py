import sys
import os
import pandas as pd
import datetime
from ..client.csin_client import CSIN_Client

parent_directory = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
)  # For adding the project URI as a Python PATH.

downloader = CSIN_Client()


def load_mensual_data():

    month_df = pd.read_csv(
        os.path.join(parent_directory, "utils", "mensual_performance_datalist.csv"),
        index_col=0,
    )
    actuals_m = pd.read_parquet(
        os.path.join(
            parent_directory, "compiled_outputs", "merged", "CSIN_mensual.parquet"
        )
    )
    actuals_m = actuals_m[
        (actuals_m["reporte"] == "Plantas_Desempeno")
        & (actuals_m["date"] >= "2023-01-01")
    ]
    data_collected = []
    files = actuals_m["filename"].str.split("|", expand=True).copy()
    for col in files:
        data_collected.append(
            files[col]
            .dropna()
            .apply(lambda x: "_".join(x.split("_")[2:-1]))
            .unique()
            .tolist()
        )
    data_collected = sum(data_collected, [])
    missing_files = [x for x in month_df["id"].tolist() if x not in data_collected]

    month_df = month_df[month_df["id"].isin(missing_files)]

    for id_, electricidad, aux in month_df[
        ["id", "electricidadGrid", "auxGrid"]
    ].values:
        downloader.get_planta_mensual_reporte(id_, electricidad, aux)
        print(f"Downloaded: {id_}")


def load_diario_data():

    actuals_d = pd.read_parquet(
        os.path.join(
            parent_directory, "compiled_outputs", "merged", "CSIN_diario.parquet"
        )
    )

    max_date = []
    for item in actuals_d["reporte"].unique():
        max_date.append(actuals_d[actuals_d["reporte"] == item]["date"].max())
    max_date = min(max_date)
    max_date = datetime.date(max_date.year, max_date.month, 1)
    today = datetime.date.today() - datetime.timedelta(days=7)

    months_to_get = (
        pd.date_range(start=max_date, end=today, freq="d")
        .to_period("M")
        .unique()
        .tolist()
    )
    for year_month in months_to_get:
        year = year_month.year
        month = year_month.month
        downloader.get_planta_diario_reporte(month, year)
        print(f"Downloaded: planta diario for: {year}-{month}")
        downloader.get_solar_diario_reporte(month, year)
        print(f"Downloaded: Solar diario for: {year}-{month}")
        downloader.get_pv_diario_reporte(month, year)
        print(f"Downloaded: PV diario for: {year}-{month}")
        downloader.get_hydro_diario_reporte(month, year)
        print(f"Downloaded: Hydro diario for: {year}-{month}")


def load_all():
    downloader.get_planta_mensual_reporte_list()
    load_mensual_data()
    load_diario_data()


if __name__ == "__main__":
    load_all()
