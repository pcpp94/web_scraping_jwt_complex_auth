import pandas as pd
import numpy as np
import os
from ..config import COMPILED_OUTPUTS_DIR, BASE_DIR

direc = COMPILED_OUTPUTS_DIR
files = [x for x in os.listdir(direc) if x[-8:] == ".parquet"]
output_path = os.path.join(direc, "merged")
project_dir = BASE_DIR
main_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))


def merge_diario_files():

    diario_files = [x for x in files if x.__contains__("diario")]
    df_diario = pd.DataFrame()

    for file in diario_files:
        aux = pd.read_parquet(os.path.join(direc, file))
        aux["fuente"] = file
        df_diario = pd.concat([df_diario, aux])

    global_variables = (
        df_diario.groupby(by=["variable", "fuente"])["nominal_value"]
        .count()
        .reset_index()["variable"]
        .value_counts()[
            df_diario.groupby(by=["variable", "fuente"])["nominal_value"]
            .count()
            .reset_index()["variable"]
            .value_counts()
            == 4
        ]
        .index.tolist()
    )
    df_diario = df_diario.reset_index(drop=True)
    indices = df_diario[df_diario["variable"].isin(global_variables)].index
    df_diario.loc[indices, "flag"] = True
    df_diario["flag"] = df_diario["flag"].fillna(False)
    df_diario = df_diario.reset_index(drop=True)

    df_diario = df_diario[
        [
            "date",
            "reporte",
            "company_name",
            "company_CSIN_id",
            "company_id",
            "plant_name",
            "plant_CSIN_id",
            "plant_id",
            "filename",
            "variable",
            "remarks",
            "nominal_value",
            "fuente",
            "flag",
        ]
    ]
    df_diario.to_parquet(os.path.join(output_path, "CSIN_diario.parquet"))
    df_diario.to_csv(os.path.join(output_path, "CSIN_diario.csv"))
    print(f"Merged diario CSIN files")


def merge_mensual_files():

    tempo2 = pd.read_csv(
        os.path.join(main_dir, "CSIN__", "utils", "CSIN__dictionary.csv"),
        index_col=0,
    ).reset_index(drop=True)

    month_files = [
        x
        for x in files
        if x.__contains__("mensual") and x != "Plantas_mensual_issues.parquet"
    ]

    df_month = pd.DataFrame()

    for file in month_files:
        aux = pd.read_parquet(os.path.join(direc, file))
        aux["fuente"] = file
        df_month = pd.concat([df_month, aux])

    aux_m = df_month[df_month["sub_reporte"] == "aux"].copy()
    aux_m["company_name"] = aux_m["company_CSIN_id"].map(
        dict(tempo2[["company_CSIN_id", "company_name"]].drop_duplicates().values)
    )
    electricidad_m = df_month[df_month["sub_reporte"] == "electricidad"].copy()
    electricidad_m["company_name"] = electricidad_m["company_CSIN_id"].map(
        dict(tempo2[["company_CSIN_id", "company_name"]].drop_duplicates().values)
    )
    electricidad_m[["unidad_type", "unidad_number"]] = electricidad_m[
        "unidad_id"
    ].str.extract(r"(TD|ST)[\s#-]*(\d+)", expand=False)
    electricidad_m["unidad_id_standard"] = (
        electricidad_m["unidad_type"] + "-" + electricidad_m["unidad_number"]
    )
    electricidad_m = electricidad_m.drop(columns="unidad_number")

    m_list = (
        df_month[df_month["sub_reporte"] == "electricidad"]["company_CSIN_id"]
        .unique()
        .tolist()
    )
    m_list.sort()

    df_diario = pd.read_parquet(os.path.join(output_path, "CSIN_diario.parquet"))
    d_list = df_diario["company_CSIN_id"].unique().tolist()
    d_list.sort()

    companies = [x for x in d_list if x not in m_list]

    # Get the following fields from the diario to add onto the mensual.
    to_keep = [
        "electricidad_aux_mwh",
        "electricidad_aux_mwh_offline",
        "electricidad_gross_mwh",
        "electricidad_net_mwh",
        "gas_m3",
        "gas_MM_YUI",
    ]

    df_aux = df_diario[df_diario["company_CSIN_id"].isin(companies)]
    df_aux["date"] = df_aux["date"].dt.to_period("M")
    df_aux["date"] = pd.to_datetime(df_aux["date"].astype(str) + "-01")
    df_aux = df_aux[df_aux["variable"].isin(to_keep)]
    df_aux["reporte"] = df_aux["reporte"] + "_grouped_M"
    df_aux["sub_reporte"] = "electricidad"
    df_aux["fuente"] = "Plantas_mensual_electricidad.parquet"
    df_aux["unidad_id"] = np.nan

    df_aux = (
        df_aux.groupby(
            by=[
                "date",
                "company_name",
                "company_CSIN_id",
                "plant_name",
                "plant_CSIN_id",
                "unidad_id",
                "reporte",
                "sub_reporte",
                "filename",
                "variable",
                "fuente",
            ],
            dropna=False,
        )["nominal_value"]
        .sum()
        .reset_index()
    )

    unidad_type = dict(
        zip(
            [
                "Solar_grouped_M",
                "Plantas_grouped_M",
                "PV_grouped_M",
                "Hydro_grouped_M",
            ],
            ["Solar", np.nan, "PV", "ST"],
        )
    )

    df_aux["unidad_type"] = df_aux["reporte"].map(unidad_type)
    df_aux["unidad_id_standard"] = np.nan

    df_aux = df_aux[df_aux["nominal_value"] > 0]

    df_aux = df_aux.merge(
        tempo2[["plant_CSIN_id", "plant_id"]].drop_duplicates(),
        how="left",
        on="plant_CSIN_id",
    )
    df_aux = df_aux.merge(
        tempo2[["company_CSIN_id", "company_id"]].drop_duplicates(),
        how="left",
        on="company_CSIN_id",
    )

    electricidad_m = electricidad_m.merge(
        tempo2[["plant_CSIN_id", "plant_id"]].drop_duplicates(),
        how="left",
        on="plant_CSIN_id",
    )
    electricidad_m = electricidad_m.merge(
        tempo2[["company_CSIN_id", "company_id"]].drop_duplicates(),
        how="left",
        on="company_CSIN_id",
    )

    aux_m = aux_m.merge(
        tempo2[["plant_CSIN_id", "plant_id"]].drop_duplicates(),
        how="left",
        on="plant_CSIN_id",
    )
    aux_m = aux_m.merge(
        tempo2[["company_CSIN_id", "company_id"]].drop_duplicates(),
        how="left",
        on="company_CSIN_id",
    )

    df_month = pd.concat([aux_m, electricidad_m, df_aux]).reset_index(drop=True)

    actuals = [
        "efic",
        "efic_net",
        "gasolina_burnt_galones",
        "gas_m3",
        "hours_forced_problema",
        "hours_forced_problema_percentage",
        "hours_month",
        "hours_service",
        "load_factor",
        "electricidad_ae_mwh",
        "electricidad_aux_mwh",
        "electricidad_capacity_mw",
        "electricidad_forced_problema_mwh",
        "electricidad_gross_mwh",
        "electricidad_net_mwh",
        "rel",
        "hours_aux_forced_hours",
        "hours_aux_service",
        "aux_forced",
        "aux_performance_ratio",
        "aux_production_mig",
    ]

    old = pd.read_parquet(
        os.path.join(project_dir, "previous_webpage", "CSIN_mensual.parquet")
    ).rename(columns={"company": "company_id"})
    old["variable"] = old["variable"].apply(
        lambda x: "gasolina_burnt_galones" if x == "gas_KIT_burnt_galones" else x
    )
    old["variable"] = old["variable"].apply(
        lambda x: "gas_m3" if x == "gas_1000_ft3" else x
    )
    old["variable"] = old["variable"].apply(
        lambda x: "gas_m3" if x == "gas_burnt_1000_ft3" else x
    )
    old = old[old["variable"].isin(actuals)]

    old = old[old["reporte"] == "Plantas_Desempeno"]
    old["unidad_id"] = old["unidad_id"].str.strip()
    old["unidad_id"] = old["unidad_id"].apply(
        lambda x: (
            x[::-1].replace("-", ".", 1)[::-1] if x in ["LF-00-1", "LF-30-2"] else x
        )
    )

    indices = old[
        (old["company_id"] == "PRIM") & (old["unidad_id"].isin(["TD-58", "TD-11"]))
    ].index
    old = old.drop(index=indices).reset_index(drop=True)

    old["unidad_id"] = old["unidad_id"].apply(
        lambda x: "Penelope" if x in ["PPL-1", "PPL-2", "PPL-3"] else x
    )
    old = (
        old.groupby(
            by=[
                "unidad_id",
                "reporte",
                "sub_reporte",
                "date",
                "company_id",
                "filename",
                "variable",
                "fuente",
                "unidad_type",
                "unidad_id_standard",
            ],
            dropna=False,
        )["nominal_value"]
        .sum()
        .reset_index()
    )

    old = old.merge(
        df_month[
            [
                "company_name",
                "company_CSIN_id",
                "plant_name",
                "plant_CSIN_id",
                "plant_id",
                "company_id",
                "unidad_id",
            ]
        ].drop_duplicates(),
        how="left",
        on=["company_id", "unidad_id"],
    )

    indices = old[(old["company_id"] == "FFFF")].index
    old.loc[indices, "company_name"] = "FFFF"

    old = old[old["date"] < "2023-01-01"]

    df_month = pd.concat([df_month, old]).reset_index(drop=True)

    df_month.to_parquet(os.path.join(output_path, "CSIN_mensual.parquet"))
    df_month.to_csv(os.path.join(output_path, "CSIN_mensual.csv"))
    print(f"Merged Mensual CSIN files")


def merge_all():
    merge_diario_files()
    merge_mensual_files()


if __name__ == "__main__":
    merge_all()
