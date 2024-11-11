import os
import pandas as pd
import numpy as np
import re

from ..config import OUTPUTS_DIR, COMPILED_OUTPUTS_DIR, BASE_DIR

outputs_dir = OUTPUTS_DIR
compiled_outputs_dir = COMPILED_OUTPUTS_DIR
main_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))
project_dir = BASE_DIR


pattern = re.compile(r"(electricidad)|(aux)")

files = pd.DataFrame({"files": os.listdir(outputs_dir)})
new_files = files["files"].str.split("_", expand=True)
new_files[0].unique()
files_df = files.copy()
files_df["reporte"] = "fill"

index_ = files[files["files"].str.startswith("solar")].index
files_df.loc[index_, "reporte"] = "solar"
files = files[~files["files"].str.startswith("solar")]
index_ = files[files["files"].str.startswith("pv")].index
files_df.loc[index_, "reporte"] = "pv"
files = files[~files["files"].str.startswith("pv")]
index_ = files[files["files"].str.startswith("hydro")].index
files_df.loc[index_, "reporte"] = "hydro"
files = files[~files["files"].str.startswith("hydro")]
index_ = files[files["files"].str.startswith("plantas_mensual")].index
files_df.loc[index_, "reporte"] = "plantas_mensual"
files = files[~files["files"].str.startswith("plantas_mensual")]
index_ = files[files["files"].str.startswith("plantas_diario")].index
files_df.loc[index_, "reporte"] = "plantas_diario"
files = files[~files["files"].str.startswith("plantas_diario")]
files_m = files_df[files_df["reporte"] == "plantas_mensual"]
files_m["reporte"] = (
    files_m["reporte"]
    + "_"
    + files_m["files"].apply(lambda x: pattern.search(x).group(0))
)


def compile_plantas_diario():

    df = pd.DataFrame()

    for file in files_df[files_df["reporte"] == "plantas_diario"]["files"].tolist():
        aux = pd.read_csv(os.path.join(outputs_dir, file), index_col=0)
        aux["filename"] = file
        df = pd.concat([df, aux])
    df["reporte"] = "Plantas"
    df = df.drop(columns=["id", "submittedDate", "status", "type"])

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = pd.melt(
        df,
        id_vars=[
            "date",
            "remarks",
            "company_CSIN_id",
            "company_name",
            "plant_CSIN_id",
            "plant_id",
            "plant_name",
            "reporte",
            "filename",
        ],
        value_vars=[
            "electricidad_gross_kwh",
            "aux_gross_tig",
            "electricidad_net_kwh",
            "aux_net_tig",
            "gas_m3",
            "gas_m3",
            "gas_MM_YUI",
            "gasolina_received_DD",
            "gasolina_consumo_DD",
            "gasolina_adj",
            "high_heat_value",
            "low_heat_value",
            "gasolina_closing_stock",
            "gasolina_opening_stock",
        ],
        var_name="variable",
        value_name="nominal_value",
    )

    for plant in df["plant_id"].unique():
        for var in df[df["plant_id"] == plant]["variable"].unique():
            indices = df[(df["plant_id"] == plant) & (df["variable"] == var)].index
            crazy_up = (
                df.loc[indices, "nominal_value"].quantile(0.99)
                + (
                    df.loc[indices, "nominal_value"].quantile(0.99)
                    - df.loc[indices, "nominal_value"].median()
                )
                * 7
            )
            crazy_down = (
                df.loc[indices, "nominal_value"].quantile(0.01)
                + (
                    df.loc[indices, "nominal_value"].quantile(0.01)
                    - df.loc[indices, "nominal_value"].median()
                )
                * 2
            )
            # print(plant, var, len(df.loc[indices, 'nominal_value'][(df.loc[indices, 'nominal_value'] > crazy_up) | (df.loc[indices, 'nominal_value'] < crazy_down)])/len(df.loc[indices, 'nominal_value']))
            df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
                lambda x: np.nan if ((x > crazy_up) | (x < crazy_down)) else x
            )

    for change in [x for x in df["variable"].unique() if x.__contains__("kwh")]:
        indices = df[df["variable"] == change].index
        df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"] / 1000
    renaming = dict(
        zip(
            [x for x in df["variable"].unique() if x.__contains__("kwh")],
            [
                x.replace("kwh", "mwh")
                for x in df["variable"].unique()
                if x.__contains__("kwh")
            ],
        )
    )
    df["variable"] = df["variable"].replace(renaming)

    gross = (
        df[df["variable"] == "electricidad_gross_mwh"]
        .drop(columns=["remarks", "variable"])
        .rename(columns={"nominal_value": "gross"})
    )
    net = (
        df[df["variable"] == "electricidad_net_mwh"]
        .drop(columns=["remarks", "variable"])
        .rename(columns={"nominal_value": "net"})
    )
    aux = gross.merge(
        net,
        how="inner",
        on=[
            "date",
            "company_CSIN_id",
            "company_name",
            "plant_CSIN_id",
            "plant_id",
            "plant_name",
            "reporte",
            "filename",
        ],
    )
    aux["nominal_value"] = aux["gross"] - aux["net"]
    aux = aux.drop(columns=["gross", "net"])
    aux["variable"] = "electricidad_aux_mwh"
    aux["remarks"] = None
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = pd.concat([df, aux])
    df = df.reset_index(drop=True)

    df.to_csv(os.path.join(compiled_outputs_dir, "Plantas_diario.csv"))
    df.to_parquet(os.path.join(compiled_outputs_dir, "Plantas_diario.parquet"))
    print(f"Compiled diario Plantas")


def compile_plantas_mensual():

    df = pd.DataFrame()

    for file, sub_reporte in files_m[
        files_m["reporte"] == "plantas_mensual_electricidad"
    ].values:
        aux = pd.read_csv(os.path.join(outputs_dir, file), index_col=0)
        aux["sub_reporte"] = sub_reporte.split("_")[-1]
        aux["filename"] = file
        df = pd.concat([df, aux])

    df["reporte"] = "Plantas_Desempeno"
    df = df.drop(columns="id")
    df = df.rename(
        columns=(
            {
                "reporteDate": "date",
                "unidadId": "unidad_id",
                "usuarioId": "company_CSIN_id",
                "usuarioName": "company_name",
                "objetoId": "plant_CSIN_id",
                "objetoDescription": "plant_name",
                "generationMwh": "electricidad_gross_mwh",
                "exportMwh": "electricidad_net_mwh",
                "auxMuh": "electricidad_aux_mwh",
                "loadFactor": "load_factor",
                "efic": "efic",
                "electricidadFor": "hours_forced_problema_percentage",
                "gasBurnt": "gas_m3",
                "KITBurnt": "gasolina_galones",
                "capacityMwh": "electricidad_capacity_mw",
                "monthHrs": "hours_month",
                "electricidadFohHours": "hours_forced_problema",
                "aeMwh": "electricidad_ae_mwh",
                "foMwh": "electricidad_forced_problema_mwh",
                "rel": "rel",
                "serviceHours": "hours_service",
                "netefic": "efic_net",
            }
        )
    )

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = pd.melt(
        df,
        id_vars=[
            "date",
            "company_name",
            "company_CSIN_id",
            "plant_name",
            "plant_CSIN_id",
            "unidad_id",
            "reporte",
            "sub_reporte",
            "filename",
        ],
        value_vars=[
            "electricidad_gross_mwh",
            "electricidad_net_mwh",
            "electricidad_aux_mwh",
            "load_factor",
            "efic",
            "hours_forced_problema_percentage",
            "gas_m3",
            "gasolina_galones",
            "electricidad_capacity_mw",
            "hours_forced_problema",
            "hours_month",
            "electricidad_ae_mwh",
            "electricidad_forced_problema_mwh",
            "rel",
            "hours_service",
            "efic_net",
        ],
        var_name="variable",
        value_name="nominal_value",
    )

    df = (
        df.groupby(
            by=[
                "date",
                "company_name",
                "company_CSIN_id",
                "plant_name",
                "plant_CSIN_id",
                "unidad_id",
                "reporte",
                "sub_reporte",
                "variable",
            ],
            dropna=False,
        )
        .agg({"filename": lambda x: "|".join(x), "nominal_value": "mean"})
        .reset_index()
    )

    items = ["electricidad_gross_mwh", "electricidad_net_mwh", "electricidad_aux_mwh"]
    for item in items:
        indices = df[
            (df["company_CSIN_id"] == "KMM_14") & (df["variable"] == item)
        ].index
        df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"] / 1000

    items = ["load_factor"]
    for item in items:
        indices = df[
            (df["company_CSIN_id"] == "KMM_14") & (df["variable"] == item)
        ].index
        df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
            lambda x: x / 1000 if x > 100 else x
        )

    items = ["gasolina_galones"]
    for item in items:
        indices = df[(df["variable"] == item)].index
        df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
            lambda x: 0 if x > 10000 else x
        )

    for unidad in df["unidad_id"].unique():
        for var in df[df["unidad_id"] == unidad]["variable"].unique():
            indices = df[(df["unidad_id"] == unidad) & (df["variable"] == var)].index
            crazy_up = (
                df.loc[indices, "nominal_value"].quantile(0.99)
                + (
                    df.loc[indices, "nominal_value"].quantile(0.99)
                    - df.loc[indices, "nominal_value"].median()
                )
                * 7
            )
            crazy_down = (
                df.loc[indices, "nominal_value"].quantile(0.01)
                + (
                    df.loc[indices, "nominal_value"].quantile(0.01)
                    - df.loc[indices, "nominal_value"].median()
                )
                * 2
            )
            # print(unidad, var, len(df.loc[indices, 'nominal_value'][(df.loc[indices, 'nominal_value'] > crazy_up) | (df.loc[indices, 'nominal_value'] < crazy_down)])/len(df.loc[indices, 'nominal_value']))
            df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
                lambda x: np.nan if ((x > crazy_up) | (x < crazy_down)) else x
            )

    df.to_csv(os.path.join(compiled_outputs_dir, "Plantas_mensual_electricidad.csv"))
    df.to_parquet(
        os.path.join(compiled_outputs_dir, "Plantas_mensual_electricidad.parquet")
    )
    print(f"Compiled Mensual Plantas electricidad")

    df = pd.DataFrame()

    for file, sub_reporte in files_m[
        files_m["reporte"] == "plantas_mensual_aux"
    ].values:
        aux = pd.read_csv(os.path.join(outputs_dir, file), index_col=0)
        aux["sub_reporte"] = sub_reporte.split("_")[-1]
        aux["filename"] = file
        df = pd.concat([df, aux])

    df["reporte"] = "Plantas_Desempeno"
    df = df.drop(columns="id")
    df = df.rename(
        columns=(
            {
                "reporteDate": "date",
                "unidadId": "unidad_id",
                "usuarioId": "company_CSIN_id",
                "usuarioName": "company_name",
                "objetoId": "plant_CSIN_id",
                "objetoDescription": "plant_name",
                "productionMDD": "aux_production_mig",
                "performanceRatio": "aux_performance_ratio",
                "auxFor": "aux_forced",
                "auxFohHrs": "hours_aux_forced_hours",
                "serviceHrs": "hours_aux_service",
            }
        )
    )

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = pd.melt(
        df,
        id_vars=[
            "date",
            "company_name",
            "company_CSIN_id",
            "plant_name",
            "plant_CSIN_id",
            "unidad_id",
            "reporte",
            "sub_reporte",
            "filename",
        ],
        value_vars=[
            "aux_production_mig",
            "aux_performance_ratio",
            "aux_forced",
            "hours_aux_forced_hours",
            "hours_aux_service",
        ],
        var_name="variable",
        value_name="nominal_value",
    )

    df = (
        df.groupby(
            by=[
                "date",
                "company_name",
                "company_CSIN_id",
                "plant_name",
                "plant_CSIN_id",
                "unidad_id",
                "reporte",
                "sub_reporte",
                "variable",
            ],
            dropna=False,
        )
        .agg({"filename": lambda x: "|".join(x), "nominal_value": "mean"})
        .reset_index()
    )

    for unidad in df["unidad_id"].unique():
        for var in df[df["unidad_id"] == unidad]["variable"].unique():
            indices = df[(df["unidad_id"] == unidad) & (df["variable"] == var)].index
            iqf = df.loc[indices, "nominal_value"].quantile(0.75) - df.loc[
                indices, "nominal_value"
            ].quantile(0.25)
            upper_bound = df.loc[indices, "nominal_value"].quantile(0.75) + iqf * 1.5
            df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
                lambda x: x / 1000 if (x > upper_bound) else x
            )

    df.to_csv(os.path.join(compiled_outputs_dir, "Plantas_mensual_aux.csv"))
    df.to_parquet(os.path.join(compiled_outputs_dir, "Plantas_mensual_aux.parquet"))
    print(f"Compiled Mensual Plantas aux")


def compile_solar_diario():

    df = pd.DataFrame()

    for file in files_df[files_df["reporte"] == "solar"]["files"].tolist():
        aux = pd.read_csv(os.path.join(outputs_dir, file), index_col=0)
        aux["filename"] = file
        df = pd.concat([df, aux])

    df["reporte"] = "Solar"
    df = df.drop(columns=["id", "submittedDate", "status", "createdDate"])
    df = df.rename(
        columns={
            "usuarioId": "company_CSIN_id",
            "usuarioName": "company_name",
            "objetoId": "plant_CSIN_id",
            "objetoDescription": "plant_name",
            "reporteDate": "date",
            "energyGen": "electricidad_gross_mwh",
            "energyExp": "electricidad_net_mwh",
            "syncHours": "sync_hours",
            "gasConsMmscf": "gas_m3",
            "gasConsMmbtu": "gas_MM_YUI",
            "KITReceived": "gasolina_received_DD",
            "KITOpeningBalance": "gasolina_opening_stock_DD",
            "KITClosingStock": "gasolina_closing_stock_DD",
        }
    )

    ids = [
        "date",
        "remarks",
        "company_CSIN_id",
        "company_name",
        "plant_CSIN_id",
        "plant_name",
        "reporte",
        "filename",
    ]
    values = [x for x in df.columns if x not in ids]
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = pd.melt(
        df,
        id_vars=ids,
        value_vars=values,
        var_name="variable",
        value_name="nominal_value",
    )

    df = (
        df.groupby(by=[x for x in ids if x != "filename"] + ["variable"], dropna=False)
        .agg({"filename": lambda x: "|".join(x), "nominal_value": "mean"})
        .reset_index()
    )

    for plant in df["plant_name"].unique():
        for var in df[df["plant_name"] == plant]["variable"].unique():
            indices = df[(df["plant_name"] == plant) & (df["variable"] == var)].index
            iqf = df.loc[indices, "nominal_value"].quantile(0.95) - df.loc[
                indices, "nominal_value"
            ].quantile(0.05)
            upper_bound = df.loc[indices, "nominal_value"].quantile(0.95) + iqf * 1.5
            lower_bound = df.loc[indices, "nominal_value"].quantile(0.05) - iqf * 1.5
            # print(f"{var}:{iqf} {upper_bound}, {lower_bound}")
            df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
                lambda x: np.nan if ((x > upper_bound) | (x < lower_bound)) else x
            )

    df.to_csv(os.path.join(compiled_outputs_dir, "Solar_diario.csv"))
    df.to_parquet(os.path.join(compiled_outputs_dir, "Solar_diario.parquet"))
    print(f"Compiled Solar")


def compile_hydro_diario():

    df = pd.DataFrame()

    for file in files_df[files_df["reporte"] == "hydro"]["files"].tolist():
        aux = pd.read_csv(os.path.join(outputs_dir, file), index_col=0)
        aux["filename"] = file
        df = pd.concat([df, aux])

    df["reporte"] = "Hydro"

    df = df.drop(columns=["id", "submittedDate", "status", "type", "grossMWh"])
    df = df.rename(
        columns={
            "usuarioId": "company_CSIN_id",
            "usuarioName": "company_name",
            "objetoId": "plant_CSIN_id",
            "objetoId.1": "plant_id",
            "objetoDescription": "plant_name",
            "reporteDate": "date",
            "grossGenerationMWh": "electricidad_gross_mwh",
            "auxilaryConsumptionMWh": "electricidad_aux_mwh",
            "fohMWh": "forced_problema_loss_gross_mwh",
            "pohMWh": "planned_problema_loss_gross_mwh",
        }
    )

    ids = [
        "date",
        "remarks",
        "company_CSIN_id",
        "company_name",
        "plant_CSIN_id",
        "plant_id",
        "plant_name",
        "reporte",
        "filename",
    ]
    values = [x for x in df.columns if x not in ids]
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = pd.melt(
        df,
        id_vars=ids,
        value_vars=values,
        var_name="variable",
        value_name="nominal_value",
    )

    df = (
        df.groupby(by=[x for x in ids if x != "filename"] + ["variable"], dropna=False)
        .agg({"filename": lambda x: "|".join(x), "nominal_value": "mean"})
        .reset_index()
    )
    for plant in df["plant_id"].unique():
        for var in df[df["plant_id"] == plant]["variable"].unique():
            indices = df[(df["plant_id"] == plant) & (df["variable"] == var)].index
            iqf = df.loc[indices, "nominal_value"].quantile(0.95) - df.loc[
                indices, "nominal_value"
            ].quantile(0.05)
            upper_bound = df.loc[indices, "nominal_value"].quantile(0.95) + iqf * 1.5
            lower_bound = df.loc[indices, "nominal_value"].quantile(0.05) - iqf * 1.5
            # print(f"{var}:{iqf} {upper_bound}, {lower_bound}")
            df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
                lambda x: np.nan if ((x > upper_bound) | (x < lower_bound)) else x
            )

    df.to_csv(os.path.join(compiled_outputs_dir, "Hydro_diario.csv"))
    df.to_parquet(os.path.join(compiled_outputs_dir, "Hydro_diario.parquet"))
    print(f"Compiled Hydro")


def compile_pv_diario():

    df = pd.DataFrame()

    for file in files_df[files_df["reporte"] == "pv"]["files"].tolist():
        aux = pd.read_csv(os.path.join(outputs_dir, file), index_col=0)
        aux["filename"] = file
        df = pd.concat([df, aux])

    df["reporteDate"] = pd.to_datetime(df["reporteDate"], format="%Y-%m-%d")

    df["reporte"] = "PV"
    df = df.drop(columns=["id", "submittedDate", "status", "createdDate"])
    df = df.rename(
        columns={
            "usuarioId": "company_CSIN_id",
            "usuarioName": "company_name",
            "objetoId": "plant_CSIN_id",
            "objetoDescription": "plant_name",
            "reporteDate": "date",
            "grossGenerationCapacityDc": "electricidad_pv_capacity_DC_mw",
            "grossGenerationCapacityAc": "electricidad_pv_capacity_AC_mw",
            "grossEnergyGenerationForecast": "electricidad_gross_mwh_forecast",
            "grossEnergyGenerationInverter": "electricidad_gross_mwh",
            "energyExport": "electricidad_net_mwh",
            "auxEnergyConsumptionOffline": "electricidad_aux_mwh_offline",
            "auxEnergyConsumptionOnline": "electricidad_aux_mwh",
            "peakMaximumGeneration": "electricidad_peak_mw",
            "peakMaximumGenerationTime": "electricidad_peak_time",
        }
    )

    ids = [
        "date",
        "remarks",
        "company_CSIN_id",
        "company_name",
        "plant_CSIN_id",
        "plant_name",
        "reporte",
        "filename",
    ]
    values = [x for x in df.columns if x not in ids]

    df = pd.melt(
        df,
        id_vars=ids,
        value_vars=values,
        var_name="variable",
        value_name="nominal_value",
    )

    df = (
        df.groupby(by=[x for x in ids if x != "filename"] + ["variable"], dropna=False)
        .agg({"filename": lambda x: "|".join(x), "nominal_value": "mean"})
        .reset_index()
    )
    for plant in df["plant_name"].unique():
        for var in df[df["plant_name"] == plant]["variable"].unique():
            indices = df[(df["plant_name"] == plant) & (df["variable"] == var)].index
            iqf = df.loc[indices, "nominal_value"].quantile(0.95) - df.loc[
                indices, "nominal_value"
            ].quantile(0.05)
            upper_bound = df.loc[indices, "nominal_value"].quantile(0.95) + iqf * 1.5
            lower_bound = df.loc[indices, "nominal_value"].quantile(0.05) - iqf * 1.5
            # print(f"{var}:{iqf} {upper_bound}, {lower_bound}")
            df.loc[indices, "nominal_value"] = df.loc[indices, "nominal_value"].apply(
                lambda x: np.nan if ((x > upper_bound) | (x < lower_bound)) else x
            )

    df.to_csv(os.path.join(compiled_outputs_dir, "PV_diario.csv"))
    df.to_parquet(os.path.join(compiled_outputs_dir, "PV_diario.parquet"))
    print(f"Compiled PV")


def compile_all():
    compile_plantas_mensual()
    compile_plantas_diario()
    compile_hydro_diario()
    compile_pv_diario()
    compile_solar_diario()


if __name__ == "__main__":
    compile_all()
