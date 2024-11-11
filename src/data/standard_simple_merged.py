import pandas as pd
import numpy as np
import os
from ..config import COMPILED_OUTPUTS_DIR, BASE_DIR

direc = os.path.join(COMPILED_OUTPUTS_DIR, "merged")
files = [x for x in os.listdir(direc) if x[-8:] == ".parquet"]
output_path = os.path.join(direc, "standard")
project_dir = BASE_DIR
main_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))

mmscf_to_MM_YUI = 1030


def clean_diario_files():

    diario = pd.read_parquet(os.path.join(direc, "CSIN_diario.parquet")).reset_index(
        drop=True
    )

    to_drop = [
        "accumulated_dni_kwh/m2",
        "KIT_received",
        "gasolina_received_DD",
        "emerg_gasolina_generator_imperial_gallons",
        "gas_m3",
        "gas_consumption_MM_ft3",
        "heat_trans_fluid_KIT_imperial_gallons",
        "higher_heating_value",
        "htf_heaters_on_gas_m3",
        "max_dni_kwh/m2",
        "solar_heat_input_mwh",
    ]
    solar_index = diario[
        (diario["reporte"] == "Solar") & (diario["variable"].isin(to_drop))
    ].index
    diario = diario.drop(index=solar_index).reset_index(drop=True)

    to_drop = [
        "gas_1000_ft3",
        "gas_m3",
        "oil_adj",
        "gasolina_adj",
        "high_heat_value",
        "low_heat_value",
        "oil_received_DD",
        "oil_consumo_DD",
        "gasolina_received_DD",
        "gasolina_consumo_DD",
    ]
    planta_index = diario[
        (diario["reporte"] == "Plantas") & (diario["variable"].isin(to_drop))
    ].index
    diario = diario.drop(index=planta_index).reset_index(drop=True)

    to_drop = ["aux_sea_temp_celsius"]
    hydro_index = diario[
        (diario["reporte"] == "Hydro") & (diario["variable"].isin(to_drop))
    ].index
    diario = diario.drop(index=hydro_index).reset_index(drop=True)

    to_drop = [
        "average_ghi_mwh",
        "aux_import",
        "electricidad_pv_capacity_AC_mw",
        "electricidad_pv_capacity_DC_mw",
    ]
    pv_index = diario[
        (diario["reporte"] == "PV") & (diario["variable"].isin(to_drop))
    ].index
    diario = diario.drop(index=pv_index).drop_duplicates().reset_index(drop=True)

    gas_df = (
        diario[(diario["variable"].isin(["gas_MM_YUI", "gas_m3"]))]
        .pivot(
            index=[
                "date",
                "reporte",
                "company_name",
                "company_CSIN_id",
                "company_id",
                "plant_name",
                "plant_CSIN_id",
                "plant_id",
                "filename",
                "remarks",
                "fuente",
                "flag",
            ],
            columns="variable",
            values="nominal_value",
        )
        .reset_index()
    )
    diario = diario[~(diario["variable"].isin(["gas_MM_YUI", "gas_m3"]))]
    gas_df["gas_MM_YUI"] = gas_df["gas_MM_YUI"].fillna(0)
    gas_df["gas_m3"] = gas_df["gas_m3"].fillna(0)
    gas_df["gas_MM_YUI"] = gas_df.apply(
        lambda x: (
            np.nan
            if ((x["gas_MM_YUI"] == 0) and (x["gas_m3"] > 0))
            else x["gas_MM_YUI"]
        ),
        axis=1,
    )
    gas_df["gas_m3"] = gas_df.apply(
        lambda x: (
            np.nan if ((x["gas_m3"] == 0) and (x["gas_MM_YUI"] > 0)) else x["gas_m3"]
        ),
        axis=1,
    )
    gas_df["gas_MM_YUI"] = gas_df.apply(
        lambda x: (
            x["gas_m3"] * mmscf_to_MM_YUI
            if pd.isna(x["gas_MM_YUI"])
            else x["gas_MM_YUI"]
        ),
        axis=1,
    )
    gas_df["gas_m3"] = gas_df.apply(
        lambda x: (
            x["gas_MM_YUI"] / mmscf_to_MM_YUI if pd.isna(x["gas_m3"]) else x["gas_m3"]
        ),
        axis=1,
    )
    gas_df = pd.melt(
        gas_df,
        id_vars=[
            "date",
            "reporte",
            "company_name",
            "company_CSIN_id",
            "company_id",
            "plant_name",
            "plant_CSIN_id",
            "plant_id",
            "filename",
            "remarks",
            "fuente",
            "flag",
        ],
        value_vars=["gas_m3", "gas_MM_YUI"],
        var_name="variable",
        value_name="nominal_value",
    )
    diario = pd.concat([diario, gas_df]).reset_index(drop=True)

    diario.to_parquet(os.path.join(output_path, "CSIN_diario_standard.parquet"))
    diario.to_csv(os.path.join(output_path, "CSIN_diario_standard.csv"))
    print(f"Standard diario CSIN files OK")


def clean_mensual_files():

    mensual = pd.read_parquet(os.path.join(direc, "CSIN_mensual.parquet")).reset_index(
        drop=True
    )

    to_drop = [
        "gas_KIT_burnt_galones",
        "gasolina_burnt_galones",
        "efic_net",
        "hours_forced_problema_percentage",
        "electricidad_ae_mwh",
        "rel",
    ]
    electricidad_index = mensual[
        (mensual["reporte"] == "Plantas_Desempeno")
        & (mensual["sub_reporte"] == "electricidad")
        & (mensual["variable"].isin(to_drop))
    ].index
    mensual = mensual.drop(index=electricidad_index).reset_index(drop=True)
    mensual.loc[change_index, "nominal_value"] = (
        mensual.loc[change_index, "nominal_value"] / 1000
    )
    change_index = mensual[mensual["variable"] == "gas_m3"].index
    tempo = mensual.loc[change_index, :].copy()
    tempo["nominal_value"] = tempo["nominal_value"] * mmscf_to_MM_YUI
    tempo["variable"] = "gas_MM_YUI"
    mensual = pd.concat([mensual, tempo]).reset_index(drop=True)

    to_drop = ["aux_forced"]
    aux_index = mensual[
        (mensual["reporte"] == "Plantas_Desempeno")
        & (mensual["sub_reporte"] == "aux")
        & (mensual["variable"].isin(to_drop))
    ].index
    mensual = mensual.drop(index=aux_index).reset_index(drop=True)
    change_index = mensual[mensual["variable"] == "aux_production_mig"].index
    mensual.loc[change_index, "nominal_value"] = (
        mensual.loc[change_index, "nominal_value"] * 1000
    )
    mensual.loc[change_index, "variable"] = "aux_production_tig"

    mensual.to_parquet(os.path.join(output_path, "CSIN_mensual_standard.parquet"))
    mensual.to_csv(os.path.join(output_path, "CSIN_mensual_standard.csv"))
    print(f"Standard Mensual CSIN files OK")


def clean_standard():
    clean_diario_files()
    clean_mensual_files()


if __name__ == "__main__":
    clean_standard()
