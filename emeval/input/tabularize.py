import numpy as np
import pandas as pd


def constant_valued_column(name: str, value, indices) -> pd.DataFrame:
    return pd.DataFrame({name: np.full(indices.size, value)}, index=indices)


def tabularize_pv_map(pv_map: dict) -> dict:
    df_map = dict()

    for phone_os, phones in pv.map().items():
        for phone_label, phone_map in phones.items():
            pk = {"phone_label": phone_label, "role": phone_map["role"]}
            for r in (phone_map["calibration_ranges"] + phone_map["evaluation_ranges"]):
                for df_label, df in {k: v for k, v in r.items() if isinstance(v, pd.DataFrame)}.items():
                    curr_pk = pk.copy()

                    if r in phone_map["calibration_ranges"]:
                        curr_pk.update({"range_type": "calibration"})
                    elif r in phone_map["evaluation_ranges"]:
                        curr_pk.update({"range_type": "evaluation"})

                    curr_pk.update({k: v for k, v in r.items()
                                    if not (isinstance(v, pd.DataFrame) or isinstance(v, list) or isinstance(v, dict))})

                    updated_df = df.assign(**curr_pk)

                    if r in phone_map["evaluation_ranges"]:
                        for etr in r["evaluation_trip_ranges"]:
                            if df_label in etr:
                                if "trip_range_id" not in updated_df.columns:
                                    updated_df = pd.concat(
                                        [updated_df,
                                         constant_valued_column("trip_range_id", etr["trip_id"], etr[df_label].index),
                                         constant_valued_column("trip_range_id_base", etr["trip_id_base"], etr[df_label].index),
                                         constant_valued_column("trip_range_run", etr["trip_run"], etr[df_label].index)],
                                        axis=1)
                                else:
                                    updated_df.at[etr[df_label].index, "trip_range_id"] = etr["trip_id"]
                                    updated_df.at[etr[df_label].index, "trip_range_id_base"] = etr["trip_id_base"]
                                    updated_df.at[etr[df_label].index, "trip_range_run"] = etr["trip_run"]

                            for esr in etr["evaluation_section_ranges"]:
                                if df_label in esr:
                                    if "trip_section_id" not in updated_df.columns:
                                        updated_df = pd.concat(
                                            [updated_df,
                                             constant_valued_column("trip_section_id", esr["trip_id"], esr[df_label].index),
                                             constant_valued_column("trip_section_id_base", esr["trip_id_base"], esr[df_label].index),
                                             constant_valued_column("trip_section_run", esr["trip_run"], esr[df_label].index)],
                                            axis=1)
                                    else:
                                        updated_df.at[esr[df_label].index, "trip_section_id"] = esr["trip_id"]
                                        updated_df.at[esr[df_label].index, "trip_section_id_base"] = esr["trip_id_base"]
                                        updated_df.at[esr[df_label].index, "trip_section_run"] = esr["trip_run"]

                    for ts_col in [c for c in updated_df.columns.tolist() if "ts" in c]:
                        updated_df[ts_col] = updated_df[ts_col].astype(float)

                    if phone_os not in df_map:
                        df_map[phone_os] = dict()

                    if df_label not in df_map[phone_os]:
                        df_map[phone_os][df_label] = updated_df
                    else:
                        df_map[phone_os][df_label] = pd.concat([df_map[phone_os][df_label], updated_df], ignore_index=True)

    return df_map
