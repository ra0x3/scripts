import os
import sys
import pathlib
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt


def team_ref(x):
    if not x or pd.isnull(x):
        return None
    return x.split()[1].lower()


def fg_made(x):
    if not x or pd.isnull(x):
        return 0
    return x.split("-")[0]


def fg_att(x):
    if not x or pd.isnull(x):
        return 0
    return x.split("-")[1]


def cleaner(x):
    if x == "--":
        return 0
    return x


def to_date(row):
    months = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }

    conds = [
        not row["year"],
        pd.isnull(row["year"]),
        not row["day"],
        pd.isnull(row["day"]),
        not row["month"],
        pd.isnull(row["month"]),
    ]

    if any(conds):
        return None

    year = str(int(row["year"]))
    month = int(row["month"])
    month = months[month]
    day = str(int(row["day"]))

    if len(day) == 1:
        day = "0" + day

    d = f"{month} {day}, {year}"
    d = dt.datetime.strptime(d, "%B %d, %Y")
    return d


def stack_csvs():
    files = os.listdir("data")
    frames = []
    for i in tqdm(range(len(files)), desc="Loading CSV files..."):
        file = files[i]
        p = "data/" + file
        df = pd.read_csv(p, low_memory=False)
        frames.append(df)

    df = pd.concat(frames, axis=0, ignore_index=True)

    print("Processing data...")

    df["team_ref"] = df["team"].apply(team_ref)
    df["threept_made"] = df["threept"].apply(fg_made)
    df["threept_att"] = df["threept"].apply(fg_att)
    df["fg_made"] = df["fg"].apply(fg_made)
    df["fg_att"] = df["fg"].apply(fg_att)
    df["ft_made"] = df["ft"].apply(fg_made)
    df["ft_att"] = df["ft"].apply(fg_att)
    df["t_threept_made"] = df["t_threept"].apply(fg_made)
    df["t_threept_att"] = df["t_threept"].apply(fg_att)
    df["t_fg_made"] = df["t_fg"].apply(fg_made)
    df["t_fg_att"] = df["t_fg"].apply(fg_att)
    df["t_ft_made"] = df["t_ft"].apply(fg_made)
    df["t_ft_att"] = df["t_fg"].apply(fg_att)
    df["pts_reb_ast"] = df["pts"] + df["reb"] + df["ast"]
    df["date"] = df.apply(lambda row: to_date(row), axis=1)

    df.rename(columns={"3pt_p": "threept_p"}, inplace=True)

    try:
        df.drop(["Unnamed: 0.1", "Unnamed: 0"], axis=1, inplace=True)
    except KeyError as err:
        print("Error: ", str(err))

    floats = [
        "min",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pm",
        "pts",
        "t_oreb",
        "t_dreb",
        "t_reb",
        "t_ast",
        "t_stl",
        "t_blk",
        "t_tov",
        "t_pf",
        "t_pm",
        "t_pts",
        "fg_p",
        "threept_p",
        "ft_p",
    ]

    for col in floats:
        try:
            df[col] = df[col].astype("float32")
        except ValueError:
            df[col] = df[col].apply(cleaner)
            df[col] = df[col].astype("float32")

    now = dt.datetime.now().strftime("%Y-%m-%d")
    df.drop_duplicates(inplace=True)
    name = "data/stacked_" + now + ".csv"
    df.to_csv(name)


if __name__ == "__main__":
    stack_csvs()
