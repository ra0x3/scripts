import time
import os
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import re
import itertools
import multiprocessing
from tqdm import tqdm
import psycopg2

min_game_id = 401126813
max_game_id = 401474910

def injury_report():
    url = "https://www.cbssports.com/nba/injuries/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    data = []
    stack = []

    for item in soup.find_all("td", class_="TableBase-bodyTd"):
        parts = list(filter(lambda x: not (not x), item.text.split(" ")))
        stack.append(parts)
        if not len(stack) % 5:
            name, pos, date, injury, status = stack[-5:]
            stack = []

            name = name[1]
            pos = pos[1]
            date = " ".join(date[2:]).strip("\n")
            injury = injury[1]
            status = " ".join(status[1:])

            data.append({"name": name, "position": pos, "date": date, "injury": injury, "status": status})

    df = pd.DataFrame(data)
    filename = "nba_injury_report_" + dt.datetime.now().strftime("%Y-%m-%d") + ".csv"
    df.to_csv(filename)

    return df


def stat_as_pcnt(x):
    if not x or "-" not in x:
        return x
    x, y = x.split("-")
    if y == "0":
        return 0.0
    return round(float(x) / float(y), 3)


def is_pcnt_stat(x):
    return "-" in x and x.index("-") > 0


def box_score(game_id, cursor):
    url = "https://www.espn.com/nba/boxscore/_/gameId/" + game_id
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    aggregate = soup.find_all(class_=["ResponsiveTable", "ResponsiveTable--fixed-left", "Boxscore"])[1]
    items = [item for item in aggregate.find_all(class_=["Table__TR Table__TR--sm Table__even"])]
    meta = [x.text for x in soup.find_all(class_=["GameInfo__Meta"])][0].split(",")

    teams = []
    headers = []
    data = []
    skip = set()

    for i, item in enumerate(items):
        link = item.find(class_="Boxscore__AthleteName")
        if link:
            data.append(
                {
                    "player": link.text,
                    "dnp": 0,
                    "dnpr": None,
                    "vs": None,
                    "game_id": game_id,
                }
            )
            skip.add(i)
            continue

    months = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12,
    }

    for _, item in enumerate(items):
        header = item.find_all("div", class_="Table__customHeader")
        if header and len(header) == 14:
            labels = [x.text.lower() for x in item.find_all("div", class_="Table__customHeader")]
            if labels[0] == "min":
                for d in data:
                    for l in labels:
                        d[l] = None
                        if len(headers) < 14:
                            headers.append(l)
                skip.add(i)

    for _, item in enumerate(items):
        if not teams:
            teams_ = [item.find("img").get("alt") for item in soup.find_all(class_="Boxscore__Title")]
            records_ = [x.text for x in soup.find_all(class_=["Gamestrip__Record"])]
            scores_ = list(
                itertools.chain(*[x.text.split() for x in soup.find_all(class_=["Gamestrip__Score", "relative"])])
            )
            scores_ = [x for x in scores_ if x.startswith("home") or x.startswith("away")]
            scores_ = [int(re.findall(r"\d+", s)[0]) for s in scores_]

            for j, (t, r, s) in enumerate(zip(teams_, records_, scores_)):
                other_j = 1 if j == 0 else 0
                d = dict([(l, None) for l in headers])

                ((a, b), (e, f)) = [x.split(" ")[0].split("-") for x in r.split(", ")]
                d["team"] = t
                d["home"] = int(j == 0)
                d["w"] = int(scores_[j] > scores_[other_j])
                d["score"] = s
                d["homew"] = e
                d["homel"] = f
                d["awayw"] = a
                d["awayl"] = b
                d["vs"] = teams_[other_j]
                d["time"] = meta[0]
                d["year"] = meta[2][:5]
                d["month"] = months[meta[1].split()[0].strip()]
                d["day"] = meta[1][-2:]
                teams.append(d)

    player = team = 0

    for i, item in enumerate(items):
        if i in skip:
            continue

        stats = [x.text for x in item.find_all("td", class_=["Table__TD"])]
        if len(stats) == 14 and not stats[0] and stats[-1]:
            for s, h in zip(stats, headers):
                if is_pcnt_stat(s):
                    sp = stat_as_pcnt(s)
                    hp = h + "_p"
                    teams[team][hp] = sp
                teams[team][h] = s
            team += 1

    team = 0

    for i, item in enumerate(items):
        if i in skip:
            continue

        stats = [x.text for x in item.find_all("td", class_=["Table__TD"])]
        if len(stats) == 14 and not stats[0] and stats[-1]:
            for s, h in zip(stats, headers):
                h = "t_" + h
                teams[team][h] = s
            team += 1

    player = team = bk = 0
    pos = -1

    for i, item in enumerate(items):
        if i in skip:
            continue

        stats = [x.text for x in item.find_all("td", class_=["Table__TD"])]
        if stats[0] == "MIN":
            pos += 1

        bk = 0 if pos <= 1 else 1

        if len(stats) == 14 and stats[0] and stats[0] != "MIN":
            for s, h in zip(stats, headers):
                if is_pcnt_stat(s):
                    sp = stat_as_pcnt(s)
                    hp = h + "_p"
                    data[player][hp] = sp
                data[player]["team"] = teams[bk]["team"]
                data[player]["vs"] = teams[bk]["vs"]
                data[player]["month"] = teams[bk]["month"]
                data[player]["day"] = teams[bk]["day"]
                data[player]["year"] = teams[bk]["year"]
                data[player]["time"] = teams[bk]["time"]
                data[player]["home"] = teams[bk]["home"]
                data[player][h] = s

                for k, v in teams[bk].items():
                    if k.startswith("t_"):
                        data[player][k] = v
            player += 1

        elif len(stats) == 1 and stats[0] and stats[0] == stats[0].upper():
            data[player]["dnp"] = 1
            data[player]["dnpr"] = stats[0]
            data[player]["team"] = teams[bk]["team"]
            data[player]["vs"] = teams[bk]["vs"]
            data[player]["month"] = teams[bk]["month"]
            data[player]["day"] = teams[bk]["day"]
            data[player]["year"] = teams[bk]["year"]
            data[player]["time"] = teams[bk]["time"]
            data[player]["home"] = teams[bk]["home"]

            for k, v in teams[bk].items():
                if k.startswith("t_"):
                    data[player][k] = v

            player += 1

    df = pd.DataFrame(data)
    df = df.rename(
        columns={
            "3pt": "threept",
            "t_3pt": "t_threept",
            "t_+/-": "t_pm",
            "+/-": "pm",
            "to": "tov",
            "t_to": "t_tov",
            "3pt_p": "threept_p",
        }
    )
    df_filename = "data/boxscore_" + game_id + "_" + dt.datetime.now().strftime("%Y-%m-%d") + ".csv"
    df.to_csv(df_filename)

    return df, response.text


def scrape_task(items):
    conn = psycopg2.connect(dbname="nba", user="postgres", password="", host="localhost", port=5432)
    cursor = conn.cursor()
    for i in tqdm(range(len(items)), desc="Scraping boxscore data..."):
        game_id = items[i]
        cursor.execute("SELECT * FROM site WHERE game_id = %s", (str(game_id),))
        data = cursor.fetchone()
        if data:
            print("Game({}) already scraped...".format(game_id))
            continue
        print("Grabbing game: {}".format(game_id))
        try:
            (_, html) = box_score(str(game_id), cursor)
            cursor.execute(f"INSERT INTO site (game_id, html) VALUES (%s, %s)", (game_id, html))
        except Exception as err:
            print("Failed to get info for game({}): {}".format(game_id, str(err)))
            cursor.execute(f"INSERT INTO site (game_id, html) VALUES (%s, %s)", (game_id, "null"))
        conn.commit()
        time.sleep(1)


def into_n_chunks(x, n):
    for i in range(n):
        yield x[i::n]


if __name__ == "__main__":
    num_procs = multiprocessing.cpu_count()
    game_ids = list(range(min_game_id, max_game_id))
    chunks = into_n_chunks(game_ids, num_procs)

    for chunk in chunks:
        tasks = [multiprocessing.Process(target=scrape_task, args=(chunk,)) for chunk in chunks]
        for i, task in enumerate(tasks):
            task.start()
            print("Starting process ({}/{}): {}".format(i, i + 1, num_procs, task.name))

        for task in tasks:
            task.join()
