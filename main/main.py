from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import os
import json
date_now = datetime.now()
date_5_jours = date_now - timedelta(days=5)
json_file = "data.json"

if not os.path.exists(json_file) or os.path.getsize(json_file) == 0:
    data = []
    isEvent = 0
else:
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        isEvent = data[-1].get("isEvent", 0)

if isEvent == 0:
    date_start = date_5_jours
    date_end = date_now
else:
    date_start = date_now
    date_end = date_now

news_historiques = []

for event in self.activities:
    for db in db_liste:
        historiques = self.fetch_activities(
            db_info=db,
            event=event,
            start=date_start,
            end=date_end
        )
        if historiques:
            isEvent = 1   # on passe isEvent à 1 après récupération réussie
            for h in historiques:
                news_historiques.append(h)


data.extend(news_historiques)


for d in data:
    d["isEvent"] = isEvent


with open(json_file, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)

print("historiques sauvegardées !!")


 """historiques = self.fetch_activities(db_info=db,event=event,start=date_start,end=date_end)
                  if historiques is not None and not historiques.empty:
                    isEvent = 1
                    for _, historique in historiques.iterrows():
                        news_historiques.append(historique.to_dict())

            data_h.extend(news_historiques)
            for d in data_h:
                d["isEvent"]=isEvent
            with open (json_file, "w", encoding="utf-8") as f:
                json.dump(data_h, f, indent=4)
                print("historiques sauvegardées")
        except Exception as e:
                print("Erreur activités",e)"""


def start_activities(self):
    try:
        db_liste = self.db_model.read_all()

        date_now = datetime.now()
        date_3_mois = date_now - relativedelta(months=3)
        json_file = "data.json"

        # ----------------------------
        # Charger données JSON
        # ----------------------------
        if not os.path.exists(json_file) or os.path.getsize(json_file) == 0:
            data_h = []
            isEvent = 0
        else:
            with open(json_file, "r") as f:
                data_h = json.load(f)

            # récupérer isEvent depuis le dernier élément
            isEvent = data_h[-1].get("isEvent", 0) if data_h else 0

        # ----------------------------
        # Définir période
        # ----------------------------
        if isEvent == 0:
            date_start = date_3_mois
            date_end = date_now
        else:
            date_start = date_now
            date_end = date_now

        news_historiques = []  # contiendra les nouveaux events
        alls = []  # contiendra tout pour ClickHouse

        # ----------------------------
        # Parcours des events + DB
        # ----------------------------
        for event in self.activities:
            print(event)

            for db in db_liste:

                # Récupération des données router
                data = self.focus_model.extract_data(date_start, date_end, db)
                df = pd.DataFrame(json.loads(data))

                ids = list(set(df['id_router'].to_list()))

                # Récupération des events (API)
                df_event = self.fetch_activities(
                    db_info=db, 
                    event=event,
                    start=date_start, 
                    end=date_end
                )

                # Si événements trouvés
                if df_event is not None and not df_event.empty:
                    isEvent = 1

                    # Ajouter events au JSON
                    for _, row in df_event.iterrows():
                        event_dict = row.to_dict()
                        event_dict["isEvent"] = isEvent
                        news_historiques.append(event_dict)

                # Préparation ClickHouse
                df_event['dwh_id'] = df_event['Email'].map(
                    lambda email: generate_id(
                        db['id'], email, "dggf?s025mPMjdx-mMnFv"
                    ) if pd.notna(email) and email else ''
                )

                df_event['event_type'] = event

                df_event['removals_raison'] = df_event['Reason'] if 'Reason' in df_event.columns else ""

                if 'MessageId' not in df_event.columns:
                    df_event['MessageId'] = 0

                df_event = df_event[['dwh_id', 'event_type', 'removals_raison', 'MessageId', 'Date']]
                df_event['database_id'] = db['id']

                df_event['MessageId'] = pd.to_numeric(df_event['MessageId'], errors='coerce').fillna(0).astype(int)
                df_event['Date'] = pd.to_datetime(df_event['Date'], errors='coerce')

                alls.append(df_event)

        # ----------------------------
        # Sauvegarde JSON (une seule fois)
        # ----------------------------
        data_h.extend(news_historiques)

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data_h, f, indent=4, ensure_ascii=False)

        print("historiques sauvegardées dans JSON")

        # ----------------------------
        # Merge pour ClickHouse
        # ----------------------------
        if alls:
            ds = pd.concat(alls, ignore_index=True)

            df_merge = df[['id_router', 'tag', 'adv_id', 'brand']].rename(columns={'id_router': 'MessageId'})
            df_merge['MessageId'] = pd.to_numeric(df_merge['MessageId'], errors='coerce').fillna(0).astype(int)
            df_merge['adv_id'] = pd.to_numeric(df_merge['adv_id'], errors='coerce').fillna(0).astype(int)

            ds = ds.merge(df_merge, on='MessageId', how='left')

            # chunk insert
            chunk_size = 100000
            chunks = np.array_split(ds, max(1, len(ds) // chunk_size))

            for chunk in chunks:
                self.events_model.insert_dataframe(chunk)

            ds.to_csv('dsfqs.csv', index=False)

            del df, ds, df_event, df_merge

    except Exception as e:
        print('error activities', e)
        pass
