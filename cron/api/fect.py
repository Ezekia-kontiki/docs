def start_activities(self):
    try:
        # Lire toutes les sources
        db_liste = self.db_model.read_all()

        date_now = datetime.now()
        json_file = "data.json"

        # Charger l'historique
        if not os.path.exists(json_file) or os.path.getsize(json_file) == 0:
            data_h = []
            isEvent = 0
        else:
            with open(json_file, "r", encoding="utf-8") as f:
                data_h = json.load(f)

            isEvent = data_h[-1].get("isEvent", 0) if data_h else 0

        # --------------------------
        # 📌 GESTION DATE_START / DATE_END
        # --------------------------
        if isEvent == 1:
            # 🔥 Récupérer seulement les données d'aujourd'hui
            date_start = date_now.replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = date_now
        else:
            # 🔥 Prendre les 2 derniers jours (adaptable)
            date_start = date_now - timedelta(days=2)
            date_end = date_now

        news_historiques = []
        alls = []

        # --------------------------
        # 📌 BOUCLE PRINCIPALE ACTIVITÉS
        # --------------------------
        for event in self.activities:
            print(event)

            for db in db_liste:

                # Charger données de base
                data = self.focus_model.extract_data(date_start, date_end, db)
                df = pd.DataFrame(json.loads(data))

                if df.empty:
                    continue

                df['id_router'] = pd.to_numeric(df['id_router'], errors='coerce').fillna(0).astype(int)

                # Récupération des events
                df_event = self.fetch_activities(
                    db_info=db, 
                    event=event,
                    start=date_start,
                    end=date_end
                )

                if df_event is None or df_event.empty:
                    continue

                # Taguer isEvent sur chaque ligne
                for _, row in df_event.iterrows():
                    event_dict = row.to_dict()
                    event_dict["isEvent"] = 1
                    news_historiques.append(event_dict)

                # Champs calculés
                df_event['dwh_id'] = df_event['Email'].map(
                    lambda email: generate_id(db['id'], email, "dggf?s025mPMjdx-mMnFv") 
                    if pd.notna(email) and email else ''
                )

                df_event['event_type'] = event
                df_event['removals_raison'] = df_event['Reason'] if 'Reason' in df_event.columns else ""

                if 'MessageId' not in df_event.columns:
                    df_event['MessageId'] = 0

                if 'Date' not in df_event.columns:
                    print("⚠ Aucune colonne Date dans df_event")
                    continue

                df_event = df_event.rename(columns={'Date': 'date_event'})

                df_event['MessageId'] = pd.to_numeric(df_event['MessageId'], errors='coerce').fillna(0).astype(int)
                df_event['date_event'] = pd.to_datetime(df_event['date_event'], errors='coerce')
                df_event['database_id'] = db['id']

                # Fusion
                df_merge = df[['id_router', 'tag', 'adv_id', 'brand']].rename(columns={'id_router': 'MessageId'})
                df_merge['MessageId'] = pd.to_numeric(df_merge['MessageId'], errors='coerce').fillna(0).astype(int)
                df_merge['adv_id'] = pd.to_numeric(df_merge['adv_id'], errors='coerce').fillna(0).astype(int)

                df_final = df_event.merge(df_merge, on='MessageId', how='le_
