def __init__(self, config=None, path_file=None):
        self.config = config
        self.path_file = path_file
        self.url_old = "https://stats.kontikimedia.com/publicapi"
        self.url = "https://stats.kontikimedia.com/publicapi/statsapi"
        self.url_es = "https://stats.kontikimedia.com/publicapi/esapi"
 
        self.url_stats_sms = "https://stats.kontikimedia.com/publicapi/cmapi"
        self.timeout = 10
        self.path_tags = path_file + '/Utilities/stats_backup/stats_tags.txt'
        self.path_country = path_file +  '/Utilities/stats_backup/stats_country.txt'
        self.path_advertiser = path_file + '/Utilities/stats_backup/stats_advertiser.txt'
        self.path_models =  path_file + '/Utilities/stats_backup/stats_models.txt'
        self.path_clients =  path_file + '/Utilities/stats_backup/stats_clients.txt'
        self.stats_update = 0
        self.stats_data = []
 
def getListTags(self, apikey):
        try:
            if os.path.exists(self.path_tags):
                with open(self.path_tags, 'r') as fic:
                    data = json.load(fic)
                return data
 
            with requests.post(self.url + "/gettags", data={"userapikey": apikey}, timeout=self.timeout) as r:
                result = r.json()
            if 'auth' in result[0]:
                return []
            else:
                newlist = sorted(result, key=lambda d: d['tag'])
                with open(self.path_tags, 'w') as fic:
                    json.dump(newlist, fic)
                return newlist
        except Exception as e:
            self.logger.error(f"error for getting tags list : {e}")
            return []
 
 
 
apikey: b48fac8d4d8bb8200896ca4c66ca0180
 


 import pandas as pd

def startGetTags(self):
    apikey = Config.TAGS_CONF["apikey"]
    tags = self.getListTags(apikey)

    for tag in tags:
        df = pd.DataFrame([tag])  # ← conversion dict → DataFrame
        self.tags_model.insert_dataframe(df)

    print("ok")



import pandas as pd
from datetime import datetime

def startGetTags(self):
    try:
        apikey = Config.TAGS_CONF["apikey"]
        tags = self.getListTags(apikey)

        if not tags:
            print("Aucun tag récupéré")
            return

        # Convertir tags (list[dict]) en DataFrame ClickHouse
        df = pd.DataFrame(tags)

        # Ajouter une colonne dwtag si absente
        if "dwtag" not in df.columns:
            df["dwtag"] = df["tag"].map(
                lambda x: x.lower().replace(" ", "_").replace("-", "_")
            )

        # 1. Vider la table
        self.tags_model.execute("TRUNCATE TABLE planifik.tags")

        # 2. Réinsérer les nouveaux tags
        self.tags_model.insert_dataframe(df)

        print("Tags mis à jour avec succès")

    except Exception as e:
        print("Erreur lors de startGetTags :", e)
