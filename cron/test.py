from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def test_root():
    return {"Message":"world"}


users = [
    {"id": 1, "nom": "rakot"},
    {"id": 2, "nom": "test"},
    {"id": 3, "nom": "test1"},
]

@app.get("/users/")
async def get_users():
    return users

@app.get("/users/{user_id}")
async def get_user(user_id: int):
    for client in users:
        if client["id"] == user_id:
            return client
        return {"error": "user introuvable"}

listes = [
    {"id": 1, "name": "Pomme"},
    {"id": 2, "name": "Banane"},
    {"id": 3, "name": "Orange"},
]

@app.get("/items/{item_id}")
async def read_item(item_id: int):
    for item in listes:
        if item["id"] == item_id:
            return item
    return {"error": "item introuvable"}


from apscheduler.schedulers.background import BackgroundScheduler

def start_cron(self):
    scheduler = BackgroundScheduler()

    # Exécuter toutes les 1 heure
    scheduler.add_job(
        self.startGetTags,
        trigger='interval',
        hours=1,
        id='tags_cron',
        replace_existing=True
    )

    scheduler.start()
    print("Cron Tags démarré → toutes les 1h")
