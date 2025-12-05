from fastapi import FastAPI, HTTPException
import clickhouse_connect

app = FastAPI()

# Connexion client ClickHouse
client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="",
    database="planifik"
)

@app.get("/tags")
def get_tags():
    try:
        query = "SELECT id, tag, dwtag FROM tags ORDER BY id"
        result = client.query(query)
        
        # Convertir en liste de dict
        tags = [
            {"id": row[0], "tag": row[1], "dwtag": row[2]}
            for row in result.result_rows
        ]

        return {"count": len(tags), "data": tags}

    except Exception as e:
        raise HTTPException(500, f"Erreur lors de la récupération des tags : {e}")

@app.get("/tags/{tag_id}")
def get_tag_by_id(tag_id: int):
    try:
        query = f"SELECT id, tag, dwtag FROM tags WHERE id = {tag_id}"
        result = client.query(query)

        if not result.result_rows:
            raise HTTPException(404, "Tag non trouvé")

        row = result.result_rows[0]
        return {"id": row[0], "tag": row[1], "dwtag": row[2]}

    except Exception as e:
        raise HTTPException(500, f"Erreur : {e}")
