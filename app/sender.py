import psycopg2
from bottle import Bottle, request
import redis
import json
import os

class Sender(Bottle):
    def __init__(self):
        super().__init__()

        self.route("/", method="POST", callback=self.send)
        redis_host=os.getenv("REDIS_HOST","queue")
        self.fila=redis.StrictRedis(host='queue', port=6379, db=0)

        db_host=os.getenv("DB_HOST","db")
        db_user=os.getenv("DB_USER","postgres")
        db_name=os.getenv("DB_NAME","emailsender")
        dsn=f"dbname={db_name} user={db_user} host={db_host}"
        self.conn = psycopg2.connect(dsn)

    def register_message(self, assunto,mensagem):
        SQL = "INSERT INTO emails (assunto,mensagem) VALUES (%s, %s)"
        cur = self.conn.cursor()
        cur.execute(SQL, (assunto, mensagem))
        self.conn.commit()
        cur.close()
        
        msg = {"assunto": assunto, "mensagem": mensagem}
        self.fila.rpush("sender", json.dumps(mensagem))
        print("Mensagem registrada no banco de dados...")

    def send(self):
        assunto = request.forms.get("assunto")
        mensagem = request.forms.get("mensagem")
        self.register_message(assunto, mensagem)
        return f"Mensagem enfileirada: {assunto} {mensagem}"

if __name__=="__main__":
    sender=Sender()
    print("servidor rodando...")
    sender.run(host="0.0.0.0", port=8080, debug=True)
