import redis
import json
from time import sleep
from random import randint
import os

if __name__ == "__main__":
    redis_host=os.getenv("REDIS_HOST","queue")
    r = redis.Redis(host=redis_host, port=6379, db=0)
    print("aguardando mensagens...")
    while True:
        mensagem = json.loads(r.blpop("sender")[1])
        print("Enviando a mensagem... ", mensagem['assunto'])
        sleep(randint(15,45))
        print("mensagem enviada com sucesso!!")