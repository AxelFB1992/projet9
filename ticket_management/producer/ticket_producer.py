import os, json, random, time
from datetime import datetime
from kafka import KafkaProducer

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda-tickets:9092")

def generer_ticket(ticket_id):
    return {
        "ticket_id": "TKT-{:05d}".format(ticket_id),
        "client_id": "CLI-{:03d}".format(random.randint(1, 50)),
        "created_at": datetime.now().isoformat(),
        "demande": "Test",
        "type_demande": "Incident technique",
        "priorite": "Haute"
    }

def main():
    print("Connexion a " + bootstrap_servers, flush=True)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Connecte!", flush=True)
    ticket_id = 1
    while True:
        ticket = generer_ticket(ticket_id)
        producer.send("client_tickets", value=ticket)
        producer.flush()
        print("Envoye : " + ticket["ticket_id"], flush=True)
        ticket_id += 1
        time.sleep(5)

if __name__ == "__main__":
    main()
