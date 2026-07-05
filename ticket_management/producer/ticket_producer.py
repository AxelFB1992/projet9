import os, json, random, time
from datetime import datetime
from kafka import KafkaProducer

TYPES_DEMANDE = [
    "Incident technique",
    "Demande d'information",
    "Facturation",
    "Accès et permissions",
    "Performance"
]

PRIORITES = ["Basse", "Moyenne", "Haute", "Critique"]

DEMANDES = {
    "Incident technique": [
        "Le systeme est inaccessible depuis ce matin",
        "Erreur 500 sur l'interface web",
        "Le capteur IoT ne remonte plus de donnees",
        "Probleme de synchronisation des donnees"
    ],
    "Demande d'information": [
        "Je n'arrive pas a me connecter a mon compte",
        "Demande de reinitialisation de mot de passe",
        "Besoin d'une extraction de donnees historiques",
        "Comment acceder au module de reporting"
    ],
    "Facturation": [
        "Ma facture du mois dernier semble incorrecte",
        "Demande de remboursement suite a double facturation",
        "Besoin d'une facture pro forma",
        "Erreur sur le montant facture"
    ],
    "Acces et permissions": [
        "J'ai besoin d'acces au module de reporting",
        "Demande de creation de compte utilisateur",
        "Mise a jour des droits d'acces",
        "Acces refuse sur le tableau de bord"
    ],
    "Performance": [
        "Les temps de reponse sont tres lents",
        "L'application est tres lente depuis la mise a jour",
        "Latence importante sur les requetes",
        "Timeout frequent sur l'interface"
    ]
}

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda-tickets:9092")

def generer_ticket(ticket_id):
    type_demande = random.choice(TYPES_DEMANDE)
    return {
        "ticket_id": "TKT-{:05d}".format(ticket_id),
        "client_id": "CLI-{:03d}".format(random.randint(1, 50)),
        "created_at": datetime.now().isoformat(),
        "demande": random.choice(DEMANDES[type_demande]),
        "type_demande": type_demande,
        "priorite": random.choice(PRIORITES)
    }

def main():
    print("Tentative de connexion a " + bootstrap_servers, flush=True)
    time.sleep(5)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Connecte a " + bootstrap_servers, flush=True)
    ticket_id = 1
    while True:
        ticket = generer_ticket(ticket_id)
        producer.send("client_tickets", value=ticket)
        producer.flush()
        print("Ticket envoye : {} | {} | {}".format(
            ticket["ticket_id"],
            ticket["type_demande"],
            ticket["priorite"]
        ), flush=True)
        ticket_id += 1
        time.sleep(5)

if __name__ == "__main__":
    main()
