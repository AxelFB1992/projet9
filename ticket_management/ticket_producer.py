import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Données de simulation
TYPES_DEMANDE = [
    "Incident technique",
    "Demande d'information",
    "Facturation",
    "Accès et permissions",
    "Performance"
]

PRIORITES = ["Basse", "Moyenne", "Haute", "Critique"]

DEMANDES = [
    "Le système est inaccessible depuis ce matin",
    "Je n'arrive pas à me connecter à mon compte",
    "Ma facture du mois dernier semble incorrecte",
    "J'ai besoin d'accès au module de reporting",
    "Les temps de réponse sont très lents",
    "Erreur 500 sur l'interface web",
    "Demande de réinitialisation de mot de passe",
    "Le capteur IoT ne remonte plus de données",
    "Besoin d'une extraction de données historiques",
    "Problème de synchronisation des données"
]

def generer_ticket(ticket_id):
    return {
        "ticket_id": f"TKT-{ticket_id:05d}",
        "client_id": f"CLI-{random.randint(1, 50):03d}",
        "created_at": datetime.now().isoformat(),
        "demande": random.choice(DEMANDES),
        "type_demande": random.choice(TYPES_DEMANDE),
        "priorite": random.choice(PRIORITES)
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Démarrage du producteur de tickets...")
    ticket_id = 1

    while True:
        ticket = generer_ticket(ticket_id)
        producer.send("client_tickets", value=ticket)
        print(f"Ticket envoyé : {ticket['ticket_id']} | "
              f"Client : {ticket['client_id']} | "
              f"Priorité : {ticket['priorite']} | "
              f"Type : {ticket['type_demande']}")
        ticket_id += 1
        time.sleep(5)  # Un ticket toutes les 2 secondes

if __name__ == "__main__":
    main()
