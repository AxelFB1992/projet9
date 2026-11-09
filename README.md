# projet9 - Modélisez une infrastructure dans le cloud

# Projet 9 — Modélisez une infrastructure dans le cloud

> OpenClassrooms · Parcours Data Engineer
> Architecture de données hybride et POC de traitement en temps réel de tickets clients avec Redpanda et PySpark.

---

## Contexte

Une entreprise souhaite moderniser son infrastructure de données en combinant son système d'information existant et le cloud, et traiter en temps réel les tickets de son support client.

Le projet comporte deux volets :
1. **Modéliser une architecture cible** dans le cloud (AWS), compatible avec le système d'information existant : stockage, entrepôt de données, migration et connexion sécurisée au réseau de l'entreprise.
2. **Démontrer la faisabilité du traitement en temps réel** par un POC : ingestion de tickets dans un broker de messages, transformation et agrégation en continu, stockage au format Parquet.

## Outils

| Domaine | Outils |
|---|---|
| Streaming | Redpanda (compatible Kafka), kafka-python |
| Traitement | PySpark (Structured Streaming) |
| Formats de données | Parquet, Delta Lake, Apache Iceberg |
| Conteneurisation | Docker, Docker Compose |
| Architecture cible | AWS (S3, Redshift, DMS, EC2, VPN, AD Connector) |

## Structure du dépôt

```
projet9/
├── ticket_management/      # Application principale : POC de gestion de tickets en temps réel
│   ├── producer/            # Générateur de tickets (envoi vers Redpanda)
│   ├── processor/           # Traitement PySpark Structured Streaming
│   ├── redpanda/            # Image du broker
│   ├── docker-compose.yml   # Lancement de l'ensemble du pipeline
│   └── README.md            # Documentation détaillée du POC
├── redpanda_demo/          # Démonstration : instance Redpanda et clients rpk
├── pandaonline_analytics/  # Démonstration : producteur automatique et streamer de transformation
├── spark_demo/             # Démonstration : session Spark et lecture des fichiers Parquet du POC
├── deltalake_demo/         # Démonstration : format Delta Lake (historique des versions, partitionnement)
├── apache_iceberg_demo/    # Démonstration : format Apache Iceberg (données de démonstration)
└── requirements.txt
```

## Le POC de gestion de tickets

Le pipeline, entièrement conteneurisé, enchaîne :

1. **Producteur** : génère des tickets clients (identifiant, client, priorité, type de demande, horodatage) et les publie dans un topic Redpanda.
2. **Broker Redpanda** : reçoit les tickets dans un topic partitionné ; une console web permet de suivre les messages.
3. **Processeur PySpark Structured Streaming** :
   - lit le flux en continu ;
   - **assigne automatiquement une équipe de support** selon le type de demande ;
   - calcule des **agrégations en temps réel** par priorité, sur des fenêtres de temps.
4. **Stockage** : tickets enrichis et agrégations écrits au format **Parquet**.

L'architecture, les services Docker, le format des tickets, les commandes de lancement et de vérification sont détaillés dans [`ticket_management/README.md`](ticket_management/README.md).

### Lancer le POC

```bash
cd ticket_management
docker compose up -d --build    # lancement du pipeline complet
docker compose down             # arrêt
```

## Démonstrations complémentaires

Les dossiers de démonstration ont servi à prendre en main les technologies avant le POC :
- **redpanda_demo** : instance Redpanda issue de la documentation officielle, avec production et lecture manuelles de messages via `rpk` ;
- **pandaonline_analytics** : producteur automatique, streamer qui transforme et agrège les messages, et lecteur des messages transformés ;
- **spark_demo** : création d'une session Spark et lecture des fichiers Parquet produits par le POC ;
- **deltalake_demo** et **apache_iceberg_demo** : découverte des formats de table ouverts (versions, partitionnement).

## Difficultés rencontrées et solutions

- Compatibilité des versions de Java et de PySpark.
- Corruption des points de contrôle (checkpoints) de Spark Streaming.
- Incompatibilité entre certaines versions de kafka-python et Redpanda.
- Conflits de ports entre conteneurs Docker.

## Compétences mises en œuvre

- Identifier et sélectionner les composants d'une infrastructure de données
- Évaluer leur compatibilité avec le système d'information existant
- Représenter visuellement et documenter une architecture
- Extraire, transformer et charger des données en temps réel

Remarques (readme.md initial) :

Ce Readme.md est un readme général qui a pour but de présenter les différents repertoires de ce repository contenant les différentes applications implémentées, la plupart pour des démonstrations :
  - deltalake_demo : Démonstration du format de fichier delta via la libraie deltalake sur un script python.
  - apache_iceberg_demo : Démonstration du format de fichier iceberg via la libraie apache_iceberg sur un script python --> J'ai supprimé le script car il ne marchait pas. Il ne reste plus que les données stockés.
  - spark_demo : Démonstration de spark via sa librairie et la création d'une Session spark et d'un dataframe --> J'ai rajouté quelques lignes de visualisation pour afficher les fichiers parquets de l'application générale (ticket_mangament).
  - redpanda_demo : Première démonstration d'utilisation de redpanda avec un docker-compose qui vient de la documentation officiel de redpanda (proposé par le cours Openclassrooms sur redpanda). Le conteneur (redpanda-0 historiquement) lance une instance de redpanda avec un topic, sur laquelle on peut se connecter via différents clients rpk afin soit de produire des messages (producer) manuellement soit de les lire (consumer) manuellement.
  - pandaonline_analytics : Deuxième démonstration d'utilisation de redpanda avec des commandes issues cette fois de la documentation officiel de redpanda (un tutoriel que j'ai mis en favori) et qui permet de lancer en arrière plan une instance de redpanda dans un conteneur (redpanda_1), une instance de classified_producer (déjà implémenté dans la librairie quay.io/systemcraftsman/) qui permet de produire des messages automatiquement à intervalle de temps régulier et une instance de classified_streamer (que l'on a construit avec le fichier python classified_streamer) qui permet non seulement de lire ces messages en continu mais également des procéder à des transformations sur le data frame reçu (tel qu'une agrégation). Ensuite on lance une autre instance de rpk qui permet d'écouter les messages transformés. Ce qui fait 4 entités au final : Un producer (automatique), Un consumer (simple), Un Streamer (RedPanda) et un transformateur (classified_streamer).
  - ticket_management : L'application principal de notre projet qui fait l'objet d'un autre Readme.md dans le dossier ticket_mangament.
