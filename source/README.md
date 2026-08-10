Déploiement des Bases de Données avec Docker Compose

Prérequis

    Docker installé sur votre machine

    Docker Compose installé

    Le réseau Docker datalake_network doit être créé au préalable

Structure des Fichiers

.
├── docker-compose.yml
├── data/                   # Contient les fichiers CSV à importer dans MongoDB
│   ├── fichier1.csv
│   ├── fichier2.csv
│   └── fichier3.csv
├── init-db/                # Contient les scripts d'initialisation pour PostgreSQL
│   └── script.sh
└── README.md

Instructions d'Installation

Lancez les services avec Docker Compose:

docker-compose up -d

Importation des Données
Pour MongoDB

Les fichiers CSV dans le dossier data doivent être importés dans des collections MongoDB. Voici comment procéder:

    Connectez-vous au conteneur MongoDB:

    docker exec -it mongodb bash

    Dans le conteneur, utilisez mongoimport pour importer chaque fichier CSV:

    mongoimport --uri "mongodb://root:rootpassword@localhost:27017/togo_voyages" \
    --collection profiles \
    --type csv \
    --headerline \
    --file /csvfiles/profiles_week_27.csv

    mongoimport --uri "mongodb://root:rootpassword@localhost:27017/togo_voyages" \
    --collection visas \
    --type csv \
    --headerline \
    --file /csvfiles/visas_week_27.csv

    mongoimport --uri "mongodb://root:rootpassword@localhost:27017/togo_voyages" \
    --collection travels \
    --type csv \
    --headerline \
    --file /csvfiles/travels_week_27.csv

Pour PostgreSQL

Le script dans init-db/script.sh sera automatiquement exécuté lors du démarrage du conteneur PostgreSQL. Si vous devez l'exécuter manuellement:

    Connectez-vous au conteneur PostgreSQL:
    docker exec -it postgres_db bash

    Exécutez le script:
    psql -U marc -d etablissement_db -f /scripts/script.sh