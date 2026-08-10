import pandas as pd
import json
import io
import sys
import logging
import tempfile
import os
import re
from hdfs import InsecureClient

# Configuration de la journalisation
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paramètres WebHDFS
HDFS_URL = "http://namenode:50070"
HDFS_USER = "nifi"

# Définir les dictionnaires indicators, subindicators, et categories
categories = {
    "C1": "Accès des ménages et des particuliers aux TIC et l’utilisation de ces technologies",
    "C2": "Cybergouvernement",
    "C3": "Cybersecurité",
    "C4": "Economie et démographie",
    "C5": "Education",
    "C6": "Emploi, recettes et investissement",
    "C7": "Indicateurs de diffusion",
    "C8": "Infrastructure des TIC et l’accès à ces technologies",
    "C9": "Internet",
    "C10": "Mobilemoney",
    "C12": "Prix",
    "C13": "Qualité de service",
    "C14": "Réseaux cellulaires mobiles",
    "C15": "Réseaux de téléphonie fixe",
    "C16": "Secteur des TIC et le commerce des biens de ce secteur",
    "C17": "TIC dans le secteur de l’éducation",
    "C18": "Trafic",
    "C19": "Utilisation des TIC par les entreprises"
}

indicatore = {
  "SC9i9": {"label": "Capacité de largeur de bande des liaisons internationales opérationnelles/montées", "unit": "Mbit/s", "notes": ""},
  "SC14i3": {"label": "Abonnements à la téléphonie mobile cellulaire par postpaiement", "unit": "Nombre", "notes": ""},
  "SC14i9": {"label": "Abonnements au large bande mobile, données et téléphonie (ou large bande ordinaire)", "unit": "Nombre", "notes": ""},
  "SC9i4": {"label": "Abonnements Internet par technologie DSL", "unit": "Nombre", "notes": ""},
  "SC9i3": {"label": "Abonnements Internet par fibre jusqu'au domicile/bâtiment", "unit": "Nombre", "notes": ""},
  "SC9i8": {"label": "Autres abonnements au large bande fixe", "unit": "Nombre", "notes": ""},
  "SC3i15": {"label": "Nombre d'accords entre agences gouvernementales et acteurs privés pour la cybersécurité", "unit": "Nombre", "notes": ""},
  "SC3i6": {"label": "Existence de cadres législatifs pour la cybersécurité", "unit": "Oui/Non", "notes": ""},
  "SC9i10": {"label": "Largeur de bande internationale par utilisateur de l'internet", "unit": "bit/s", "notes": ""},
  "SC2i12": {"label": "Principaux services en ligne sur l’Internet proposé à la population, par niveau de sophistication du service", "unit": "Niveau", "notes": ""},
  "SC3i8": {"label": "Existence de politiques de cybersécurité pour les entreprises publiques et privées", "unit": "Oui/Non", "notes": ""},
  "SC4i1": {"label": "Pourcentage de la population en milieu urbain", "unit": "Pourcentage", "notes": ""},
  "SC16i10": {"label": "Importations de services TIC, en pourcentage des importations totales de services", "unit": "Pourcentage", "notes": ""},
  "SC16i4": {"label": "Exportations de services fondés sur les TIC, en pourcentage des exportations totales de services", "unit": "Pourcentage", "notes": ""},
  "SC8i1": {"label": "Nombre d’abonnements à la radiodiffusion télévisuelle pour 100 habitants", "unit": "Nombre/100 habitants", "notes": ""},
  "SC3i35": {"label": "Nombre de cyberincidents liés à des infrastructures nationales critiques", "unit": "Nombre", "notes": ""},
  "SC3i39": {"label": "Nombre de menaces cybernétiques provenant d'acteurs étatiques", "unit": "Nombre", "notes": ""},
  "SC3i41": {"label": "Nombre de programmes de formation continue pour les professionnels de la cybersécurité", "unit": "Nombre", "notes": ""},
  "SC3i42": {"label": "Nombre de tentatives de phishing", "unit": "Nombre", "notes": ""},
  "SC3i21": {"label": "Nombre d'attaques visant les dispositifs mobiles", "unit": "Nombre", "notes": ""},
  "SC3i22": {"label": "Nombre de campagnes de cyberespionnage détectées", "unit": "Nombre", "notes": ""},
  "SC3i38": {"label": "Nombre de malware détectés", "unit": "Nombre", "notes": ""},
  "SC17i1": {"label": "Proportion d’élèves disposant d’un accès à l’Internet à l’école", "unit": "Pourcentage", "notes": ""},
  "SC2i15": {"label": "Proportion des administrations publiques ayant accès à l’Internet, par type d’accès", "unit": "Pourcentage", "notes": ""},
  "SC2i16": {"label": "Proportion des administrations publiques ayant un intranet", "unit": "Pourcentage", "notes": ""},
  "SC2i17": {"label": "Proportion des administrations publiques ayant un réseau local (LAN)", "unit": "Pourcentage", "notes": ""},
  "SC2i18": {"label": "Proportion des administrations publiques présentes sur le web", "unit": "Pourcentage", "notes": ""},
  "SC2i13": {"label": "Proportion de fonctionnaires des administrations publiques utilisant couramment des ordinateurs", "unit": "Pourcentage", "notes": ""},
  "SC2i14": {"label": "Proportion de fonctionnaires des administrations publiques utilisant couramment l’Internet", "unit": "Pourcentage", "notes": ""},
  "PC19i11": {"label": "Proportion des personnes employées utilisant régulièrement des ordinateurs", "unit": "Pourcentage", "notes": ""},
  "SC17i4": {"label": "Proportion d’écoles disposant d’un accès à l’Internet, par mode d’accès", "unit": "Pourcentage", "notes": ""},
  "SC17i3": {"label": "Proportion d’écoles ayant l’électricité", "unit": "Pourcentage", "notes": ""},
  "SC17i7": {"label": "Proportion d’écoles disposant d’une installation de communication téléphonique", "unit": "Pourcentage", "notes": ""},
  "SC17i2": {"label": "Proportion d’enseignants des écoles qualifiés dans le domaine des TIC", "unit": "Pourcentage", "notes": ""},
  "PC19i4": {"label": "Proportion des entreprises ayant une présence sur le web", "unit": "Pourcentage", "notes": ""},
  "PC19i7": {"label": "Proportion des entreprises utilisant des ordinateurs", "unit": "Pourcentage", "notes": ""},
  "SC17i9": {"label": "Rapport du nombre d’élèves au nombre d’ordinateurs dans les écoles offrant un enseignement assisté par ordinateur", "unit": "Ratio", "notes": ""},
  "PC1i28": {"label": "Proportion des particuliers qui ont acheté des biens ou des services en ligne, par type de bien et de service acheté", "unit": "Pourcentage", "notes": ""},
  "PC1i27": {"label": "Proportion de particuliers qui ont acheté des biens ou des services en ligne, par mode de livraison", "unit": "Pourcentage", "notes": ""},
  "PC1i2": {"label": "Proportion de particuliers qui ont acheté des biens ou des services en ligne, par type de moyen de paiement", "unit": "Pourcentage", "notes": ""},
  "PC1i17": {"label": "Pourcentage de particuliers n'utilisant pas l'internet, par type de raison", "unit": "Pourcentage", "notes": ""},
  "PC1i1": {"label": "Proportion de particuliers qui n'ont pas acheté de biens ou de services en ligne, par type de raison", "unit": "Pourcentage", "notes": ""},
  "SC14i11": {"label": "Abonnements prépayés à la téléphonie mobile cellulaire", "unit": "Nombre", "notes": ""},
  "SC3i43": {"label": "Nombre de violations de la vie privée liées à des cyberattaques", "unit": "Nombre", "notes": ""},
  "SC3i37": {"label": "Nombre de fuites de données dues à des employés internes (menaces internes)", "unit": "Nombre", "notes": ""},
  "SC3i36": {"label": "Nombre de cyberpoliciers formés", "unit": "Nombre", "notes": ""},
  "SC10i1": {"label": "Le nombre d'agents enregistrés et actifs par lesquels les clients accèdent aux services de mobile money", "unit": "Nombre", "notes": ""},
  "SC13i7": {"label": "Taux de baisse des pannes réseau annuelles", "unit": "Pourcentage", "notes": ""},
  "PC1i22": {"label": "Pourcentage de personnes ayant des compétences en TIC, par type de compétences", "unit": "Pourcentage", "notes": ""},
  "SC15i7": {"label": "Nombre de ménages couverts par des réseaux de télévision par câble (CATV) utilisant des câbles coaxiaux", "unit": "Nombre", "notes": ""},
  "SC15i8": {"label": "Nombre de ménages couverts par des réseaux fibre jusqu'aux locaux", "unit": "Nombre", "notes": ""},
  "SC18i20": {"label": "Trafic téléphonique fixe à fixe local, en minutes", "unit": "Minutes", "notes": ""},
  "SC15i6": {"label": "Nombre de ménages couverts par d'autres réseaux fixes filaires (autres que cuivre/DSL, CATV et FTTP)", "unit": "Nombre", "notes": ""},
  "PC1i21": {"label": "Pourcentage de particuliers utilisant l'internet, par type d'activité", "unit": "Pourcentage", "notes": ""},
  "SC13i5": {"label": "Délai de résolution des défaillances pour le service large bande fixe, en heures", "unit": "Heures", "notes": ""},
  "SC15i3": {"label": "Abonnements pour un débit de 10 Mbit/s à moins de 30 Mbit/s", "unit": "Nombre", "notes": ""},
  "SC15i5": {"label": "Abonnements pour un débit égal ou supérieur à 100 Mbit/s", "unit": "Nombre", "notes": ""},
  "SC15i4": {"label": "Abonnements pour un débit de 30 Mbit/s à moins de 100 Mbit/s", "unit": "Nombre", "notes": ""},
  "SC9i20": {"label": "Vitesse moyenne de téléchargement sur les réseaux fixes", "unit": "Mbit/s", "notes": ""},
  "SC5i4": {"label": "Taux brut de scolarisation", "unit": "Pourcentage", "notes": ""},
  "PC1i36": {"label": "Taux de pénétration de l'Internet dans la population", "unit": "Pourcentage", "notes": ""},
  "PC1i19": {"label": "Pourcentage de particuliers utilisant l'internet, par fréquence d'utilisation", "unit": "Pourcentage", "notes": ""},
  "SC18i25": {"label": "Trafic téléphonique international entrant total, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i26": {"label": "Trafic téléphonique international sortant total, en minutes", "unit": "Minutes", "notes": ""},
  "SC9i7": {"label": "Abonnements pour un débit supérieur ou égal à 10 Mbit/s", "unit": "Nombre", "notes": ""},
  "SC9i5": {"label": "Abonnements pour un débit allant de 2 Mbit/s à 10 Mbit/s", "unit": "Nombre", "notes": ""},
  "SC9i6": {"label": "Abonnements pour un débit allant de 256 kbit/s à 2 Mbit/s", "unit": "Nombre", "notes": ""},
  "SC16i9": {"label": "Importations de services fondés sur les TIC, en pourcentage des importations totales de services", "unit": "Pourcentage", "notes": ""},
  "SC3i40": {"label": "Nombre de personnes formées à la cybersécurité", "unit": "Nombre", "notes": ""},
  "SC3i9": {"label": "Existence de politiques de réponse aux incidents cybernétiques au niveau gouvernemental", "unit": "Oui/Non", "notes": ""},
  "PC1i29": {"label": "Proportion des particuliers utilisant l’Internet, par type d’appareil portable, et réseau utilisé pour accéder à Internet", "unit": "Pourcentage", "notes": ""},
  "PC1i18": {"label": "Pourcentage de particuliers possédant un téléphone mobile cellulaire", "unit": "Pourcentage", "notes": ""},
  "SC10i2": {"label": "Le nombre d'abonnements mobile money", "unit": "Nombre", "notes": ""},
  "SC3i29": {"label": "Nombre de cyberattaques impliquant des services gouvernementaux", "unit": "Nombre", "notes": ""},
  "SC10i5": {"label": "Nombre de point de vente mobile money pour 5000 habitants", "unit": "Nombre/5000 habitants", "notes": ""},
  "SC10i4": {"label": "La valeur des transactions traitées par l'industrie à travers différents produits", "unit": "USD", "notes": ""},
  "SC6i8": {"label": "Investissements annuels dans le service téléphonique fixe", "unit": "USD", "notes": ""},
  "SC6i10": {"label": "Investissements annuels dans les services de large bande fixe", "unit": "USD", "notes": ""},
  "SC6i19": {"label": "Personnes employées par les opérateurs de télécommunications mobiles", "unit": "Nombre", "notes": ""},
  "SC9i11": {"label": "Largeur de bande internationale", "unit": "Mbit/s", "notes": ""},
  "SC6i3": {"label": "Capacité de stockage des centres de données", "unit": "Octets", "notes": ""},
  "SC16i3": {"label": "Contribution du secteur des TIC à la valeur ajoutée brute", "unit": "Pourcentage", "notes": ""},
  "SC16i12": {"label": "Part des biens TIC en pourcentage du commerce total, annuel", "unit": "Pourcentage", "notes": ""},
  "PC16i1": {"label": "Proportion de la population active totale du secteur des entreprises occupée dans le secteur des TIC", "unit": "Pourcentage", "notes": ""},
  "PC19i1": {"label": "Proportion des entreprises ayant un extranet", "unit": "Pourcentage", "notes": ""},
  "PC19i3": {"label": "Proportion des entreprises ayant un réseau local (LAN)", "unit": "Pourcentage", "notes": ""},
  "PC19i5": {"label": "Proportion des entreprises passant des commandes par Internet", "unit": "Pourcentage", "notes": ""},
  "PC19i6": {"label": "Proportion des entreprises recevant des commandes par Internet", "unit": "Pourcentage", "notes": ""},
  "PC19i9": {"label": "Proportion des entreprises utilisant l’Internet, par type d’accès", "unit": "Pourcentage", "notes": ""},
  "PC19i10": {"label": "Proportion des entreprises utilisant l’Internet, par type d’activités", "unit": "Pourcentage", "notes": ""},
  "PC19i8": {"label": "Proportion des entreprises utilisant l’Internet", "unit": "Pourcentage", "notes": ""},
  "PC19i2": {"label": "Proportion des entreprises ayant un intranet", "unit": "Pourcentage", "notes": ""},
  "PC19i12": {"label": "Proportion des personnes employées utilisant régulièrement l’Internet", "unit": "Pourcentage", "notes": ""},
  "SC14i20": {"label": "Quantité de spectre exploitée sous licence pour les systèmes IMT", "unit": "MHz", "notes": ""},
  "SC14i17": {"label": "Quantité de spectre exploitée sous licence pour les systèmes IMT dans le bloc < 1GHz", "unit": "MHz", "notes": ""},
  "SC14i19": {"label": "Quantité de spectre exploitée sous licence pour les systèmes IMT dans le bloc 1 GHz-6 GHz", "unit": "MHz", "notes": ""},
  "SC14i18": {"label": "Quantité de spectre exploitée sous licence pour les systèmes IMT dans le bloc > 6 GHz", "unit": "MHz", "notes": ""},
  "SC14i24": {"label": "Quantité de spectre mise à disposition pour les systèmes IMT", "unit": "MHz", "notes": ""},
  "SC14i21": {"label": "Quantité de spectre mise à disposition pour les systèmes IMT dans le bloc < 1GHz", "unit": "MHz", "notes": ""},
  "SC14i23": {"label": "Quantité de spectre mise à disposition pour les systèmes IMT dans le bloc 1 GHz-6 GHz", "unit": "MHz", "notes": ""},
  "SC14i22": {"label": "Quantité de spectre mise à disposition pour les systèmes IMT dans le bloc > 6 GHz", "unit": "MHz", "notes": ""},
  "SC4i2": {"label": "Taille de la population", "unit": "Nombre", "notes": ""},
  "SC9i21": {"label": "Vitesse moyenne de téléchargement sur les réseaux mobiles", "unit": "Mbit/s", "notes": ""},
  "SC3i1": {"label": "Adoption de l'intelligence artificielle dans la cybersécurité", "unit": "Niveau", "notes": ""},
  "SC3i2": {"label": "Budget national consacré à la cybersécurité", "unit": "USD", "notes": ""},
  "SC3i14": {"label": "Nombre d’attaques zero-day découvertes", "unit": "Nombre", "notes": ""},
  "SC3i33": {"label": "Nombre de cyberattaques sur les systèmes industriels (SCADA, OT)", "unit": "Nombre", "notes": ""},
  "SC3i34": {"label": "Nombre de cybercriminels arrêtés ou poursuivis", "unit": "Nombre", "notes": ""},
  "SC15i9": {"label": "Nombre de ménages desservis par le réseau à fils de cuivre classique", "unit": "Nombre", "notes": ""},
  "SC18i8": {"label": "Total du trafic vocal mobile sortant, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i9": {"label": "Total du trafic vocal sortant (émis depuis) des réseaux de téléphonie fixe, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i6": {"label": "Total du trafic vocal entrant à destination des réseaux de téléphonie fixe, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i7": {"label": "Total du trafic vocal entrant à destination des réseaux mobiles, en minutes", "unit": "Minutes", "notes": ""},
  "SC10i3": {"label": "Le nombre de services de mobile money actifs dans le pays", "unit": "Nombre", "notes": ""},
  "PC1i6": {"label": "Nombre d'utilisateurs actifs des services de santé numérique (télémédecine, dossiers médicaux électroniques)", "unit": "Nombre", "notes": ""},
  "SC6i12": {"label": "Nombre de câbles sous-marins internationaux", "unit": "Nombre", "notes": ""},
  "SC12i3": {"label": "Panier de la téléphonie mobile cellulaire – forte consommation (140 minutes, 70 SMS et 2 Go)", "unit": "USD", "notes": ""},
  "SC12i1": {"label": "Panier de la téléphonie mobile cellulaire – faible consommation (70 mins + 20 SMS+ 500 MB)", "unit": "USD", "notes": ""},
  "SC12i2": {"label": "Panier de la téléphonie mobile cellulaire – faible consommation (70 mins + 20 SMS)", "unit": "USD", "notes": ""},
  "SC12i4": {"label": "Panier du large bande fixe 5 Go", "unit": "USD", "notes": ""},
  "SC12i5": {"label": "Panier du large bande mobile pour les données seules (2 GB)", "unit": "USD", "notes": ""},
  "SC15i10": {"label": "Pourcentage d'abonnements à la téléphonie fixe en zone urbaine", "unit": "Pourcentage", "notes": ""},
  "SC14i13": {"label": "Pourcentage de la population couverte par au moins un réseau mobile 3G", "unit": "Pourcentage", "notes": ""},
  "SC14i14": {"label": "Pourcentage de la population couverte par au moins un réseau mobile 5G", "unit": "Pourcentage", "notes": ""},
  "SC14i15": {"label": "Pourcentage de la population couverte par un réseau mobile cellulaire", "unit": "Pourcentage", "notes": ""},
  "SC15i11": {"label": "Pourcentage des abonnements résidentiels à la téléphonie fixe", "unit": "Pourcentage", "notes": ""},
  "SC13i6": {"label": "Taux d'appels interrompus sur les réseaux mobiles cellulaires", "unit": "Pourcentage", "notes": ""},
  "SC4i3": {"label": "Taux de change annuel moyen par USD", "unit": "Unité monétaire/USD", "notes": ""},
  "SC6i22": {"label": "Taux de disponibilité des centres de données (Data Center Uptime)", "unit": "Pourcentage", "notes": ""},
  "PC1i34": {"label": "Taux de participation des adultes aux programmes de littératie numérique", "unit": "Pourcentage", "notes": ""},
  "PC1i39": {"label": "Taux de pénétration des réseaux sociaux dans la population adulte", "unit": "Pourcentage", "notes": ""},
  "SC13i8": {"label": "Taux d'échec d'appels sur les réseaux mobiles cellulaires", "unit": "Pourcentage", "notes": ""},
  "SC13i9": {"label": "Temps d'activation du service pour le large bande fixe", "unit": "Heures", "notes": ""},
  "SC18i10": {"label": "Trafic international entrant à destination d'un réseau mobile, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i11": {"label": "Trafic Internet large bande fixe, en exaoctets", "unit": "Exaoctets", "notes": ""},
  "SC18i12": {"label": "Trafic Internet large bande mobile à l'intérieur du pays", "unit": "Exaoctets", "notes": ""},
  "SC18i13": {"label": "Trafic mobile sortant à destination de réseaux fixes, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i14": {"label": "Trafic mobile sortant vers d'autres réseaux mobiles, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i15": {"label": "Trafic mobile sortant vers le même réseau mobile, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i16": {"label": "Trafic mobile sortant vers l'international, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i18": {"label": "Trafic sur l'Internet en large bande mobile (hors du pays, itinérance vers l'extérieur)", "unit": "Exaoctets", "notes": ""},
  "SC18i17": {"label": "Trafic sur l'Internet en large bande mobile (hors du pays, itinérance vers l'extérieur avec un accord d'itinérance au niveau du pays), en exaoctets", "unit": "Exaoctets", "notes": ""},
  "SC18i21": {"label": "Trafic téléphonique fixe à fixe national, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i22": {"label": "Trafic téléphonique fixe à mobile, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i23": {"label": "Trafic téléphonique fixe international entrant, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i24": {"label": "Trafic téléphonique fixe international sortant, en minutes", "unit": "Minutes", "notes": ""},
  "SC18i27": {"label": "Trafic téléphonique mobile national, en minutes", "unit": "Minutes", "notes": ""},
  "SC13i1": {"label": "Débit de téléchargement moyen pour le large bande fixe, en bits", "unit": "bit/s", "notes": ""},
  "SC13i2": {"label": "Débit de téléchargement moyen pour le large bande mobile, en bits", "unit": "bit/s", "notes": ""},
  "SC13i3": {"label": "Débit de transfert moyen pour le large bande fixe, en bits", "unit": "bit/s", "notes": ""},
  "SC13i4": {"label": "Débit de transfert moyen pour le large bande mobile, en bits", "unit": "bit/s", "notes": ""},
  "SC7i2": {"label": "Abonnements à la télévision multicanal de Terre", "unit": "Nombre", "notes": ""},
  "SC7i1": {"label": "Abonnements à la télévision multicanal", "unit": "Nombre", "notes": ""},
  "SC7i3": {"label": "Abonnements à la télévision par câble", "unit": "Nombre", "notes": ""},
  "SC7i4": {"label": "Abonnements à la télévision par satellite", "unit": "Nombre", "notes": ""},
  "SC7i5": {"label": "Abonnements à la TVIP", "unit": "Nombre", "notes": ""},
  "SC14i6": {"label": "Abonnements actifs aux réseaux large bande mobiles LTE/WiMAX", "unit": "Nombre", "notes": ""},
  "SC14i7": {"label": "Abonnements au large bande fixe hertzien de Terre", "unit": "Nombre", "notes": ""},
  "SC14i8": {"label": "Abonnements au large bande mobile, données et téléphonie", "unit": "Nombre", "notes": ""},
  "SC15i1": {"label": "Abonnements à la téléphonie fixe", "unit": "Nombre", "notes": ""},
  "SC14i1": {"label": "Abonnements au téléphone mobile cellulaire", "unit": "Nombre", "notes": ""},
  "SC7i6": {"label": "Autres abonnements à la télévision de Terre", "unit": "Nombre", "notes": ""},
  "SC6i11": {"label": "Investissements annuels dans les services de télécommunication", "unit": "USD", "notes": ""},
  "SC14i12": {"label": "Numéros de téléphone mobile cellulaire portés", "unit": "Nombre", "notes": ""},
  "SC6i20": {"label": "Recettes des réseaux mobiles", "unit": "USD", "notes": ""},
  "SC18i5": {"label": "SMS envoyés", "unit": "Nombre", "notes": ""},
  "SC16i8": {"label": "Importations de biens TIC, en pourcentage des importations totales", "unit": "Pourcentage", "notes": ""},
  "SC16i5": {"label": "Exportations de services TIC, en pourcentage des exportations totales de services", "unit": "Pourcentage", "notes": ""},
  "SC14i2": {"label": "Abonnements à des réseaux mobiles M2M", "unit": "Nombre", "notes": ""},
  "SC14i4": {"label": "Abonnements actifs au large bande mobile", "unit": "Nombre", "notes": ""},
  "SC14i5": {"label": "Abonnements actifs au large bande mobile pour 100 habitants", "unit": "Nombre/100 habitants", "notes": ""},
  "SC9i2": {"label": "Abonnements au large bande fixe", "unit": "Nombre", "notes": ""},
  "SC15i2": {"label": "Abonnements au protocole VoIP", "unit": "Nombre", "notes": ""},
  "SC6i7": {"label": "Investissement extérieur annuel dans les télécommunications", "unit": "USD", "notes": ""},
  "SC18i1": {"label": "Itinérance hors réseau d'origine (itinérance sortante avec un accord d'itinérance au niveau du pays), en minutes", "unit": "Minutes", "notes": ""},
  "SC18i2": {"label": "Itinérance hors réseau d'origine (itinérance sortante), en minutes", "unit": "Minutes", "notes": ""},
  "SC3i13": {"label": "Niveau de sécurisation des infrastructures télécoms contre les cyberattaques", "unit": "Niveau", "notes": ""},
  "PC1i7": {"label": "Obstacles à l'accès des ménages à l'Internet", "unit": "Type", "notes": ""},
  "SC3i16": {"label": "Nombre d'attaques menées par des acteurs étatiques étrangers", "unit": "Nombre", "notes": ""},
  "SC3i17": {"label": "Nombre d'attaques par déni de service distribué (DDoS)", "unit": "Nombre", "notes": ""},
  "SC3i18": {"label": "Nombre d'attaques réussies sur les infrastructures critiques", "unit": "Nombre", "notes": ""},
  "PC1i3": {"label": "Dépenses des ménages au titre des TIC", "unit": "USD", "notes": ""},
  "SC3i7": {"label": "Existence de formations en cybersécurité dans les institutions éducatives", "unit": "Oui/Non", "notes": ""},
  "SC16i7": {"label": "Importations de biens TIC, en pourcentage des exportations totales", "unit": "Pourcentage", "notes": ""},
  "SC3i25": {"label": "Nombre de centres de sécurité opérationnelle (SOC)", "unit": "Nombre", "notes": ""},
  "SC3i24": {"label": "Nombre de campagnes de sensibilisation à la cybersécurité", "unit": "Nombre", "notes": ""},
  "SC7i8": {"label": "Taux de pénétration des services de télévision par câble et satellite", "unit": "Pourcentage", "notes": ""},
  "SC3i12": {"label": "Investissement dans les technologies de détection et de réponse aux cybermenaces", "unit": "USD", "notes": ""},
  "PC10i1": {"label": "Nombre d’utilisateurs de services de banque en ligne et de paiement mobile", "unit": "Nombre", "notes": ""},
  "SC3i27": {"label": "Nombre de cyberattaques ciblées", "unit": "Nombre", "notes": ""},
  "PC1i5": {"label": "Nombre d'heures de contenu numérique consommé par habitant (streaming, médias sociaux, jeux)", "unit": "Heures", "notes": ""},
  "SC5i3": {"label": "Années moyennes de scolarité", "unit": "Années", "notes": ""},
  "SC3i30": {"label": "Nombre de cyberattaques du secteur santé", "unit": "Nombre", "notes": ""},
  "SC2i3": {"label": "Nombre de documents administratifs émis sous format électronique", "unit": "Nombre", "notes": ""},
  "SC2i4": {"label": "Nombre de partenariats public-privé dans le développement de services publics numériques", "unit": "Nombre", "notes": ""},
  "SC18i4": {"label": "Nombre de pays avec lesquels il existe un accord d'itinérance au niveau du pays", "unit": "Nombre", "notes": ""},
  "SC18i3": {"label": "Nombre de pays avec lesquels il existe un accord d'itinérance au niveau de l'opérateur", "unit": "Nombre", "notes": ""},
  "SC2i5": {"label": "Nombre de plaintes ou d’incidents de sécurité liés à l’utilisation des services publics numériques", "unit": "Nombre", "notes": ""},
  "SC2i6": {"label": "Nombre de plateformes numériques interconnectées entre les différentes administrations", "unit": "Nombre", "notes": ""},
  "SC2i7": {"label": "Nombre de portails gouvernementaux proposant des services en ligne", "unit": "Nombre", "notes": ""},
  "SC2i8": {"label": "Nombre de projets gouvernementaux utilisant l'intelligence artificielle pour améliorer les services", "unit": "Nombre", "notes": ""},
  "SC10i6": {"label": "Nombre d'opération de retraits, par jour, mois, et par année", "unit": "Nombre", "notes": ""},
  "SC3i32": {"label": "Nombre de cyberattaques sur les systèmes de santé", "unit": "Nombre", "notes": ""},
  "SC3i31": {"label": "Nombre de cyberattaques sur les institutions financières", "unit": "Nombre", "notes": ""},
  "PC1i4": {"label": "Nombre d’utilisateurs de plateformes de télétravail et d’enseignement à distance", "unit": "Nombre", "notes": ""},
  "SC2i9": {"label": "Nombre d'institutions publiques intégrant les technologies cloud dans leurs opérations", "unit": "Nombre", "notes": ""},
  "SC10i7": {"label": "Nombre total d'opérations P2P, par jour, mois, et par année", "unit": "Nombre", "notes": ""},
  "SC2i10": {"label": "Nombre d'utilisateurs actifs des services publics en ligne", "unit": "Nombre", "notes": ""},
  "SC14i10": {"label": "Abonnements au large bande mobile, données uniquement", "unit": "Nombre", "notes": ""},
  "SC9i15": {"label": "Taux de pénétration de la fibre optique", "unit": "Pourcentage", "notes": ""},
  "SC2i20": {"label": "Taux de couverture géographique des services publics en ligne", "unit": "Pourcentage", "notes": ""},
  "PC1i8": {"label": "Pourcentage de la population ayant effectué des démarches administratives en ligne au cours de l'année", "unit": "Pourcentage", "notes": ""},
  "SC14i16": {"label": "Pourcentage de la population desservie par au moins un réseau mobile LTE/WiMAX", "unit": "Pourcentage", "notes": ""},
  "SC9i17": {"label": "Taux de raccordement à la fibre des ménages et des entreprises", "unit": "Pourcentage", "notes": ""},
  "SC2i23": {"label": "Nombre de formation sur le digital", "unit": "Nombre", "notes": ""},
  "SC2i19": {"label": "Volume de transaction enregistrés par les services publics gérés par l'ATD", "unit": "Nombre", "notes": ""},
  "SC6i21": {"label": "Taux de défaillance des infrastructures de télécommunications", "unit": "Pourcentage", "notes": ""},
  "SC6i24": {"label": "Taux de modernisation des infrastructures réseau pour supporter les nouvelles technologies", "unit": "Pourcentage", "notes": ""},
  "SC6i23": {"label": "Taux de disponibilité des services d'urgence via télécommunications (112, 911, etc.)", "unit": "Pourcentage", "notes": ""},
  "SC17i5": {"label": "Proportion d’écoles disposant d’un poste de radio utilisé à des fins d’enseignement", "unit": "Pourcentage", "notes": ""},
  "SC17i6": {"label": "Proportion d’écoles disposant d’un poste de télévision utilisé à des fins d’enseignement", "unit": "Pourcentage", "notes": ""},
  "SC17i8": {"label": "Proportion d’étudiants inscrits, poursuivant des études postsecondaires dans des domaines liés aux TIC", "unit": "Pourcentage", "notes": ""},
  "PC10i2": {"label": "Taux de pénétration au mobile money", "unit": "Pourcentage", "notes": ""},
  "PC1i10": {"label": "Pourcentage de la population utilisant des applications gouvernementales mobiles", "unit": "Pourcentage", "notes": ""},
  "PC1i9": {"label": "Pourcentage de la population ayant un compte sur une plateforme d'identification numérique", "unit": "Pourcentage", "notes": ""},
  "PC1i32": {"label": "Taux d’utilisation des applications de gestion des finances personnelles (fintech)", "unit": "Pourcentage", "notes": ""},
  "PC1i30": {"label": "Taux d’adoption des solutions de mobilité numérique (transport intelligent, covoiturage, etc.)", "unit": "Pourcentage", "notes": ""},
  "PC1i37": {"label": "Taux de pénétration des outils d’apprentissage en ligne (MOOC, plateformes d’éducation)", "unit": "Pourcentage", "notes": ""},
  "PC1i38": {"label": "Taux de pénétration des plateformes numériques de consultation médicale", "unit": "Pourcentage", "notes": ""},
  "PC1i26": {"label": "Proportion de ménages ayant accès à l'Internet, par type de service", "unit": "Pourcentage", "notes": ""},
  "PC1i11": {"label": "Pourcentage de ménages disposant d'un accès à Internet", "unit": "Pourcentage", "notes": ""},
  "PC1i12": {"label": "Pourcentage de ménages disposant d'un ordinateur", "unit": "Pourcentage", "notes": ""},
  "PC1i14": {"label": "Pourcentage de ménages disposant d'une radio", "unit": "Pourcentage", "notes": ""},
  "PC1i15": {"label": "Pourcentage de ménages disposant d'une Télévision", "unit": "Pourcentage", "notes": ""},
  "PC1i13": {"label": "Pourcentage de ménages disposant d'un téléphone", "unit": "Pourcentage", "notes": ""},
  "PC1i16": {"label": "Pourcentage de ménages disposant d'une télévision multicanal, par type de service", "unit": "Pourcentage", "notes": ""},
  "PC1i40": {"label": "Taux d'utilisation de l'internet des objets (IoT) au sein des ménages (domotique, objets connectés)", "unit": "Pourcentage", "notes": ""},
  "PC1i25": {"label": "Pourcentage de personnes utilisant un téléphone mobile cellulaire", "unit": "Pourcentage", "notes": ""},
  "PC1i20": {"label": "Pourcentage de particuliers utilisant l'internet, par lieu d'utilisation", "unit": "Pourcentage", "notes": ""},
  "PC1i23": {"label": "Pourcentage de personnes utilisant l'internet", "unit": "Pourcentage", "notes": ""},
  "PC1i24": {"label": "Pourcentage de personnes utilisant un ordinateur", "unit": "Pourcentage", "notes": ""},
  "PC1i35": {"label": "Taux de participation des citoyens aux formations en compétences numériques", "unit": "Pourcentage", "notes": ""},
  "PC1i41": {"label": "Taux d'utilisation des smartphones pour accéder aux services numériques", "unit": "Pourcentage", "notes": ""},
  "SC6i2": {"label": "Capacité total de bande passante des câbles sous-marins", "unit": "Mbit/s", "notes": ""},
  "SC6i1": {"label": "Personnes employées par tous les opérateurs de télécommunications, femmes", "unit": "Nombre", "notes": ""},
  "PC1i31": {"label": "Taux d’adoption du e-commerce parmi les utilisateurs d’Internet", "unit": "Pourcentage", "notes": ""},
  "PC1i33": {"label": "Taux d'adoption des technologies de stockage en ligne (cloud ou drive) par les ménages", "unit": "Pourcentage", "notes": ""},
  "SC13i10": {"label": "Temps de latence des paquets pour le large bande fixe, en millisecondes", "unit": "Millisecondes", "notes": ""},
  "SC13i11": {"label": "Temps de latence des paquets pour le large bande mobile, en millisecondes", "unit": "Millisecondes", "notes": ""},
  "SC13i12": {"label": "Temps moyen de réparation ou de rétablissement des services en cas de panne", "unit": "Heures", "notes": ""},
  "SC9i14": {"label": "Nombre de points d'accès Wi-Fi public", "unit": "Nombre", "notes": ""},
  "SC9i13": {"label": "Nombre de fournisseurs d'accès à internet (FAI)", "unit": "Nombre", "notes": ""},
  "SC6i17": {"label": "Nombre de tours de télécommunication et d'antennes relais", "unit": "Nombre", "notes": ""},
  "SC18i19": {"label": "Trafic téléphonique fixe à fixe interurbain, en minutes", "unit": "Minutes", "notes": ""},
  "SC9i19": {"label": "Utilisation de la largeur de bande internationale, en Mbit/s", "unit": "Mbit/s", "notes": ""},
  "SC2i22": {"label": "Nombre de services publics digitalisés via USSD", "unit": "Nombre", "notes": ""},
  "SC2i25": {"label": "Taux de paiement d'impôt en utilisant les plateformes numériques", "unit": "Pourcentage", "notes": ""},
  "SC2i29": {"label": "Taux de satisfaction des utilisateurs des services publics des plateformes gérées par l'ATD", "unit": "Pourcentage", "notes": ""},
  "SC3i45": {"label": "Nombre d'entreprises certifiées ISO/IEC 27001", "unit": "Nombre", "notes": ""},
  "SC3i46": {"label": "Nombre d'entreprises victimes d'une violation de données", "unit": "Nombre", "notes": ""},
  "SC3i48": {"label": "Nombre d'incidents de compromission des systèmes bancaires", "unit": "Nombre", "notes": ""},
  "SC3i49": {"label": "Nombre d'incidents de cyberespionnage", "unit": "Nombre", "notes": ""},
  "SC3i50": {"label": "Nombre d'incidents de fraude en ligne", "unit": "Nombre", "notes": ""},
  "SC3i51": {"label": "Nombre d'incidents de perte de données sensibles", "unit": "Nombre", "notes": ""},
  "SC3i56": {"label": "Pourcentage de réseaux gouvernementaux utilisant des technologies de sécurité modernes", "unit": "Pourcentage", "notes": ""},
  "SC3i57": {"label": "Pourcentage de systèmes avec des vulnérabilités connues non corrigées", "unit": "Pourcentage", "notes": ""},
  "SC3i58": {"label": "Prévalence des ransomwares", "unit": "Nombre", "notes": ""},
  "SC3i67": {"label": "Taux de détection des vulnérabilités dans les infrastructures critiques", "unit": "Pourcentage", "notes": ""},
  "SC3i68": {"label": "Taux de mise en œuvre des recommandations de cybersécurité par les entreprises", "unit": "Pourcentage", "notes": ""},
  "SC3i69": {"label": "Taux de participation des entreprises aux simulations d'incidents de cybersécurité", "unit": "Pourcentage", "notes": ""},
  "SC3i73": {"label": "Taux d'utilisation des technologies de surveillance et de détection des intrusions", "unit": "Pourcentage", "notes": ""},
  "SC3i74": {"label": "Temps de réponse moyen aux incidents de cybersécurité", "unit": "Heures", "notes": ""},
  "PC3i1": {"label": "Temps moyen pour détecter une violation de données", "unit": "Heures", "notes": ""},
  "PC3i2": {"label": "Temps moyen pour répondre à une violation de données", "unit": "Heures", "notes": ""},
  "SC5i1": {"label": "Alphabétisation des adultes", "unit": "Pourcentage", "notes": ""},
  "SC5i2": {"label": "Années de scolarité attendues", "unit": "Années", "notes": ""},
  "SC6i25": {"label": "Capacité utilisée de bande passante des câbles sous-marins", "unit": "Mbit/s", "notes": ""},
  "SC6i26": {"label": "Chiffre d’affaires du secteur", "unit": "Milliards de FCFA", "notes": ""},
  "SC9i22": {"label": "Taux de pénétration FTTH par rapport à la population", "unit": "Pourcentage", "notes": ""},
  "SC9i23": {"label": "Taux de pénétration FTTH par rapport au ménage", "unit": "Pourcentage", "notes": ""},
  "SC9i24": {"label": "Taux de pénétration data fixe et mobile", "unit": "Pourcentage", "notes": ""},
  "SC9i25": {"label": "Taux de pénétration data haut débit fixe et mobile", "unit": "Pourcentage", "notes": ""},
  "SC10i8": {"label": "Taux de pénétration mobile money", "unit": "Pourcentage", "notes": ""},
  "SC13i13": {"label": "Délai d'établissement d'appel", "unit": "Secondes", "notes": ""},
  "SC13i14": {"label": "Taux de succès d'appels", "unit": "Pourcentage", "notes": ""},
  "SC13i15": {"label": "Qualité vocale", "unit": "Score MOS", "notes": ""},
  "SC13i16": {"label": "Taux de coupure d'appel", "unit": "Pourcentage", "notes": ""},
  "SC13i17": {"label": "Taux d'échec de téléchargement de page web", "unit": "Pourcentage", "notes": ""},
  "SC13i18": {"label": "Délai de téléchargement d'une page web", "unit": "Secondes", "notes": ""},
  "SC13i19": {"label": "Débit montant uplink", "unit": "bit/s", "notes": ""},
  "SC13i20": {"label": "Débit descendant downlink", "unit": "bit/s", "notes": ""},
  "SC13i21": {"label": "Taux de succès de transfert montant uplink", "unit": "Pourcentage", "notes": ""},
  "SC13i22": {"label": "Taux de succès de transfert descendant downlink", "unit": "Pourcentage", "notes": ""},
  "SC13i23": {"label": "Taux de succès de requêtes USSD", "unit": "Pourcentage", "notes": ""},
  "SC13i24": {"label": "Taux de succès de réception de SMS", "unit": "Pourcentage", "notes": ""},
  "SC13i25": {"label": "Nombre d’indisponibilité d'une station de base", "unit": "Nombre", "notes": ""},
  "SC13i26": {"label": "Délai d’indisponibilité d'une station de base", "unit": "Heures", "notes": ""},
  "SC13i27": {"label": "Taux de rétablissement de cartes SIM en 24 heures", "unit": "Pourcentage", "notes": ""},
  "SC13i28": {"label": "Taux de traitement des réclamations en moins de 7 jours", "unit": "Pourcentage", "notes": ""},
  "SC14i25": {"label": "Taux de pénétration à la téléphonie mobile", "unit": "Pourcentage", "notes": ""},
  "SC14i26": {"label": "Taux de pénétration data mobile", "unit": "Pourcentage", "notes": ""},
  "SC14i27": {"label": "Taux de pénétration data mobile haut débit", "unit": "Pourcentage", "notes": ""},
  "SC14i28": {"label": "Taux de pénétration à la téléphonie fixe et mobile", "unit": "Pourcentage", "notes": ""},
  "SC14i29": {"label": "Pourcentage de la population couverte par au moins un réseau mobile 2G", "unit": "Pourcentage", "notes": ""},
  "SC14i30": {"label": "Pourcentage de la population couverte par au moins un réseau mobile 4G", "unit": "Pourcentage", "notes": ""},
  "SC15i12": {"label": "Taux de pénétration à la téléphonie fixe", "unit": "Pourcentage", "notes": ""},
  "SC15i13": {"label": "Taux de pénétration data fixe par rapport à la population", "unit": "Pourcentage", "notes": ""},
  "SC15i14": {"label": "Taux de pénétration data fixe par rapport au ménage", "unit": "Pourcentage", "notes": ""},
  "PC3i75": {"label": "Nombre de cyberattaques du secteur finance", "unit": "Nombre", "notes": ""},
  "PC3i76": {"label": "Nombre de cyberattaques du secteur énergie", "unit": "Nombre", "notes": ""},
  "PC3i77": {"label": "Nombre de cyberattaques du secteur commerce", "unit": "Nombre", "notes": ""},
  "PC3i3": {"label": "Proportion d'entreprises respectant les règles nationales de cybersécurité", "unit": "Pourcentage", "notes": ""},
  "PC3i4": {"label": "Proportion d'entreprises victimes d'une violation de données", "unit": "Pourcentage", "notes": ""},
  "PC3i5": {"label": "Proportion d'entreprises victimes de ransomwares", "unit": "Pourcentage", "notes": ""},
  "PC3i6": {"label": "Proportion d'entreprises victimes de violations de la vie privée liées à des cyberattaques signalées", "unit": "Pourcentage", "notes": ""},
  "SC6i27": {"label": "Nombre total de startups labellisées par l'ATD", "unit": "Nombre", "notes": ""},
  "SC6i28": {"label": "Nombre total de startups accompagnées par le MENTD", "unit": "Nombre", "notes": ""},
  "SC2i33": {"label": "Nombre d’incidents de sécurité remontés depuis les plateformes gérées par l'ATD utilisées par la population", "unit": "Nombre", "notes": ""},
  "SC2i35": {"label": "Nombre de plaintes remontées depuis la plateforme gérée par l'ATD utilisées par la population", "unit": "Nombre", "notes": ""},
  "SC2i36": {"label": "Nombre d'entreprises dans le secteur TIC créées au cours des 12 derniers mois", "unit": "Nombre", "notes": ""},
  "SC2i37": {"label": "Nombre d'entreprises dans le secteur TIC de moins de trois ans", "unit": "Nombre", "notes": ""},
  "SC2i38": {"label": "Nombre d'entreprises dans le secteur TIC de plus de trois ans", "unit": "Nombre", "notes": ""},
  "SC17i10": {"label": "Proportion d’écoles privées ayant l’électricité", "unit": "Pourcentage", "notes": ""},
  "SC17i11": {"label": "Proportion d’écoles publiques ayant l’électricité", "unit": "Pourcentage", "notes": ""}
}

subindicators = {
}
# Étape 1 : Convertir Excel en CSV
def excel_to_csv(excel_data, separator="|"):
    """
    Convertit un fichier Excel en fichiers CSV (un par feuille).
    
    Args:
        excel_data (bytes): Données binaires du fichier Excel (lues depuis STDIN).
        separator (str): Séparateur à utiliser dans les fichiers CSV (par défaut : "|").
    
    Returns:
        dict: Dictionnaire où les clés sont les noms des feuilles et les valeurs sont les contenus CSV.
    """
    try:
        logging.info("Reading Excel data from STDIN")
        xl = pd.ExcelFile(io.BytesIO(excel_data))
        logging.info(f"Available sheets: {xl.sheet_names}")
        
        csv_contents = {}
        
        for sheet_name in xl.sheet_names:
            try:
                df = pd.read_excel(xl, sheet_name=sheet_name, header=None)
            except Exception as e:
                logging.error(f"Failed to read sheet {sheet_name}: {e}")
                continue
            
            if df.empty:
                logging.warning(f"Sheet {sheet_name} is empty, skipping")
                continue
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, sep=separator, index=False, header=False, na_rep="")
            csv_content = csv_buffer.getvalue()
            csv_contents[sheet_name] = csv_content  # Garder le nom complet de la feuille (e.g., "C5-2017")
            
            logging.info(f"Converted sheet {sheet_name} to CSV with separator '{separator}'")
        
        if not csv_contents:
            logging.error("No sheets were converted to CSV")
            raise ValueError("No sheets were converted to CSV")
        
        return csv_contents
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

def save_csv_to_hdfs(csv_contents, hdfs_base_path):
    """
    Enregistre les fichiers CSV dans HDFS via WebHDFS avec la structure csv_output/<category>/<year>.csv.
    
    Args:
        csv_contents (dict): Dictionnaire contenant les contenus CSV (clé : nom de la feuille, valeur : contenu CSV).
        hdfs_base_path (str): Chemin de base dans HDFS où enregistrer les fichiers CSV.
    
    Returns:
        dict: Dictionnaire des chemins HDFS des fichiers sauvegardés (clé : nom de la feuille, valeur : chemin HDFS).
    """
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        csv_output_base = os.path.join(hdfs_base_path, "csv_output")
        client.makedirs(csv_output_base)
        logging.info(f"Created base directory in HDFS: {csv_output_base}")

        saved_files = {}

        for sheet_name, content in csv_contents.items():
            # Extraire la catégorie et l'année à partir du nom de la feuille (e.g., "C5-2017")
            parts = sheet_name.split('-')
            if len(parts) != 2:
                logging.warning(f"Invalid sheet name format {sheet_name}, skipping")
                continue

            category = parts[0].strip()  # e.g., "C5"
            year = parts[1].strip()      # e.g., "2017"

            # Créer le répertoire de la catégorie
            category_dir = os.path.join(csv_output_base, category)
            client.makedirs(category_dir)
            logging.info(f"Created category directory in HDFS: {category_dir}")

            # Nom du fichier CSV et chemin HDFS
            csv_filename = f"{year}.csv"  # e.g., "2017.csv"
            csv_hdfs_path = os.path.join(category_dir, csv_filename)
            
            # Écrire le contenu CSV dans un fichier temporaire local
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as tmp_output:
                tmp_output_path = tmp_output.name
                tmp_output.write(content)
            
            # Téléverser le fichier temporaire vers HDFS
            client.upload(csv_hdfs_path, tmp_output_path, overwrite=True)
            logging.info(f"Saved CSV to HDFS: {csv_hdfs_path}")
            os.remove(tmp_output_path)

            saved_files[sheet_name] = csv_hdfs_path
        
        return saved_files
    
    except Exception as e:
        logging.error(f"Failed to save CSV to HDFS via WebHDFS: {e}")
        raise

# Étape 2 : Traiter les CSV pour initialiser le big_json
def process_csvs_and_init_json(hdfs_base_dir, separator="|"):
    """
    Process saved CSV files from HDFS, validate category code, and initialize the big JSON object.
    
    Args:
        hdfs_base_dir (str): HDFS directory where CSV files are saved (base path + csv_output).
        separator (str): Separator used in the CSV files.
    
    Returns:
        dict: The initialized big JSON object with categories.
    """
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        csv_output_dir = os.path.join(hdfs_base_dir, "csv_output")
        
        try:
            client.status(csv_output_dir)
        except Exception as e:
            logging.error(f"HDFS base directory {csv_output_dir} does not exist: {e}")
            raise FileNotFoundError(f"HDFS base directory {csv_output_dir} does not exist")
        
        big_json = {
            "categories": {}
        }
        
        # Lister les répertoires de catégories (e.g., C5, C17)
        category_dirs = client.list(csv_output_dir)
        
        for category_dir in category_dirs:
            category_path = os.path.join(csv_output_dir, category_dir)
            if not client.status(category_path)['type'] == 'DIRECTORY':
                logging.warning(f"Skipping non-directory item: {category_dir}")
                continue
            
            category_code = category_dir  # e.g., "C5"
            if not category_code.startswith("C"):
                logging.warning(f"Category code {category_code} does not start with 'C', skipping")
                continue
            
            # Lister les fichiers CSV dans le répertoire de la catégorie
            csv_files = client.list(category_path)
            if not csv_files:
                logging.warning(f"No CSV files found in category directory {category_path}, skipping")
                continue
            
            # Prendre le premier fichier CSV pour extraire les métadonnées (label, etc.)
            first_csv_file = csv_files[0]
            csv_hdfs_path = os.path.join(category_path, first_csv_file)
            
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
                tmp_file_path = tmp_file.name
                client.download(csv_hdfs_path, tmp_file_path, overwrite=True)
                
                try:
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                except Exception as e:
                    logging.error(f"Failed to read CSV file {csv_hdfs_path}: {e}")
                    os.remove(tmp_file_path)
                    continue
                
                cleaned_lines = [line.strip() for line in lines if line.strip()]
                if not cleaned_lines:
                    logging.error(f"CSV file {csv_hdfs_path} is empty after cleaning")
                    os.remove(tmp_file_path)
                    continue
                
                logging.info(f"Cleaned CSV file {csv_hdfs_path}: {len(cleaned_lines)} lines after removing empty lines")
                
                first_line = cleaned_lines[0]
                logging.info(f"First line of {first_csv_file}: {first_line}")
                
                columns = first_line.split(separator)
                cleaned_columns = [col for col in columns if "unnamed" not in col.lower()]
                logging.info(f"Columns after removing 'unnamed': {cleaned_columns}")
                
                if not cleaned_columns:
                    logging.error(f"No non-unnamed columns found in first line of {first_csv_file}")
                    os.remove(tmp_file_path)
                    continue
                
                first_non_unnamed = cleaned_columns[0]
                if first_non_unnamed.upper() != category_code.upper():
                    logging.warning(f"Category code mismatch in {first_csv_file}: expected {category_code}, found {first_non_unnamed}")
                    os.remove(tmp_file_path)
                    continue
                
                logging.info(f"Category code validation passed for {first_csv_file}: {first_non_unnamed} matches {category_code}")
                
                category_label = cleaned_columns[1] if len(cleaned_columns) > 1 else category_code
                
                big_json["categories"][category_code] = {
                    "level": "category",
                    "code": category_code,
                    "label": category_label,
                    "indicators": {},
                    "disaggregation_levels": {
                        "Région": ["Region1", "Region2"],  # À remplacer par vos régions réelles
                        "Sexe": ["Homme", "Femme"]        # À remplacer par vos désagrégations réelles
                    }
                }
                
                os.remove(tmp_file_path)
        
        if not big_json["categories"]:
            logging.error("No categories were processed")
            raise ValueError("No categories were processed")
        
        return big_json
    
    except Exception as e:
        logging.error(f"Failed to process CSV files: {e}")
        raise

# Étape 3 : Mettre à jour les labels des catégories
def fix_labels(big_json):
    """
    Fix category labels in the big JSON using the categories dictionary.
    
    Args:
        big_json (dict): The big JSON object to update.
    
    Returns:
        dict: The updated big JSON object.
    """
    try:
        for category_code, category_data in big_json["categories"].items():
            if category_code in categories:
                category_data["label"] = categories[category_code]
                logging.info(f"Updated label for category {category_code}: {categories[category_code]}")
            else:
                logging.warning(f"Category code {category_code} not found in categories dictionary, leaving label as is")
        
        return big_json
    
    except Exception as e:
        logging.error(f"Failed to fix labels: {e}")
        raise

# Étape 4 : Traiter les sources et les données pour ajouter les indicateurs
def normalize_key(text):
    """Normalize a string to be used as a JSON key (lowercase, no special characters)."""
    text = text.lower()
    replacements = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'à': 'a', 'â': 'a', 'ä': 'a',
        'î': 'i', 'ï': 'i',
        'ô': 'o', 'ö': 'o',
        'û': 'u', 'ü': 'u',
        'ç': 'c',
        ' ': ''
    }
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    text = re.sub(r'[^a-z0-9]', '', text)
    return text

def process_sources_and_data(big_json, hdfs_base_dir, separator="|"):
    """
    Process source lines and data rows in each CSV to extract indicators and their data.
    
    Args:
        big_json (dict): The big JSON object to update.
        hdfs_base_dir (str): HDFS directory where CSV files are saved (base path + csv_output).
        separator (str): Separator used in the CSV files.
    
    Returns:
        dict: Updated big_json with indicators.
    """
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        csv_output_dir = os.path.join(hdfs_base_dir, "csv_output")
        
        try:
            client.status(csv_output_dir)
        except Exception as e:
            logging.error(f"HDFS base directory {csv_output_dir} does not exist: {e}")
            raise FileNotFoundError(f"HDFS base directory {csv_output_dir} does not exist")
        
        source_markers = [
            "Données ITU",
            "Données CNUCED",
            "Données autres Organismes (GSMA, UNESCO, etc.)",
            "Données exclusivement nationales"
        ]
        
        # Lister les répertoires de catégories
        category_dirs = client.list(csv_output_dir)
        
        for category_dir in category_dirs:
            category_path = os.path.join(csv_output_dir, category_dir)
            if not client.status(category_path)['type'] == 'DIRECTORY':
                logging.warning(f"Skipping non-directory item: {category_dir}")
                continue
            
            category_code = category_dir  # e.g., "C5"
            if category_code not in big_json["categories"]:
                logging.warning(f"Category {category_code} not found in big_json, skipping")
                continue
            
            logging.info(f"Processing category folder: {category_dir}")
            
            # Lister les fichiers CSV dans le répertoire de la catégorie
            csv_files = client.list(category_path)
            
            for csv_file in csv_files:
                if not csv_file.endswith(".csv"):
                    logging.warning(f"Skipping non-CSV file: {csv_file}")
                    continue
                
                csv_hdfs_path = os.path.join(category_path, csv_file)
                logging.info(f"Processing CSV file: {csv_hdfs_path}")
                
                year = os.path.splitext(csv_file)[0]  # e.g., "2017" from "2017.csv"
                
                with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
                    tmp_file_path = tmp_file.name
                    client.download(csv_hdfs_path, tmp_file_path, overwrite=True)
                    
                    try:
                        with open(tmp_file_path, "r", encoding="utf-8") as f:
                            lines = f.readlines()
                    except Exception as e:
                        logging.error(f"Failed to read CSV file {csv_hdfs_path}: {e}")
                        os.remove(tmp_file_path)
                        continue
                    
                    if len(lines) < 4:
                        logging.error(f"CSV file {csv_hdfs_path} has fewer than 4 lines")
                        os.remove(tmp_file_path)
                        continue
                    
                    line_3 = lines[2].strip().split(separator)
                    line_4 = lines[3].strip().split(separator)
                    columns = []
                    for i in range(len(line_3)):
                        main_header = line_3[i].strip()
                        sub_header = line_4[i].strip()
                        if sub_header:
                            columns.append(sub_header)
                        else:
                            columns.append(main_header)
                    
                    # Vérifier les colonnes requises
                    required_columns = ["Code TG", "Code SI TG", "Définition", "intitulé de l'indicateur (FR)", "Unité de mesure", "Notes", "Valeur Générale"]
                    missing_columns = [col for col in required_columns if col not in columns]
                    if missing_columns:
                        logging.error(f"Missing required columns in {csv_file}: {missing_columns}")
                        os.remove(tmp_file_path)
                        continue
                    
                    code_tg_index = columns.index("Code TG")
                    code_si_tg_index = columns.index("Code SI TG")
                    definition_index = columns.index("Définition")
                    intitule_index = columns.index("intitulé de l'indicateur (FR)")
                    unite_index = columns.index("Unité de mesure")
                    notes_index = columns.index("Notes")  # Corrigé : "Notes" au lieu de "Note"
                    valeur_generale_index = columns.index("Valeur Générale")
                    
                    current_source = None
                    for line_idx, line in enumerate(lines[4:], start=4):
                        line = line.strip()
                        if not line:
                            continue
                        
                        row = line.split(separator)
                        
                        if any(marker in line for marker in source_markers):
                            current_source = line.strip()
                            logging.info(f"Found source at line {line_idx + 1}: {current_source}")
                            continue
                        
                        if not current_source:
                            continue
                        
                        code_tg = row[code_tg_index].strip() if code_tg_index < len(row) else ""
                        code_si_tg = row[code_si_tg_index].strip() if code_si_tg_index < len(row) else ""
                        definition = row[definition_index].strip() if definition_index < len(row) else ""
                        intitule = row[intitule_index].strip() if intitule_index < len(row) else ""
                        unite = row[unite_index].strip() if unite_index < len(row) else ""
                        notes = row[notes_index].strip() if notes_index < len(row) else ""
                        valeur_generale = row[valeur_generale_index].strip() if valeur_generale_index < len(row) else ""
                        
                        if not code_tg:
                            logging.warning(f"Skipping row at line {line_idx + 1} in {csv_file}: Missing Code TG")
                            continue
                        
                        indicator_key = code_tg
                        sub_indicator_key = code_si_tg if code_si_tg else "NULL"
                        sub_indicator_code = f"{indicator_key}_{sub_indicator_key}" if code_si_tg else f"{indicator_key}_NULL"
                        
                        if indicator_key not in big_json["categories"][category_code]["indicators"]:
                            big_json["categories"][category_code]["indicators"][indicator_key] = {
                                "level": "indicator",
                                "code": indicator_key,
                                "label": intitule,
                                "notes": notes,
                                "subindicators": {}
                            }
                        
                        if sub_indicator_key not in big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"]:
                            big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key] = {
                                "level": "subindicator",
                                "code": sub_indicator_code,
                                "label": intitule,
                                "notes": notes,
                                "years": {}
                            }
                        
                        year_key = year
                        year_code = f"{sub_indicator_code}_{year}"
                        if year_key not in big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"]:
                            big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"][year_key] = {
                                "level": "year",
                                "code": year_code,
                                "notes": current_source,
                                "value": {
                                    "level": "region",
                                    "code": f"{year_code}_value",
                                    "value": valeur_generale,
                                    "unit": unite,
                                    "notes": ""
                                },
                                "regions": {
                                    "level": "region",
                                    "code": "region",
                                    "values": {}
                                },
                                "desagregations": {}
                            }
                        
                        year_data = big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"][year_key]
                        
                        regions = big_json["categories"][category_code]["disaggregation_levels"].get("Région", [])
                        for region in regions:
                            normalized_region = normalize_key(region)
                            try:
                                col_index = columns.index(region)
                                value = row[col_index].strip() if col_index < len(row) else ""
                                year_data["regions"]["values"][normalized_region] = {
                                    "code": f"{year_code}_{normalized_region}",
                                    "value": value,
                                    "unit": unite,
                                    "notes": ""
                                }
                            except ValueError:
                                year_data["regions"]["values"][normalized_region] = {
                                    "code": f"{year_code}_{normalized_region}",
                                    "value": "",
                                    "unit": unite,
                                    "notes": ""
                                }
                        
                        for level, subcategories in big_json["categories"][category_code]["disaggregation_levels"].items():
                            if level == "Région":
                                continue
                            normalized_level = normalize_key(level)
                            year_data["desagregations"][normalized_level] = {
                                "level": "desagregation",
                                "code": normalized_level,
                                "values": {}
                            }
                            for subcategory in subcategories:
                                normalized_subcategory = normalize_key(subcategory)
                                try:
                                    col_index = columns.index(subcategory)
                                    value = row[col_index].strip() if col_index < len(row) else ""
                                    year_data["desagregations"][normalized_level]["values"][normalized_subcategory] = {
                                        "code": f"{year_code}_{normalized_subcategory}",
                                        "value": value,
                                        "unit": unite,
                                        "notes": ""
                                    }
                                except ValueError:
                                    year_data["desagregations"][normalized_level]["values"][normalized_subcategory] = {
                                        "code": f"{year_code}_{normalized_subcategory}",
                                        "value": "",
                                        "unit": unite,
                                        "notes": ""
                                    }
                    
                    os.remove(tmp_file_path)
        
        return big_json
    
    except Exception as e:
        logging.error(f"Failed to process sources and data: {e}")
        raise

# Étape 5 : Sauvegarder le JSON final sur HDFS
def save_final_json(big_json, hdfs_output_file):
    """
    Save the final big JSON to HDFS.
    
    Args:
        big_json (dict): The big JSON object to save.
        hdfs_output_file (str): HDFS path to save the JSON file.
    """
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        # Créer le répertoire parent si nécessaire
        parent_dir = os.path.dirname(hdfs_output_file)
        client.makedirs(parent_dir)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as tmp_output:
            tmp_output_path = tmp_output.name
            json.dump(big_json, tmp_output, ensure_ascii=False, indent=4)
        
        client.upload(hdfs_output_file, tmp_output_path, overwrite=True)
        logging.info(f"Saved final JSON to HDFS: {hdfs_output_file}")
        os.remove(tmp_output_path)
    
    except Exception as e:
        logging.error(f"Failed to save final JSON to HDFS: {e}")
        raise

# Point d'entrée du script
if __name__ == "__main__":
    try:
        # Lire les données binaires (fichier Excel) depuis STDIN
        excel_data = sys.stdin.buffer.read()
        
        # Chemin de sortie dans HDFS pour les fichiers CSV intermédiaires (passé comme argument depuis NiFi)
        hdfs_base_path = sys.argv[1]
        
        # Construire le chemin de sortie pour le JSON final
        # hdfs_base_path = /data/ministere_economie_numerique/observatoire_numerique/68125741c117df7d540d99c3/processing/step1_csvs
        # Remonter au répertoire parent de /processing/step1_csvs pour obtenir /data/ministere_economie_numerique/observatoire_numerique/68125741c117df7d540d99c3
        parent_dir = os.path.dirname(os.path.dirname(hdfs_base_path))
        # Construire le chemin /results/data.json dans le répertoire parent
        hdfs_output_file = os.path.join(parent_dir, "results", "data.json")
        # Résultat : /data/ministere_economie_numerique/observatoire_numerique/68125741c117df7d540d99c3/results/data.json
        
        # Étape 1 : Convertir Excel en CSV et sauvegarder sur HDFS
        csv_data = excel_to_csv(excel_data, separator="|")
        saved_files = save_csv_to_hdfs(csv_data, hdfs_base_path)
        
        # Étape 2 : Traiter les CSV pour initialiser le big_json
        big_json = process_csvs_and_init_json(hdfs_base_path, separator="|")
        
        # Étape 3 : Mettre à jour les labels des catégories
        big_json = fix_labels(big_json)
        
        # Étape 4 : Traiter les sources et les données pour ajouter les indicateurs
        updated_big_json = process_sources_and_data(big_json, hdfs_base_path, separator="|")
        
        # Étape 5 : Sauvegarder le JSON final sur HDFS
        save_final_json(updated_big_json, hdfs_output_file)
        
        # Indiquer que le script a réussi
        print("Success")
        
    except Exception as e:
        logging.error(f"Failed to process Excel to final JSON: {e}")
        sys.exit(1)