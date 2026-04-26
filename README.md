🧬 AnnotSV Database

📖 Description

AnnotSV Database est un projet développé dans le cadre de mon stage de 1ʳᵉ année de BTS SIO (option SISR) au sein de l’unité INSERM UMR1078 (Brest).
L’objectif du projet est de construire une base de données relationnelle SQLite à partir des fichiers générés par l’outil AnnotSV, afin de faciliter l’analyse, le filtrage et la comparaison de variants structuraux (SV).

Le script AnnotSV_DB.py permet :

De créer automatiquement la base SQLite suivant un schéma normalisé.

D’importer des fichiers AnnotSV (.tsv) et d’enregistrer les informations sous forme de tables.

De générer automatiquement des statistiques détaillées à la fin de chaque import (gènes les plus affectés, frameshifts, fréquence des SV, etc.).

🧩 Technologies utilisées :

Python 3

SQLite3

CSV

Argparse

OS (gestion de fichiers)

🏗️ Fonctionnalités principales : 

🧱 Création de base	Génère une base SQLite prête à accueillir les données AnnotSV

📥 Importation de fichiers	Intègre un ou plusieurs fichiers .tsv AnnotSV

🧩 Analyse de variants	Détection de variants identiques (>80 % d’overlap)

📊 Statistiques automatiques	Affiche un résumé complet à la fin de chaque import

🧮 Calcul de fréquence	Calcule la fréquence des SV par échantillon

🧬 Gènes et transcripts	Identifie les gènes et transcripts affectés par chaque SV.

⚙️ Utilisation :

Voici les principales commandes disponibles :

# 🏗️ Créer la base de données
python AnnotSV_DB.py --create --db AnnotSV_DB.db

# 📥 Importer un fichier AnnotSV (TSV)
python AnnotSV_DB.py --import "C:\chemin\vers\le\fichier.tsv" --db AnnotSV_DB.db


💡 À la fin de chaque import, le script affiche automatiquement :

le nombre de samples, gènes, transcripts et SV importés,

la répartition des SV par échantillon,

les gènes et transcripts les plus impactés,

la présence de frameshifts dans les transcripts.

💡 Amélioration future possible

Une future version du script pourrait inclure une commande :

python AnnotSV_DB.py --stats --db AnnotSV_DB.db


…pour permettre de consulter les statistiques de la base sans refaire un import.
Cela rendrait l’outil plus flexible et réutilisable dans le temps.

📦 Installation :

Cloner le dépôt :

git clone https://github.com/jonathanlecann/AnnotSV_DataBase.git
cd AnnotSV_DataBase


Aucune dépendance externe requise
Le script repose uniquement sur des bibliothèques natives de Python :

import sqlite3

import csv

import argparse

import os
