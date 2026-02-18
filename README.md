# Weather Data Processing

![Python](https://img.shields.io/badge/python-3.8+-blue)
![Airflow](https://img.shields.io/badge/airflow-2.x-orange)
![Kedro](https://img.shields.io/badge/kedro-0.18-green)

Pipeline de données météo automatisé démontrant la mise en place d’une ingestion d’API, d’un traitement de données structuré et d’une orchestration avec Apache Airflow, dans une logique de projet Data Engineering professionnel.

## Vue d'ensemble

Ce projet automatise la collecte de données météorologiques en temps réel via l'API OpenWeatherMap, les transforme en format structuré et les stocke dans une base de données PostgreSQL. Le pipeline s'exécute automatiquement toutes les 7 minutes grâce à l'orchestration Airflow.

### Fonctionnalités principales

- ✅ Récupération automatique des données météorologiques par ville
- ✅ Géocodage inversé pour obtenir les coordonnées GPS
- ✅ Transformation et nettoyage des données
- ✅ Orchestration Airflow avec exécution périodique
- ✅ Containerisation Docker pour reproductibilité
- ✅ Pipeline Kedro modulaire et testable
- ✅ Stockage des données en base PostgreSQL

## Architecture

Le projet repose sur une architecture de pipeline de données orchestrée, découpée en plusieurs étapes clairement identifiées :

OpenWeatherMap API

↓

Ingestion des données brutes

↓

Pipeline Kedro (nettoyage & transformation)

↓

Base de données PostgreSQL

↓

Orchestration et planification via Airflow


Apache Airflow est utilisé pour planifier et orchestrer l’exécution des pipelines Kedro, tandis que Docker permet de garantir un environnement reproductible.

Voici la structure du repertoire
```
weather_data_processing/
├── kedroweather/          # Pipeline Kedro principal
│   ├── src/               # Code source du pipeline
│   ├── data/              # Données à différents étapes
│   ├── conf/              # Configuration (paramètres, credentials)
│   ├── tests/             # Tests unitaires
│   ├── Dockerfile         # Containerisation
│   └── requirements.txt    # Dépendances Python
│
├── airflow_dags/          # Orchestration Airflow
│    ├── dags/              # DAGs Airflow
│    ├── logs/              # Logs d'exécution
│    ├── plugins/           # Extensions Airflow
│    └── docker-compose.yml # Stack Airflow
└── output/          # Répertoire de sortie
```

## Structure du pipeline Kedro

### Pipeline : `weather_cities`

Le pipeline exécute deux nœuds en séquence :

1. **`city_weather_info_node`** - Récupération des données
   - Fonction : `get_city_weather_info()`
   - Récupère pour chaque ville :
     - Les coordonnées géographiques (latitude, longitude)
     - Les données météorologiques actuelles (température, humidité, description)
   - Entrée : liste de villes depuis `params:cities`
   - Sortie : `city_weather_info` (données brutes JSON)

2. **`readable_city_weather_data_node`** - Transformation et chargement en DB
   - Fonction : `readable_weather_data()`
   - Transforme les données JSON en DataFrame pandas
   - Charge les données dans PostgreSQL (table `weather_data`)
   - Sortie : `readable_city_weather_data` (données sauvegardées en DB)

### Structure des données (Data Catalog)

Le projet suit la structure Kedro standard :

- **`01_raw/`** - Données brutes en provenance de l'API
- **`02_intermediate/`** - Données transformées intermédiaires (base de données)
- **`03_primary/`** - Données nettoyées et validées (PostgreSQL)
- **`04_feature/`** - Données avec features engineering
- **`05_model_input/`** - Données prêtes pour modélisation
- **`06_models/`** - Modèles entraînés
- **`07_model_output/`** - Résultats des prédictions
- **`08_reporting/`** - Rapports et visualisations

**Base de données PostgreSQL** : Table principale `weather_data` contenant :
- `id` (clé primaire)
- `city` (nom de la ville)
- `timestamp` (date/heure de la mesure)
- `temperature` (température en Celsius)
- `humidity` (humidité relative %)
- `weather_description` (description météo)
- `created_at` (date d'insertion)

## Installation

### Prérequis

- Python 3.8+
- Docker et Docker Compose
- PostgreSQL (fourni dans Docker Compose)
- Clé API OpenWeatherMap (obtenir sur https://openweathermap.org/api)

### Étapes d'installation

1. **Cloner le repositorio**
   ```bash
   git clone <your-repo>
   cd weatherDataProcessing
   ```

2. **Configurer la clé API et la base de données**
   
   Dans `kedroweather/conf/local/credentials.yml` :
   ```yaml
   api_key: "votre_cle_api_ici"
   db_connection: "postgresql://user:password@localhost:5432/weather_db"
   ```
   
   Ou via variables d'environnement :
   ```bash
   export API_KEY="votre_cle_api_ici"
   export DB_CONNECTION="postgresql://user:password@localhost:5432/weather_db"
   ```

3. **Configurer les villes**
   
   Dans `kedroweather/conf/base/parameters_weather_cities.yml` :
   ```yaml
   cities:
     - Paris
     - Lyon
     - Marseille
   ```

4. **Installer les dépendances (mode local)**
   ```bash
   cd kedroweather
   pip install -r requirements.txt
   ```

## Utilisation

### Mode local avec Kedro

Exécuter le pipeline manuellement :
```bash
cd kedroweather
kedro run
```

Afficher le pipeline visualisé :
```bash
kedro viz
```

Exécuter un nœud spécifique :
```bash
kedro run --node city_weather_info_node
```

Exécuter avec paramètres personnalisés :
```bash
kedro run --params cities='[Paris,Londres,Berlin]'
```

### Mode Docker avec Airflow

1. **Construire l'image Kedro**
   ```bash
   cd kedroweather
   docker-compose build
   ```

2. **Lancer la stack Airflow**
   ```bash
   cd airflow_dags
   docker-compose up -d
   ```

3. **Accéder à Airflow**
   - URL : `http://localhost:8080`
   - Utilisateur : `admin`
   - Mot de passe : `admin`

4. **Déclencher le DAG**
   - Aller sur l'onglet "DAGs"
   - Trouver `kedro_pipeline`
   - Cliquer sur le bouton play
   - Les exécutions suivantes se font automatiquement toutes les 7 minutes

### Vérifier les résultats

Le pipeline stocke les données directement dans PostgreSQL. Pour consulter les données :

```bash
# Accéder à la base de données
psql -U postgres -d weather_db -h localhost

# Consulter les données
SELECT * FROM weather_data ORDER BY created_at DESC LIMIT 10;

# Statistiques par ville
SELECT city, COUNT(*), AVG(temperature), MAX(humidity)
FROM weather_data
GROUP BY city;
```

Contenu de la table `weather_data` :
```
 id | city  | timestamp           | temperature | humidity | weather_description | created_at
---+-------+---------------------+-------------+----------+---------------------+------------------------
  1 | Paris | 2026-01-27 19:15:00 |         8.5 |       65 | Nuageux             | 2026-01-27 19:15:30
  2 | Lyon  | 2026-01-27 19:15:00 |         6.2 |       70 | Partiellement clair | 2026-01-27 19:15:35
```

Ces données peuvent ensuite être utilisées pour de la visualisation, de l'analyse ou intégrées dans un système de BI.


## Configuration

### Paramètres Kedro

`kedroweather/conf/base/parameters.yml` :
- Configuration générale du pipeline

`kedroweather/conf/base/parameters_weather_cities.yml` :
- Liste des villes à traiter

`kedroweather/conf/base/catalog.yml` :
- Définition des données (sources, formats, emplacements)

### Airflow

`airflow_dags/docker-compose.yml` :
- Configuration de la stack Airflow
- Service PostgreSQL intégré
- Variables d'environnement
- Volumes montés

`airflow_dags/dags/kedro_pipeline_dag.py` :
- Définition du DAG
- Planification (toutes les 7 minutes)
- Configuration du conteneur Docker

## Fichiers clés

### Kedro

| Fichier | Rôle |
|---------|------|
| `src/kedroweather/pipelines/weather_cities/nodes.py` | Fonctions métier du pipeline |
| `src/kedroweather/pipelines/weather_cities/pipeline.py` | Définition du pipeline (nœuds + dépendances) |
| `src/kedroweather/pipeline_registry.py` | Enregistrement des pipelines |
| `conf/base/catalog.yml` | Catalogue des données (configuration I/O) |

### Airflow

| Fichier | Rôle |
|---------|------|
| `dags/kedro_pipeline_dag.py` | DAG principal exécutant le pipeline |
| `docker-compose.yml` | Stack Airflow complète |

## Développement

### Tests

Exécuter les tests unitaires :
```bash
cd kedroweather
pytest tests/
```

Structure des tests :
```
tests/
├── pipelines/
│   └── weather_cities/
│       └── test_pipeline.py
└── test_run.py
```

### Logs

- **Logs Kedro** : `kedroweather/logs/`
- **Logs Airflow** : `airflow_dags/logs/`

Consulter les logs d'une exécution :
```bash
# Airflow
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler

# Kedro (dans le DAG)
docker-compose logs -f airflow-worker
```

### Ajouter une nouvelle ville

1. Modifier `kedroweather/conf/base/parameters_weather_cities.yml`
2. Ajouter la ville à la liste `cities`
3. Relancer le pipeline (automatique via Airflow)

### Ajouter des données supplémentaires

Modifier `nodes.py` pour récupérer des champs supplémentaires :

```python
def readable_weather_data(weather_data: dict) -> dict[str, pd.DataFrame]:
    # ...
    df = pd.DataFrame([{
        "city": city,
        'timestamp': pd.to_datetime(current['dt'], unit='s'),
        'temperature': current['temp'],
        'humidity': current['humidity'],
        'pressure': current['pressure'],  # Nouveau champ
        'weather_description': current['weather'][0]['description']
    }])
    # ...
```

## Troubleshooting

### Erreur : "City not found"
- Vérifier l'orthographe de la ville
- Utiliser le nom en anglais (ex: "Paris" au lieu de "París")

### Erreur : "API_KEY not found"
- Vérifier que `API_KEY` est défini :
  ```bash
  echo $API_KEY  # Afficher la variable
  ```
- Vérifier les credentials dans `conf/local/credentials.yml`

### Airflow ne démarre pas
- Vérifier les ports (8080, 5432) ne sont pas utilisés
- Supprimer et recréer les conteneurs :
  ```bash
  docker-compose down -v
  docker-compose up -d
  ```

### Données ne s'écrivent pas dans `output/`
- Vérifier les permissions du répertoire
- Vérifier le mount du volume dans `docker-compose.yml`

## Architecture technique

### Stack technologique

- **Orchestration** : Apache Airflow
- **Pipeline** : Kedro
- **Langage** : Python 3.8+
- **Données** : pandas, requests
- **API** : OpenWeatherMap
- **Containerisation** : Docker, Docker Compose
- **Logs** : Airflow logs, Kedro logs

### Flux de données

```
OpenWeatherMap API
       ↓
get_city_coordinates() → Récupère lat/lon
       ↓
get_city_weather_info() → Récupère données météo
       ↓
readable_weather_data() → Transforme en DataFrame
       ↓
PostgreSQL → Table weather_data (INSERT)
```

## Performance

- **Fréquence** : Toutes les 7 minutes
- **Temps d'exécution** : ~5-10 secondes par exécution
- **Rétention** : Les données s'accumulent (considérer un archivage pour production)

## About the author

Ce projet a été réalisé par **Lucas Vltn**, Data Engineer junior, dans un objectif de montée en compétences et de professionnalisation.  
Il illustre ma capacité à concevoir, structurer et orchestrer des pipelines de données de bout en bout, depuis l’ingestion d’une API externe jusqu’à la production de données exploitables, en utilisant des outils standards de l’écosystème data (Airflow, Kedro, Docker).


## Notes de production

Pour un déploiement en production :

1. **Sécurité**
   - Stocker les secrets dans un vault (Vault, AWS Secrets Manager)
   - Ne pas commiter de credentials
   - Utiliser une connexion SSL/TLS pour PostgreSQL

2. **Scalabilité**
   - Augmenter le nombre de workers Airflow
   - Optimiser les index PostgreSQL (créer un index sur `created_at` et `city`)

3. **Base de données**
   - Archiver/nettoyer les vieilles données
   - Implémenter une stratégie de partitionnement par date
   - Ajouter des validations et qualité de données
   - Configurer les backups réguliers
   - Mettre en place le monitoring (requêtes lentes, disque, CPU)

## License

Projet personnel - Tous droits réservés

## Contact

Lucas

---

**Dernière mise à jour** : Janvier 2026
