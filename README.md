# artefact-data_engineer_use_case_balogun

# Projet Data Engineering - Ingestion E-Commerce

Pipeline complet d'ingestion, normalisation et orchestration de données e-commerce.

---

## 1. MÉTHODOLOGIE & JUSTIFICATIONS

### 1.1 Analyse Exploratoire (ANALYSE_EXPLORATOIRE.ipynb)

**Objectif** : Comprendre la structure brute avant normalisation.

**Résultats** :
- Dataset : 2253 lignes × 29 colonnes
- Entités métier : 580 clients, 499 produits, 905 commandes, 2253 articles vendus
- Problème identifié : Redondance massive (même client/produit répétés, 74-78% de stockage gaspillé)

### 1.2 Normalisation en 3FN (MODELISATION_3FN_FINAL.pdf)

**Pourquoi normaliser ?**
- Table brute = dépendances partielles (product_name dépend de product_id, pas de item_id)
- Conséquence : redondance, maintenance difficile, risque d'anomalies d'insertion

**Solution : 4 tables indépendantes en 3FN**
```
CUSTOMERS (580)  ──1:N──> SALES (905)
                              │
                            1:N
                              │
PRODUCTS (499) <─N:1─ SALE_ITEMS (2253)
```

**Avantages mesurables**
- Réduction stockage : -70%
- Maintenance : Modification d'un prix = 1 ligne (au lieu de N)
- Intégrité : Clés étrangères garantissent la cohérence

### 1.3 Implémentation (init.sql)

**4 tables en 3FN** :
- `CUSTOMERS` : Clé primaire = customer_id
- `PRODUCTS` : Clé primaire = product_id
- `SALES` : Clé primaire = sale_id, FK = customer_id
- `SALE_ITEMS` : Clé primaire = item_id, FK = sale_id + product_id

### 1.4 Script Python (main.py)

**Fonctionnalités essentielles**
- Valide format date YYYYMMDD
- Télécharge CSV depuis Minio
- Filtre par date fournie
- Insère dans 4 tables avec upsert (`ON CONFLICT ... DO UPDATE`)
- Logging détaillé

**Idempotence garantie** : Exécutions multiples = même état final (pas de doublon)

### 1.5 Orchestration Airflow (dags/ingestion_dag.py)

**Même logique que main.py mais**
- Exécution automatique (cron : 0 2 * * * = 2h chaque matin)
- Résilience (2 retries si erreur)
- Monitoring via dashboard
- Dépendances entre tâches garanties

### 1.6 Conteneurisation (docker-compose.yml)

**Pourquoi Docker ?**
- Isolation : Chaque service dans son conteneur
- Reproducibilité : Même environnement partout (Windows/Mac/Linux)
- Dépendances figées (PostgreSQL 15, Minio, Airflow 2.7.3)

**5 conteneurs orchestrés** :
- `postgres_ecommerce` (5432) → Base cible
- `postgres_airflow` (5433) → Métadonnées Airflow
- `minio_storage` (9000-9001) → Stockage S3
- `airflow_webserver` (8080) → Interface Airflow + exécution scripts
- `airflow_scheduler` → Scheduling automatique

---

## 2. EXÉCUTION PAS À PAS

### ⚠️ NOTE IMPORTANTE : IP Minio Dynamique

**L'adresse IP de Minio peut changer à chaque redémarrage du Docker.**

Sur votre machine, elle sera probablement différente de `172.18.0.2`. Vous DEVEZ :
1. **Trouver votre IP Minio**
2. **Mettre à jour les fichiers de configuration**

---

## 2.1 Trouver l'adresse IP de Minio

```bash
# Windows
docker inspect minio_storage | findstr IPAddress

# Linux/Mac
docker inspect minio_storage | grep IPAddress

# Résultat type:
# "IPAddress": "172.18.0.2"
```

Notez cette adresse IP, elle sera nécessaire pour les modifications des fichiers main.py et ingestion_dag.py.

---

## 2.2 Mettre à jour la configuration

### Pourquoi utiliser l'IP au lieu du hostname ?

Airflow s'exécute dans son propre conteneur avec son propre réseau DNS. Le hostname `minio_storage` fonctionne dans le contexte Docker, mais le script `main.py` exécuté via `docker exec` peut avoir des problèmes de résolution DNS selon la configuration du réseau.

**Solution** : Utiliser l'IP directe (`172.18.0.2`) garantit une connexion robuste et fiable.

---

### Modifier `main.py`

Ouvrir `main.py` et chercher la section **CONFIGURATION** (vers la ligne 40) :

```python
CONFIG_POSTGRES = {
    "host": "postgres_ecommerce",
    "port": 5432,
    "user": "admin",
    "password": "password123",
    "database": "ecommerce_db"
}

CONFIG_MINIO = {
    "host": "METTEZ ICI VOTRE ADRESSE IP",  # ← Remplacer par votre IP (ex: 172.18.0.2)
    "port": 9000,
    "access_key": "minioadmin",
    "secret_key": "minioadmin123"
}
```

**Exemple avec IP trouvée** :
```python
CONFIG_MINIO = {
    "host": "172.18.0.2",  # ← Votre IP
    "port": 9000,
    "access_key": "minioadmin",
    "secret_key": "minioadmin123"
}
```

---

### Modifier `dags/ingestion_dag.py`

Ouvrir `dags/ingestion_dag.py` et chercher la section **CONFIGURATION DEPUIS VARIABLES D'ENVIRONNEMENT** (vers la ligne 27) :

```python
CONFIG_MINIO = {
    "host": "METTEZ ICI VOTRE ADRESSE IP",  # ← Remplacer par votre IP (ex: 172.18.0.2)
    "port": 9000,
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123")
}
```

**Exemple avec IP trouvée** :
```python
CONFIG_MINIO = {
    "host": "172.18.0.2",  # ← Votre IP
    "port": 9000,
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123")
}
```

---

### Récapitulatif des modifications

| Fichier | Section | Ligne | Changer | En |
|---------|---------|-------|--------|-----|
| `main.py` | CONFIG_MINIO | ~40 | `"host": "METTEZ ICI VOTRE ADRESSE IP"` | `"host": "172.18.0.2"` |
| `dags/ingestion_dag.py` | CONFIG_MINIO | ~27 | `"host": "METTEZ ICI VOTRE ADRESSE IP"` | `"host": "172.18.0.2"` |

⚠️ **Les deux fichiers DOIVENT avoir la même IP**

---

## 2.3 Prérequis Système

- **Docker Desktop** 24.0+
- **RAM** : 8 GB minimum
- **Disque** : 10 GB libre
- **Git** : 2.30+

**Vérification** :
```bash
docker --version        # Docker version 24.x.x
docker-compose --version # Docker Compose version 2.x.x
```

---

## 2.4 Structure des Dossiers

À la racine du projet (`PROJET ARTEFACT/`) :

```
 docker-compose.yml
 init.sql
 main.py                     (après modification IP)
 fashion_store_sales.csv
 ANALYSE_EXPLORATOIRE.ipynb
 MODELISATION_3FN_FINAL.pdf
 dags/
   └── ingestion_dag.py        (après modification IP)
```

---

## 2.5 Démarrer l'Infrastructure

```bash
# 1. Se placer dans le dossier du projet
cd /chemin/vers/PROJET\ ARTEFACT

# 2. Démarrer tous les conteneurs
docker-compose up -d

# 3. Attendre que tout soit prêt (90 secondes)
# Airflow nécessite du temps pour initialiser sa base de données

# 4. Vérifier que tous les conteneurs sont actifs
docker-compose ps

# Résultat attendu:
# NAME                 STATUS              PORTS
# postgres_ecommerce   Up (healthy)        0.0.0.0:5432->5432/tcp
# postgres_airflow     Up (healthy)        0.0.0.0:5433->5432/tcp
# minio_storage        Up (healthy)        0.0.0.0:9000-9001->9000-9001/tcp
# airflow_webserver    Up                  0.0.0.0:8080->8080/tcp
# airflow_scheduler    Up                  8080/tcp
```

---

## 2.6 Uploader le Fichier CSV dans Minio

```
1. Ouvrir : http://localhost:9001
2. Identifiants : minioadmin / minioadmin123

3. Créer un bucket :
   - Cliquer sur "Create Bucket"
   - Nommer: folder-source
   - Valider

4. Uploader le fichier :
   - Sélectionner le bucket "folder-source"
   - Drag & drop fashion_store_sales.csv
   - Valider
```

---

## 2.7 Tester le Script Python

**Exécuter l'ingestion pour la date 20250616** :

```bash
docker exec airflow_webserver python /opt/airflow/main.py 20250616
```

**Résultat attendu** :
```
======================================================================
DÉBUT DE L'INGESTION (Minio → PostgreSQL)
======================================================================
Date cible: 20250616
Connexion à Minio sur 172.18.0.2:9000...
Téléchargement du fichier fashion_store_sales.csv...
Fichier lu: 2253 lignes
Filtrage par date 20250616: 52 lignes trouvées
Connexion à PostgreSQL établie
Insertion dans CUSTOMERS... CUSTOMERS: 22 lignes traitées
Insertion dans PRODUCTS... PRODUCTS: 48 lignes traitées
Insertion dans SALES... SALES: 22 lignes traitées
Insertion dans SALE_ITEMS... SALE_ITEMS: 52 lignes traitées
======================================================================
INGESTION RÉUSSIE
======================================================================
```

---

## 2.8 Vérifier les Données dans PostgreSQL

```bash
# Compter les lignes par table
docker exec postgres_ecommerce psql -U admin -d ecommerce_db -c \
  "SELECT 'customers' as table_name, COUNT(*) FROM customers
   UNION ALL
   SELECT 'products', COUNT(*) FROM products
   UNION ALL
   SELECT 'sales', COUNT(*) FROM sales
   UNION ALL
   SELECT 'sale_items', COUNT(*) FROM sale_items;"

# Résultat attendu:
#  table_name | count
# -----------+-------
#  customers |    22
#  products  |    48
#  sales     |    22
#  sale_items|    52
```

---

## 2.9 Tester l'Idempotence

L'idempotence garantit que relancer avec les mêmes paramètres ne crée pas de doublons.

```bash
# Première exécution
docker exec airflow_webserver python /opt/airflow/main.py 20250616

# Deuxième exécution (même date)
docker exec airflow_webserver python /opt/airflow/main.py 20250616

# Vérifier que le nombre de lignes n'a pas augmenté
docker exec postgres_ecommerce psql -U admin -d ecommerce_db -c \
  "SELECT COUNT(*) FROM customers;"

# Résultat : Doit afficher 22 (pas 44)
```

---

## 2.10 Tester le DAG Airflow

**Vérifier que le DAG est détecté** :

```bash
docker exec airflow_webserver airflow dags list

# Résultat attendu:
# dag_id                             | filepath         | owner         | paused
# ===================================+==================+===============+=======
# ingestion_ecommerce_minio_postgres | ingestion_dag.py | data_engineer | False
```

**Tester l'exécution du DAG** :

```bash
docker exec airflow_webserver airflow dags test ingestion_ecommerce_minio_postgres 2025-06-16

# Résultat attendu:
# [2026-01-21T23:13:58.000+0000] DAG RUN FINISHED: state=success
# CUSTOMERS: 22 lignes traitées
# PRODUCTS: 48 lignes traitées
# SALES: 22 lignes traitées
# SALE_ITEMS: 52 lignes traitées
```

**Accéder au Dashboard Airflow** :

```
http://localhost:8080
Identifiants : airflow / airflow
```

Vous verrez le DAG `ingestion_ecommerce_minio_postgres` avec ses 2 tâches.

---

## 2.11 Vérifier l'Intégrité des Données

```bash
# Chercher les ventes orphelines (customer_id inexistant)
docker exec postgres_ecommerce psql -U admin -d ecommerce_db -c \
  "SELECT COUNT(*) as orphaned_sales
   FROM sales s
   LEFT JOIN customers c ON s.customer_id = c.customer_id
   WHERE c.customer_id IS NULL;"

# Résultat attendu: 0 (aucune orpheline)
```

---

## 3. ACCÈS AUX SERVICES

| Service | URL | Identifiants |
|---------|-----|--------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| Minio | http://localhost:9001 | minioadmin / minioadmin123 |
| PostgreSQL | localhost:5432 | admin / password123 |

---

## 4. RÉSUMÉ ÉTAPES EXÉCUTION

1.  **Trouver l'IP Minio** : `docker inspect minio_storage | grep IPAddress`
2. **Mettre à jour** : main.py et dags/ingestion_dag.py avec l'IP
3. **Démarrer** : `docker-compose up -d`
4. **Attendre** : 90 secondes minimum
5. **Uploader CSV** : http://localhost:9001 (bucket folder-source)
6. **Tester** : `docker exec airflow_webserver python /opt/airflow/main.py 20250616`
7. **Valider** : Données dans PostgreSQL
8. **Monitorer** : http://localhost:8080 (Airflow)
