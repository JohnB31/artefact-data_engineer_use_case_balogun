from datetime import datetime, timedelta
import logging
import os
from io import BytesIO
import pandas as pd
import psycopg2
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# =============================================================================
# CONFIGURATION DU LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION DEPUIS VARIABLES D'ENVIRONNEMENT
# =============================================================================

MINIO_HOST = os.getenv("MINIO_HOST", "minio_storage")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_ecommerce")

CONFIG_MINIO = {
    "host": "172.18.0.2",
    "port": 9000,
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123")
}

CONFIG_POSTGRES = {
    "host": POSTGRES_HOST,
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "password123"),
    "database": os.getenv("POSTGRES_DB", "ecommerce_db")
}

NOM_BUCKET = "folder-source"
CLE_FICHIER_CSV = "fashion_store_sales.csv"

# =============================================================================
# PARAMÈTRES DU DAG
# =============================================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG_ID = 'ingestion_ecommerce_minio_postgres'

# =============================================================================
# DÉFINITION DU DAG
# =============================================================================

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG pour ingérer les données e-commerce depuis Minio vers PostgreSQL',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['e-commerce', 'data-ingestion', 'minio', 'postgres'],
)

# =============================================================================
# FONCTIONS PYTHON
# =============================================================================

def valider_date(date_str):
    """Valider le format de date YYYYMMDD"""
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        logger.error(f"Format de date invalide: {date_str}. Attendu: YYYYMMDD")
        raise AirflowException(f"Format de date invalide: {date_str}")

def telecharger_et_filtrer_depuis_minio(ds, **context):
    """
    Télécharger le fichier CSV depuis Minio et filtrer par date
    ds = date de la tâche Airflow au format YYYY-MM-DD
    """
    
    # Convertir la date Airflow (YYYY-MM-DD) en format attendu (YYYYMMDD)
    date_str = ds.replace('-', '')
    logger.info(f"Traitement de la date: {date_str}")
    
    # Valider la date
    valider_date(date_str)
    
    try:
        logger.info(f"Connexion à Minio sur {CONFIG_MINIO['host']}:{CONFIG_MINIO['port']}...")
        
        # Créer le client Minio
        client_minio = Minio(
            f"{CONFIG_MINIO['host']}:{CONFIG_MINIO['port']}",
            access_key=CONFIG_MINIO['access_key'],
            secret_key=CONFIG_MINIO['secret_key'],
            secure=False
        )
        # Vérifier si le bucket existe
        if not client_minio.bucket_exists(NOM_BUCKET):
            logger.error(f"Le bucket '{NOM_BUCKET}' n'existe pas!")
            logger.info(f"Création du bucket '{NOM_BUCKET}'...")
            client_minio.make_bucket(NOM_BUCKET)
            logger.warning(f"Bucket créé mais vide. Veuillez uploader le fichier {CLE_FICHIER_CSV}")
            return None
        
        # Télécharger le fichier depuis Minio
        logger.info(f"Téléchargement du fichier {CLE_FICHIER_CSV} depuis le bucket {NOM_BUCKET}...")
        reponse = client_minio.get_object(NOM_BUCKET, CLE_FICHIER_CSV)
        contenu_csv = reponse.read()
        
        # Lire le CSV depuis le contenu en mémoire
        df = pd.read_csv(BytesIO(contenu_csv))
        logger.info(f"Fichier lu: {len(df)} lignes")
        
        # Convertir sale_date en datetime et filtrer
        df['sale_date'] = pd.to_datetime(df['sale_date'])
        date_cible = datetime.strptime(date_str, "%Y%m%d").date()
        
        df_filtre = df[df['sale_date'].dt.date == date_cible].copy()
        logger.info(f"Filtrage par date {date_str}: {len(df_filtre)} lignes trouvées")
        
        if len(df_filtre) == 0:
            logger.warning(f"Aucune donnée pour la date {date_str}")
            return None
        
        # Sauvegarder le dataframe dans XCom pour la tâche suivante
        context['task_instance'].xcom_push(key='df_filtre', value=df_filtre.to_json())
        logger.info("Données filtrées sauvegardées dans XCom")
        
        return {
            'nb_lignes': len(df_filtre),
            'date': date_str,
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement/filtrage: {e}")
        raise AirflowException(f"Erreur Minio: {str(e)}")

def inserer_dans_postgresql(ds, **context):
    """Insérer les données filtrées dans PostgreSQL (idempotente)"""
    
    # Récupérer le dataframe depuis XCom
    df_json = context['task_instance'].xcom_pull(
        task_ids='telecharger_et_filtrer_depuis_minio',
        key='df_filtre'
    )
    
    if df_json is None:
        logger.warning("Aucune donnée à insérer")
        return {'status': 'no_data'}
    
    df_filtre = pd.read_json(BytesIO(df_json.encode()))
    df_filtre['sale_date'] = pd.to_datetime(df_filtre['sale_date']).dt.strftime('%Y-%m-%d')
    logger.info(f"Récupération des données: {len(df_filtre)} lignes")
    
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(**CONFIG_POSTGRES)
        cursor = conn.cursor()
        logger.info("Connexion à PostgreSQL établie")
        
        # =====================================================================
        # TABLE 1: CUSTOMERS (idempotente)
        # =====================================================================
        logger.info("Insertion dans CUSTOMERS...")
        clients_df = df_filtre[['customer_id', 'first_name', 'last_name', 'email', 
                                'gender', 'age_range', 'country', 'signup_date']].drop_duplicates()
        
        for idx, ligne in clients_df.iterrows():
            cursor.execute("""
                INSERT INTO customers 
                (customer_id, first_name, last_name, email, gender, age_range, country, signup_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    gender = EXCLUDED.gender,
                    age_range = EXCLUDED.age_range,
                    country = EXCLUDED.country,
                    signup_date = EXCLUDED.signup_date
            """, (
                ligne['customer_id'],
                ligne['first_name'],
                ligne['last_name'],
                ligne['email'],
                ligne['gender'],
                ligne['age_range'],
                ligne['country'],
                ligne['signup_date']
            ))
        
        conn.commit()
        logger.info(f"CUSTOMERS: {len(clients_df)} lignes traitées")
        
        # =====================================================================
        # TABLE 2: PRODUCTS (idempotente)
        # =====================================================================
        logger.info("Insertion dans PRODUCTS...")
        produits_df = df_filtre[['product_id', 'product_name', 'category', 'brand', 
                                  'color', 'size', 'catalog_price', 'cost_price']].drop_duplicates()
        
        for idx, ligne in produits_df.iterrows():
            cursor.execute("""
                INSERT INTO products 
                (product_id, product_name, category, brand, color, size, catalog_price, cost_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    brand = EXCLUDED.brand,
                    color = EXCLUDED.color,
                    size = EXCLUDED.size,
                    catalog_price = EXCLUDED.catalog_price,
                    cost_price = EXCLUDED.cost_price
            """, (
                ligne['product_id'],
                ligne['product_name'],
                ligne['category'],
                ligne['brand'],
                ligne['color'],
                ligne['size'],
                ligne['catalog_price'],
                ligne['cost_price']
            ))
        
        conn.commit()
        logger.info(f"PRODUCTS: {len(produits_df)} lignes traitées")
        
        # =====================================================================
        # TABLE 3: SALES (idempotente)
        # =====================================================================
        logger.info("Insertion dans SALES...")
        ventes_df = df_filtre[['sale_id', 'sale_date', 'customer_id', 
                               'channel', 'channel_campaigns']].drop_duplicates()
        
        # Recalculer total_amount
        totaux_ventes = df_filtre.groupby('sale_id')['item_total'].sum().reset_index()
        totaux_ventes.columns = ['sale_id', 'total_amount']
        ventes_df = ventes_df.merge(totaux_ventes, on='sale_id', how='left')
        
        for idx, ligne in ventes_df.iterrows():
            cursor.execute("""
                INSERT INTO sales 
                (sale_id, sale_date, customer_id, channel, channel_campaigns, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (sale_id) DO UPDATE SET
                    sale_date = EXCLUDED.sale_date,
                    customer_id = EXCLUDED.customer_id,
                    channel = EXCLUDED.channel,
                    channel_campaigns = EXCLUDED.channel_campaigns,
                    total_amount = EXCLUDED.total_amount
            """, (
                ligne['sale_id'],
                ligne['sale_date'],
                ligne['customer_id'],
                ligne['channel'],
                ligne['channel_campaigns'],
                ligne['total_amount']
            ))
        
        conn.commit()
        logger.info(f"SALES: {len(ventes_df)} lignes traitées")
        
        # =====================================================================
        # TABLE 4: SALE_ITEMS (idempotente)
        # =====================================================================
        logger.info("Insertion dans SALE_ITEMS...")
        articles_vente_df = df_filtre[['item_id', 'sale_id', 'product_id', 'quantity', 
                                        'unit_price', 'original_price', 'discount_applied', 
                                        'discount_percent', 'item_total']]
        
        for idx, ligne in articles_vente_df.iterrows():
            cursor.execute("""
                INSERT INTO sale_items 
                (item_id, sale_id, product_id, quantity, unit_price, original_price, 
                 discount_applied, discount_percent, item_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_id) DO UPDATE SET
                    sale_id = EXCLUDED.sale_id,
                    product_id = EXCLUDED.product_id,
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    original_price = EXCLUDED.original_price,
                    discount_applied = EXCLUDED.discount_applied,
                    discount_percent = EXCLUDED.discount_percent,
                    item_total = EXCLUDED.item_total
            """, (
                ligne['item_id'],
                ligne['sale_id'],
                ligne['product_id'],
                ligne['quantity'],
                ligne['unit_price'],
                ligne['original_price'],
                ligne['discount_applied'],
                ligne['discount_percent'],
                ligne['item_total']
            ))
        
        conn.commit()
        logger.info(f"SALE_ITEMS: {len(articles_vente_df)} lignes traitées")
        
        logger.info("Ingestion terminée avec succès!")
        
        return {
            'customers': len(clients_df),
            'products': len(produits_df),
            'sales': len(ventes_df),
            'sale_items': len(articles_vente_df),
            'status': 'success'
        }
        
    except psycopg2.Error as e:
        logger.error(f"Erreur PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise AirflowException(f"Erreur PostgreSQL: {str(e)}")
        
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        if conn:
            conn.rollback()
        raise AirflowException(f"Erreur inattendue: {str(e)}")
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =============================================================================
# DÉFINITION DES TÂCHES
# =============================================================================

task_telecharger = PythonOperator(
    task_id='telecharger_et_filtrer_depuis_minio',
    python_callable=telecharger_et_filtrer_depuis_minio,
    provide_context=True,
    dag=dag,
)

task_inserer = PythonOperator(
    task_id='inserer_dans_postgresql',
    python_callable=inserer_dans_postgresql,
    provide_context=True,
    dag=dag,
)

# =============================================================================
# DÉPENDANCES
# =============================================================================

task_telecharger >> task_inserer