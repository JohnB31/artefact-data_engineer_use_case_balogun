import argparse
import logging
import sys
from datetime import datetime
import pandas as pd
import psycopg2
from minio import Minio
from io import BytesIO

# =============================================================================
# CONFIGURATION DU LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG_POSTGRES = {
    "host": "postgres_ecommerce",
    "port": 5432,
    "user": "admin",
    "password": "password123",
    "database": "ecommerce_db"
}

CONFIG_MINIO = {
    "host": "172.18.0.2",
    "port": 9000,
    "access_key": "minioadmin",
    "secret_key": "minioadmin123"
}

NOM_BUCKET = "folder-source"
CLE_FICHIER_CSV = "fashion_store_sales.csv"

# =============================================================================
# FONCTIONS
# =============================================================================

def valider_date(date_str):
    """Valider le format de date YYYYMMDD"""
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        logger.error(f"Format de date invalide: {date_str}. Attendu: YYYYMMDD")
        return False

def lire_et_filtrer_csv_depuis_minio(date_str):
    """Lire le CSV depuis Minio et filtrer par date"""
    try:
        logger.info(f"Connexion à Minio sur {CONFIG_MINIO['host']}:{CONFIG_MINIO['port']}...")
        
        # Créer le client Minio
        client_minio = Minio(
            f"{CONFIG_MINIO['host']}:{CONFIG_MINIO['port']}",
            access_key=CONFIG_MINIO['access_key'],
            secret_key=CONFIG_MINIO['secret_key'],
            secure=False
        )
        
        logger.info(f"Téléchargement du fichier {CLE_FICHIER_CSV} depuis le bucket {NOM_BUCKET}...")
        
        # Télécharger le fichier depuis Minio
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
        
        return df_filtre
        
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement/filtrage depuis Minio: {e}")
        return None

def inserer_dans_postgresql(df_filtre):
    """Insérer les données filtrées dans PostgreSQL (idempotente)"""
    
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
        clients_df = df_filtre[['customer_id', 'first_name', 'last_name', 'email', 'gender', 'age_range', 'country', 'signup_date']].drop_duplicates()
        
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
        produits_df = df_filtre[['product_id', 'product_name', 'category', 'brand', 'color', 'size', 'catalog_price', 'cost_price']].drop_duplicates()
        
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
        ventes_df = df_filtre[['sale_id', 'sale_date', 'customer_id', 'channel', 'channel_campaigns']].drop_duplicates()
        
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
        articles_vente_df = df_filtre[['item_id', 'sale_id', 'product_id', 'quantity', 'unit_price', 'original_price', 'discount_applied', 'discount_percent', 'item_total']]
        
        for idx, ligne in articles_vente_df.iterrows():
            cursor.execute("""
                INSERT INTO sale_items 
                (item_id, sale_id, product_id, quantity, unit_price, original_price, discount_applied, discount_percent, item_total)
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
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Erreur PostgreSQL: {e}")
        if conn:
            conn.rollback()
        return False
        
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =============================================================================
# MAIN
# =============================================================================

def main():
    analyseur = argparse.ArgumentParser(
        description="Script d'ingestion de données e-commerce depuis Minio vers PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Exemple: python main.py 20250616"
    )
    
    analyseur.add_argument(
        'date',
        type=str,
        help='Date au format YYYYMMDD'
    )
    
    args = analyseur.parse_args()
    
    logger.info("="*70)
    logger.info("DÉBUT DE L'INGESTION (Minio → PostgreSQL)")
    logger.info("="*70)
    logger.info(f"Date cible: {args.date}")
    
    # Valider la date
    if not valider_date(args.date):
        logger.error("Arrêt du script")
        sys.exit(1)
    
    # Lire et filtrer le CSV depuis Minio
    df_filtre = lire_et_filtrer_csv_depuis_minio(args.date)
    if df_filtre is None:
        logger.error("Arrêt du script")
        sys.exit(1)
    
    # Insérer dans PostgreSQL
    if not inserer_dans_postgresql(df_filtre):
        logger.error("Arrêt du script")
        sys.exit(1)
    
    logger.info("="*70)
    logger.info("INGESTION RÉUSSIE")
    logger.info("="*70)
    sys.exit(0)

if __name__ == "__main__":
    main()