import duckdb
import logging
import os

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)

def extract_raw_data(con):
    logging.info("Iniciando EXTRAÇÃO (Carregamento Raw)...")
    
    # Criar tabelas raw ou fazer ingestão direta do CSV
    con.execute("""
        CREATE OR REPLACE TABLE raw_customers AS 
        SELECT * FROM read_csv_auto('data/raw/customers.csv');
    """)
    con.execute("""
        CREATE OR REPLACE TABLE raw_products AS 
        SELECT * FROM read_csv_auto('data/raw/products.csv');
    """)
    con.execute("""
        CREATE OR REPLACE TABLE raw_orders AS 
        SELECT * FROM read_csv_auto('data/raw/orders.csv');
    """)
    con.execute("""
        CREATE OR REPLACE TABLE raw_order_items AS 
        SELECT * FROM read_csv_auto('data/raw/order_items.csv');
    """)
    logging.info("Extração concluída com sucesso. Tabelas raw criadas.")

def transform_data(con):
    logging.info("Iniciando TRANSFORMAÇÃO (Limpeza e Ajustes)...")
    
    # Customers - tratar nulos
    con.execute("""
        CREATE OR REPLACE TABLE tr_customers AS 
        SELECT 
            customer_id,
            customer_name,
            COALESCE(city, 'Não Informada') AS city,
            COALESCE(state, 'NI') AS state,
            CAST(signup_date AS DATE) AS signup_date
        FROM raw_customers;
    """)
    
    # Products - remover preço nulo 
    con.execute("""
        CREATE OR REPLACE TABLE tr_products AS 
        SELECT 
            product_id,
            product_name,
            category,
            CAST(price AS DOUBLE) AS price
        FROM raw_products
        WHERE price IS NOT NULL;
    """)
    
    # Orders
    con.execute("""
        CREATE OR REPLACE TABLE tr_orders AS 
        SELECT 
            order_id,
            customer_id,
            CAST(order_date AS TIMESTAMP) AS order_date,
            status
        FROM raw_orders;
    """)
    
    # Order Items - calcular valor total e limpar quantidade invalida
    con.execute("""
        CREATE OR REPLACE TABLE tr_order_items AS 
        SELECT 
            order_item_id,
            order_id,
            product_id,
            CAST(quantity AS INTEGER) AS quantity,
            CAST(unit_price AS DOUBLE) AS unit_price,
            (CAST(quantity AS INTEGER) * CAST(unit_price AS DOUBLE)) AS total_value
        FROM raw_order_items
        WHERE quantity > 0;
    """)
    
    logging.info("Transformação das tabelas tratadas concluída.")

def load_analytical_layer(con):
    logging.info("Iniciando CARGA (Criação da Tabela Analítica)...")
    
    # Consolidar em uma tabela analítica para consulta final.
    # Vamos cruzar tudo e deixar uma estrutura "flat".
    con.execute("""
        CREATE OR REPLACE TABLE analytics_sales AS 
        SELECT 
            i.order_item_id,
            i.quantity,
            i.unit_price,
            i.total_value,
            o.order_id,
            o.order_date,
            o.status,
            p.product_id,
            p.product_name,
            p.category,
            c.customer_id,
            c.customer_name,
            c.city,
            c.state
        FROM tr_order_items i
        INNER JOIN tr_orders o ON i.order_id = o.order_id
        INNER JOIN tr_products p ON i.product_id = p.product_id
        INNER JOIN tr_customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'completed'; -- Filtro analítico opcional, usar apenas pedidos concluídos
    """)
    
    logging.info("Carga da Tabela Analítica (analytics_sales) concluída.")

def main():
    logging.info("Iniciando Pipeline ETL.")
    
    # Idempotência e conexão com duckdb
    db_path = 'warehouse.duckdb'
    con = duckdb.connect(db_path)
    
    try:
        extract_raw_data(con)
        transform_data(con)
        load_analytical_layer(con)
        logging.info("Pipeline ETL finalizado com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro no pipeline ETL: {e}")
        
    finally:
        con.close()

if __name__ == "__main__":
    main()
