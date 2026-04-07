import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

def main():
    fake = Faker('pt_BR')
    Faker.seed(42)
    random.seed(42)

    # Configs
    NUM_CUSTOMERS = 3000
    NUM_PRODUCTS = 300
    NUM_ORDERS = 10000
    NUM_ORDER_ITEMS = 20000

    # Ensure output directory exists
    os.makedirs('data/raw', exist_ok=True)

    print("Gerando customers.csv...")
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customer_id = f"CUST-{i:05d}"
        customer_name = fake.name()
        # Introduce some nulls
        city = fake.city() if random.random() > 0.05 else None
        state = fake.state_abbr() if random.random() > 0.02 else None
        signup_date = fake.date_between(start_date='-2y', end_date='today')
        customers.append([customer_id, customer_name, city, state, signup_date])
    
    df_customers = pd.DataFrame(customers, columns=['customer_id', 'customer_name', 'city', 'state', 'signup_date'])
    df_customers.to_csv('data/raw/customers.csv', index=False)

    print("Gerando products.csv...")
    categories = ['Eletrônicos', 'Móveis', 'Roupas', 'Brinquedos', 'Alimentos', 'Livros']
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        product_id = f"PROD-{i:04d}"
        product_name = fake.word().capitalize() + " " + fake.word()
        category = random.choice(categories)
        # Price from 10 to 1000
        price = round(random.uniform(10.0, 1000.0), 2)
        # Introduce a few nulls in price
        if random.random() < 0.03:
            price = None
            
        products.append([product_id, product_name, category, price])
        
    df_products = pd.DataFrame(products, columns=['product_id', 'product_name', 'category', 'price'])
    df_products.to_csv('data/raw/products.csv', index=False)

    print("Gerando orders.csv...")
    statuses = ['completed', 'pending', 'canceled', 'refunded']
    orders = []
    customer_ids = df_customers['customer_id'].tolist()
    for i in range(1, NUM_ORDERS + 1):
        order_id = f"ORD-{i:06d}"
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        status = random.choices(statuses, weights=[0.7, 0.15, 0.1, 0.05], k=1)[0]
        orders.append([order_id, customer_id, order_date, status])
        
    df_orders = pd.DataFrame(orders, columns=['order_id', 'customer_id', 'order_date', 'status'])
    df_orders.to_csv('data/raw/orders.csv', index=False)

    print("Gerando order_items.csv...")
    order_items = []
    order_ids = df_orders['order_id'].tolist()
    
    # Pre-calculate a slightly realistic unit_price from products
    # if price is null in product, fallback to a random choice for unit_price
    product_dict = {p[0]: p[3] for p in products}

    for i in range(1, NUM_ORDER_ITEMS + 1):
        order_item_id = f"ITEM-{i:06d}"
        order_id = random.choice(order_ids)
        # Pick a random product
        product_id = random.choice(list(product_dict.keys()))
        quantity = random.randint(1, 5)
        
        unit_price = product_dict.get(product_id)
        if unit_price is None:
            unit_price = round(random.uniform(10.0, 500.0), 2)
            
        # Introduce some negative/invalid values just for ETL cleaning
        if random.random() < 0.01:
            quantity = -1

        order_items.append([order_item_id, order_id, product_id, quantity, unit_price])
        
    df_items = pd.DataFrame(order_items, columns=['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price'])
    df_items.to_csv('data/raw/order_items.csv', index=False)

    print("Geração concluída com sucesso! Os arquivos estão em data/raw/")

if __name__ == '__main__':
    main()
