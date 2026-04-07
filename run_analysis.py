import duckdb
import pandas as pd

def parse_analysis(con):
    print("="*50)
    print("RELATÓRIO ANALÍTICO - E-COMMERCE")
    print("="*50)

    # 1. faturamento total por mês
    print("\n1. Faturamento Total por Mês:")
    q1 = """
        SELECT 
            DATE_TRUNC('month', order_date) AS mes,
            SUM(total_value) as faturamento_total
        FROM analytics_sales
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 12;
    """
    df1 = con.execute(q1).fetchdf()
    print(df1)

    # 2. faturamento por categoria
    print("\n2. Faturamento por Categoria:")
    q2 = """
        SELECT 
            category,
            SUM(total_value) as faturamento_total
        FROM analytics_sales
        GROUP BY 1
        ORDER BY 2 DESC;
    """
    df2 = con.execute(q2).fetchdf()
    print(df2)

    # 3. quantidade de pedidos por estado
    print("\n3. Quantidade de Pedidos por Estado:")
    q3 = """
        SELECT 
            state,
            COUNT(DISTINCT order_id) as qtd_pedidos
        FROM analytics_sales
        GROUP BY 1
        ORDER BY 2 DESC;
    """
    df3 = con.execute(q3).fetchdf()
    print(df3)

    # 4. ticket médio por cliente
    print("\n4. Ticket Médio por Cliente:")
    # Calcula o total gasto por pedido e a média por cliente, 
    # ou diretamente o faturamento total daquele cliente sobre a qtde de pedidos dele
    q4 = """
        WITH cte_orders AS (
            SELECT 
                customer_id,
                order_id,
                SUM(total_value) as order_total
            FROM analytics_sales
            GROUP BY 1, 2
        )
        SELECT 
            AVG(order_total) as ticket_medio_geral
        FROM cte_orders;
    """
    df4 = con.execute(q4).fetchdf()
    print(df4)

    # 5. top 10 produtos mais vendidos
    print("\n5. Top 10 Produtos Mais Vendidos (em quantidade):")
    q5 = """
        SELECT 
            product_name,
            SUM(quantity) as qtd_vendida,
            SUM(total_value) as faturamento
        FROM analytics_sales
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10;
    """
    df5 = con.execute(q5).fetchdf()
    print(df5)
    
    # Salvar resultados em um TXT (evidência)
    with open("analysis_results.txt", "w", encoding="utf-8") as f:
        f.write("RELATORIO ANALITICO - E-COMMERCE\n\n")
        f.write("1. Faturamento Total por Mes:\n")
        f.write(df1.to_string())
        f.write("\n\n2. Faturamento por Categoria:\n")
        f.write(df2.to_string())
        f.write("\n\n3. Quantidade de Pedidos por Estado:\n")
        f.write(df3.to_string())
        f.write("\n\n4. Ticket Medio por Cliente (Geral):\n")
        f.write(df4.to_string())
        f.write("\n\n5. Top 10 Produtos Mais Vendidos (em quantidade):\n")
        f.write(df5.to_string())

def main():
    db_path = 'warehouse.duckdb'
    con = duckdb.connect(db_path)
    
    try:
        parse_analysis(con)
    except Exception as e:
        print(f"Erro na análise: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
