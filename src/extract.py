"""
Módulo de extração de dados do SQL Server.

Responsabilidade:
- Conectar ao banco
- Extrair dados das tabelas necessárias
- Retornar DataFrames do Pandas
"""

import pandas as pd
from config.db_config import get_connection


# ============================================================
# CONFIGURAÇÃO DAS QUERIES
# ============================================================

# Por que definir queries como constantes no topo?
# - Facilita manutenção (todas em um lugar)
# - Reutilizável em múltiplas funções
# - Mais fácil de testar e debugar

QUERY_SALES_DETAIL = """
    SELECT 
        SalesOrderID,
        SalesOrderDetailID,
        ProductID,
        OrderQty,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal
    FROM 
        Sales.SalesOrderDetail
"""

QUERY_SALES_HEADER = """
    SELECT 
        SalesOrderID,
        OrderDate,
        DueDate,
        ShipDate,
        Status,
        CustomerID,
        TerritoryID,
        SubTotal,
        TaxAmt,
        Freight,
        TotalDue
    FROM 
        Sales.SalesOrderHeader
"""

QUERY_PRODUCTS = """
    SELECT 
        ProductID,
        Name AS ProductName,
        ProductNumber,
        Color,
        StandardCost,
        ListPrice,
        Size,
        ProductSubcategoryID
    FROM 
        Production.Product
"""

def extract_data(query, table_name = "tabela"):
    print(f"\n{'='*60}")
    print(f"EXTRAINDO: {table_name}")
    print(f"\n{'=' * 60}")

    conn = get_connection()
    if not conn:
        print(f"Extração falhou ao conectar. Extração {table_name} abortada")
        return None

    try:
        print(f"Excultando Query...")
        data = pd.read_sql(query, conn)

        num_rows = len(data)
        num_cols = len(data.columns)

        print("Parabens Extração concluida")
        print(f"Linhas: {num_rows:,}")
        print(f"Colunas: {num_cols}")
        print(f"Memoria {data.memory_usage(deep=True).sum()/1024**2:.2f} MB")

        print(f"\n Prévia das linhas que estamos vendo")
        print(data.head(5))

        conn.close()
        print(f"Conexão fechada")

        return  data
    except Exception as e:
        print(f"\n Erro ao extrair dados da tabela {table_name}")
        print(f" {e}")
        if conn:
            conn.close()
            print(f"Conexão encerrada após o erro")
        return None

def extract_sales_detail():
    return extract_data(QUERY_SALES_DETAIL, "Sales.SalesOrderDetail")

def extract_sales_header():
    return extract_data(QUERY_SALES_HEADER, "Sales.SalesOrderHeader")

def extract_products():
    return extract_data(QUERY_PRODUCTS, "Production.Production")

def extract_all():
    print("\n" + "="*60)
    print("Iniciando uma extração de todas as tabelas: ")

    sales_detail = extract_sales_detail()
    sales_header = extract_sales_header()
    products = extract_products()

    if sales_detail is None or sales_header is None or products is None:
        print("Algumas extraxões falharam processo abortado")
        return None

    data = {
        "sales_detail": sales_detail,
        "sales_header": sales_header,
        "products": products
    }
    print("\n" + "=" * 60)
    print("Todas as extrações concluidas com sucesso")
    print("\n" + "=" * 60)

    return data