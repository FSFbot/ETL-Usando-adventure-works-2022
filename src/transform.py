import pandas as pd
import numpy as np
from datetime import datetime

PERCENTIL_A = 95
PERCENTIL_B = 80
YEARS_TO_ANALYZE = 2


def transform_data(data):
    """
    Transforma os dados extraídos em tabela analítica.
    """
    
    print(f"\n" + "="*60)
    print("  INICIANDO A TRANSFORMAÇÃO DE DADOS")
    print("="*60)

    try:
        
        print("\n STEP 1: Carregando DataFrames...")

        sales_detail = data['sales_detail']
        sales_header = data['sales_header']
        products = data['products']

        print(f"    Sales Detail: {len(sales_detail):,} linhas")
        print(f"    Sales Header: {len(sales_header):,} linhas")
        print(f"    Products: {len(products):,} linhas")


        
        print(f"\n STEP 2: JOIN Sales Detail + Sales Header...")

        sales = pd.merge(
            sales_detail,
            sales_header,
            on='SalesOrderID',
            how='inner'
        )

        print(f"    Resultado: {len(sales):,} linhas")

        if len(sales) != len(sales_detail):
            print(f"     Atenção: {len(sales_detail) - len(sales):,} registros órfãos")

        print(f"    Colunas após JOIN: {len(sales.columns)} colunas")

        
        print(f"\n STEP 3: JOIN Sales + Products...")

        sales = pd.merge(
            sales,
            products,
            on='ProductID',
            how='left'
        )
        
        print(f"    Resultado: {len(sales):,} linhas")
        print(f"    Colunas após segundo JOIN: {len(sales.columns)} colunas")
        
        # Validação
        produtos_sem_info = sales['ProductName'].isna().sum()
        if produtos_sem_info > 0:
            print(f"     {produtos_sem_info:,} vendas de produtos sem cadastro")
        else:
            print(f"    Todos os produtos têm informação!")

        
        print(f"\n STEP 4: Filtrando últimos {YEARS_TO_ANALYZE} anos...")
        
        # Data mais recente
        max_date = sales['OrderDate'].max()
        print(f"    Data mais recente: {max_date}")
        
        # Data de corte
        cutoff_date = max_date - pd.DateOffset(years=YEARS_TO_ANALYZE)
        print(f"    Data de corte: {cutoff_date}")
        
        # Filtrar
        linhas_antes = len(sales)
        sales = sales[sales['OrderDate'] >= cutoff_date]
        linhas_depois = len(sales)
        linhas_removidas = linhas_antes - linhas_depois
        
        print(f"    Resultado: {linhas_depois:,} linhas")
        print(f"    Removidas: {linhas_removidas:,} linhas ({linhas_removidas/linhas_antes*100:.1f}%)")

        print(f"\n STEP 5: Agregando dados por produto...")

        products_metrics = sales.groupby('ProductID').agg({
            'LineTotal': 'sum',          
            'OrderQty': 'sum',            
            'UnitPrice': 'mean',         
            'OrderDate': 'max',           
            'ProductName': 'first',       
            'ListPrice': 'first',         
            'StandardCost': 'first',      
            'SalesOrderID': 'count'       
        }).reset_index()

        products_metrics.columns = [
            'ProductID',
            'TotalSales',
            'QtySold',           
            'AvgUnitPrice',
            'LastSaleDate',      
            'ProductName',
            'ListPrice',
            'StandardCost',
            'NumOrders'
        ]

        print(f"Agregação concluída!!!")
        print(f"    Produtos únicos: {len(products_metrics):,}")
        print(f"Redução: {len(sales):,} linhas -> {len(products_metrics):,} linhas")


        print(f"\n STEP Calculando métricas adicionais...")

        products_metrics['AvgTicket'] = (products_metrics['TotalSales'] / products_metrics['NumOrders']).round(2)

        products_metrics['GrossMargin'] = np.where(
            products_metrics['ListPrice'] > 0,
            ((products_metrics['ListPrice'] - products_metrics['StandardCost']) / 
             products_metrics['ListPrice'] * 100),
            np.nan  # NaN se ListPrice = 0
        )

        products_metrics['AvgQtyPerOrder'] = (
            products_metrics['QtySold'] / products_metrics['NumOrders']
        )
        
        print(f"   ✅ Métricas calculadas:")
        print(f"      • Ticket Médio (AvgTicket)")
        print(f"      • Margem Bruta % (GrossMargin)")
        print(f"      • Qtd Média por Pedido (AvgQtyPerOrder)")

        print(f"\n STEP 6: Classificando performace dos produtos...")
        p95 = products_metrics['TotalSales'].quantile(PERCENTIL_A / 100)
        p80 = products_metrics['TotalSales'].quantile(PERCENTIL_B / 100)

        print(f" Percentil 95: ${p95:,.2f}")
        print(f" Percentil 80: ${p80:,.2f}")     

        products_metrics['Performance'] = np.where(
            products_metrics['TotalSales'] >= p95,  
            'A',                                     
            np.where(
                products_metrics['TotalSales'] >= p80,  
                'B',                                    
                'C'                                     
            )
        )  

        print("Classificação concluida: ")
        class_counts = products_metrics['Performance'].value_counts().sort_index()
        for classe, count in class_counts.items():
            percent = count / len(products_metrics) * 100
            print(f"   • Classe {classe}: {count:,} produtos ({percent:.2f}%)")
        
        print(f"\nSTEP 7: Finalizando transformação...")

        product_metrics = products_metrics.sort_values(
            'TotalSales', 
            ascending=False  # Descendente (maior primeiro)
        ).reset_index(drop=True)

        product_metrics = products_metrics.sort_values(
            'TotalSales', 
            ascending=False  # Descendente (maior primeiro)
        ).reset_index(drop=True)
        
        print(f"   ✅ Dados ordenados por TotalSales")
        print(f"   ✅ Valores numéricos arredondados")
        
        # Estatísticas finais
        print(f"\n📈 ESTATÍSTICAS FINAIS:")
        print(f"   💰 Total de Vendas: ${products_metrics['TotalSales'].sum():,.2f}")
        print(f"   📦 Total Produtos Vendidos: {products_metrics['QtySold'].sum():,}")
        print(f"   🏆 Top Produto: {products_metrics.iloc[0]['ProductName']}")
        print(f"      └─ Vendas: ${products_metrics.iloc[0]['TotalSales']:,.2f}")
        
        print(f"\n{'='*60}")
        print(f"✅ TRANSFORMAÇÃO COMPLETA!")
        print(f"{'='*60}")
        
        return products_metrics
    
    except Exception as e:
        print(f"\n ERRO na transformação:")
        print(f"   Tipo: {type(e).__name__}")
        print(f"   Mensagem: {e}")
        
        import traceback
        print(f"\n🔍 Traceback completo:")
        traceback.print_exc()
        
        return None