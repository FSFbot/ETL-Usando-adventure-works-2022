import pandas as pd 
from datetime import datetime
from config.db_config import get_connection


def load_data(df, table_name="Analytics.ProductSalesMetrics", truncate=True):
    """
    Carrega DataFrame no SQL Server.
    
    Args:
        df (pd.DataFrame): DataFrame com dados transformados
        table_name (str): Nome completo da tabela (schema.table)
        truncate (bool): Se True, limpa tabela antes de inserir
    
    Returns:
        bool: True se sucesso, False se falhar
    """
    
    print(f"\n{'='*60}")
    print(f" INICIANDO CARGA DE DADOS")
    print(f"{'='*60}")
    
    conn = get_connection()
    
    if not conn:
        print(f" Falha ao conectar. Carga abortada.")
        return False
    
    try:
        cursor = conn.cursor()
        
        if truncate:
            print(f"\n  Limpando tabela {table_name}...")
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            print(f" Tabela limpa")
        
        print(f"\nPreparando dados para inserção...")
        print(f"   📦 Linhas a inserir: {len(df):,}")
        
        df_copy = df.copy()
        df_copy['ProcessedAt'] = datetime.now()
        
        insert_query = f"""
            INSERT INTO {table_name} (
                ProductID, TotalSales, QtySold, AvgUnitPrice, 
                LastSaleDate, ProductName, ListPrice, StandardCost,
                NumOrders, AvgTicket, GrossMargin, AvgQtyPerOrder,
                Performance, ProcessedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        print(f"\n Inserindo dados...")
        
        rows_inserted = 0
        batch_size = 100
        
        for i in range(0, len(df_copy), batch_size):
            batch = df_copy.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                cursor.execute(
                    insert_query,
                    row['ProductID'],
                    row['TotalSales'],
                    int(row['QtySold']),
                    row['AvgUnitPrice'],
                    row['LastSaleDate'],
                    row['ProductName'],
                    row['ListPrice'],
                    row['StandardCost'],
                    int(row['NumOrders']),
                    row['AvgTicket'],
                    row['GrossMargin'] if pd.notna(row['GrossMargin']) else None,
                    row['AvgQtyPerOrder'],
                    row['Performance'],
                    row['ProcessedAt']
                )
                rows_inserted += 1
            
            conn.commit()
            
            progress = (i + len(batch)) / len(df_copy) * 100
            print(f"    Progresso: {progress:.1f}% ({i + len(batch):,}/{len(df_copy):,})")
        
        print(f"\n Carga concluída!")
        print(f"  Linhas inseridas: {rows_inserted:,}")
        
        print(f"\n🔍 Validando dados carregados...")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        print(f"  Linhas na tabela: {count:,}")
        
        if count == len(df):
            print(f"Validação OK - todos os dados foram carregados!")
        else:
            print(f"Atenção: Esperado {len(df):,}, encontrado {count:,}")
        
        cursor.close()
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"CARGA COMPLETA!")
        print(f"{'='*60}")
        
        return True
        
    except Exception as e:
        print(f"\nERRO na carga:")
        print(f"   Tipo: {type(e).__name__}")
        print(f"   Mensagem: {e}")
        
        import traceback
        print(f"\n Traceback completo:")
        traceback.print_exc()
        
        if conn:
            conn.rollback()
            conn.close()
        
        return False


def validate_load(table_name="Analytics.ProductSalesMetrics"):
    """
    Valida os dados carregados na tabela.
    
    Args:
        table_name (str): Nome completo da tabela
    
    Returns:
        dict: Dicionário com estatísticas ou None se falhar
    """
    
    print(f"\n{'='*60}")
    print(f"VALIDANDO DADOS CARREGADOS")
    print(f"{'='*60}")
    
    conn = get_connection()
    
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT 
                COUNT(*) as TotalRows,
                SUM(TotalSales) as TotalSalesSum,
                SUM(QtySold) as TotalQtySold,
                COUNT(CASE WHEN Performance = 'A' THEN 1 END) as ClassA,
                COUNT(CASE WHEN Performance = 'B' THEN 1 END) as ClassB,
                COUNT(CASE WHEN Performance = 'C' THEN 1 END) as ClassC,
                MAX(ProcessedAt) as LastProcessedAt
            FROM {table_name}
        """)
        
        result = cursor.fetchone()
        
        stats = {
            'total_rows': result[0],
            'total_sales': result[1],
            'total_qty': result[2],
            'class_a': result[3],
            'class_b': result[4],
            'class_c': result[5],
            'processed_at': result[6]
        }
        
        print(f"\n ESTATÍSTICAS DA TABELA:")
        print(f"   Total de Produtos: {stats['total_rows']:,}")
        print(f"    Total de Vendas: ${stats['total_sales']:,.2f}")
        print(f"    Total Quantidade: {stats['total_qty']:,}")
        print(f"\n Classificação:")
        print(f"   • Classe A: {stats['class_a']:,} produtos")
        print(f"   • Classe B: {stats['class_b']:,} produtos")
        print(f"   • Classe C: {stats['class_c']:,} produtos")
        print(f"\n Processado em: {stats['processed_at']}")
        
        cursor.execute(f"""
            SELECT TOP 5 
                ProductName, 
                TotalSales, 
                Performance
            FROM {table_name}
            ORDER BY TotalSales DESC
        """)
        
        print(f"\n🏆 TOP 5 PRODUTOS:")
        for row in cursor.fetchall():
            print(f"   • {row[0]}: ${row[1]:,.2f} (Classe {row[2]})")
        
        cursor.close()
        conn.close()
        
        print(f"\n{'='*60}")
        print(f" VALIDAÇÃO COMPLETA!")
        print(f"{'='*60}")
        
        return stats
        
    except Exception as e:
        print(f"\n ERRO na validação:")
        print(f"   {e}")
        
        if conn:
            conn.close()
        
        return None