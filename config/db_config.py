"""
Módulo de configuração de conexão com SQL Server.
"""

import pyodbc


def get_connection():
    """
    Estabelece e retorna uma conexão ativa com SQL Server.

    Returns:
        pyodbc.Connection: Objeto de conexão ou None se falhar
    """

    # ============================================================
    # STRING DE CONEXÃO - AJUSTE CONFORME SEU AMBIENTE
    # ============================================================

    connection_string = (
        # Driver ODBC
        "DRIVER={ODBC Driver 18 for SQL Server};"
        
        # Servidor SQL - AJUSTE AQUI SE NECESSÁRIO
        # Opções comuns:
        # - "localhost" → SQL Server local padrão
        # - "localhost\\SQLEXPRESS" → SQL Express (note as duas barras)
        # - "192.168.1.100" → Servidor remoto
        "SERVER=localhost;"
        
        # Nome do banco - AJUSTE SE SEU BANCO TEM NOME DIFERENTE
        "DATABASE=AdventureWorks2022;"
        
        # Autenticação Windows
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )

    try:
        conn = pyodbc.connect(connection_string)
        print("✅ Conexão com SQL Server estabelecida!")
        return conn

    except pyodbc.Error as e:
        print(f"❌ Erro ao conectar com SQL Server:")
        print(f"   Detalhes: {e}")
        return None

    except Exception as e:
        print(f"❌ Erro inesperado:")
        print(f"   Detalhes: {e}")
        return None


def get_connection_string_info():
    """
    Retorna informações da configuração (para debug).
    """

    info = {
        "driver": "ODBC Driver 17 for SQL Server",
        "server": "localhost",
        "database": "AdventureWorks2019",
        "authentication": "Windows (Trusted Connection)"
    }

    return info
