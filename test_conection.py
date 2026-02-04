"""
Script de teste para validar conexão com SQL Server.
"""

from config.db_config import get_connection, get_connection_string_info


def test_basic_connection():
    """
    Teste 1: Valida se consegue conectar.
    """

    print("=" * 60)
    print("🔍 TESTE 1: Conexão Básica")
    print("=" * 60)

    # Mostra configuração
    info = get_connection_string_info()
    print("\n📋 Configuração:")
    for key, value in info.items():
        print(f"   {key.capitalize()}: {value}")

    print("\n🔄 Tentando conectar...")

    # Tenta conectar
    conn = get_connection()

    if conn:
        print("✅ Conexão estabelecida com sucesso!\n")
        conn.close()
        print("✅ Conexão fechada corretamente.")
        return True
    else:
        print("❌ Falha na conexão.\n")
        print("💡 Dicas para resolver:")
        print("   1. Verifique se SQL Server está rodando")
        print("   2. Confira nome do servidor em db_config.py")
        print("   3. Valide nome do banco (AdventureWorks2019)")
        print("   4. Tente conectar pelo SSMS primeiro")
        return False


def test_query_execution():
    """
    Teste 2: Valida se consegue executar queries.
    """

    print("\n" + "=" * 60)
    print("🔍 TESTE 2: Execução de Query")
    print("=" * 60)

    conn = get_connection()

    if not conn:
        print("❌ Pulando teste - conexão não estabelecida.")
        return False

    try:
        cursor = conn.cursor()

        # Query 1: Versão do SQL Server
        print("\n📊 Executando query de teste...")
        cursor.execute("SELECT @@VERSION AS Version")
        result = cursor.fetchone()
        version_info = result[0]

        print(f"✅ SQL Server Version:")
        print(f"   {version_info[:100]}...")

        # Query 2: Testa acesso ao banco
        print("\n📊 Testando acesso ao banco...")
        cursor.execute("""
                       SELECT COUNT(*) AS TotalTables
                       FROM INFORMATION_SCHEMA.TABLES
                       WHERE TABLE_TYPE = 'BASE TABLE'
                       """)

        result = cursor.fetchone()
        total_tables = result[0]

        print(f"✅ Tabelas encontradas no AdventureWorks: {total_tables}")

        cursor.close()
        conn.close()

        print("\n✅ Todos os testes de query passaram!")
        return True

    except Exception as e:
        print(f"\n❌ Erro ao executar query:")
        print(f"   {e}")

        if conn:
            conn.close()

        return False


def run_all_tests():
    """
    Executa todos os testes em sequência.
    """

    print("\n🚀 INICIANDO TESTES DE CONEXÃO")
    print("=" * 60)

    connection_ok = test_basic_connection()

    if not connection_ok:
        print("\n⚠️  Corrija a conexão antes de continuar.")
        return

    query_ok = test_query_execution()

    print("\n" + "=" * 60)
    if connection_ok and query_ok:
        print("🎉 TODOS OS TESTES PASSARAM!")
        print("✅ Você está pronto para começar o ETL.")
    else:
        print("⚠️  Alguns testes falharam. Revise as configurações.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_all_tests()