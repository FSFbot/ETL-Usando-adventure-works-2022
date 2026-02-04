"""
Script para testar a extração de dados.
"""

from src.extract import extract_all


def test_extraction():
    """
    Testa se a extração está funcionando.
    """

    print("🧪 TESTANDO MÓDULO DE EXTRAÇÃO\n")

    # Extrai todas as tabelas
    data = extract_all()

    if data:
        print("\n📊 RESUMO DOS DADOS EXTRAÍDOS:")
        print(f"   - Sales Detail: {len(data['sales_detail']):,} linhas")
        print(f"   - Sales Header: {len(data['sales_header']):,} linhas")
        print(f"   - Products: {len(data['products']):,} linhas")

        # Mostra colunas de cada DataFrame
        print("\n📋 COLUNAS EXTRAÍDAS:")
        for name, df in data.items():
            print(f"\n   {name}:")
            print(f"   {list(df.columns)}")

    else:
        print("\n❌ Extração falhou.")


if __name__ == "__main__":
    test_extraction()