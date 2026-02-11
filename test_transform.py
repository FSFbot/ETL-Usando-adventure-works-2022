"""
Script para testar a transformação de dados.
"""

from src.extract import extract_all
from src.transform import transform_data


def test_transformation():
    """
    Testa a transformação até onde implementamos.
    """
    
    print("🧪 TESTANDO MÓDULO DE TRANSFORMAÇÃO\n")
    
    # Passo 1: Extrair dados
    print("📥 Extraindo dados...")
    data = extract_all()
    
    if not data:
        print("❌ Falha na extração. Abortando teste.")
        return
    
    # Passo 2: Transformar
    print("\n⚙️  Transformando dados...")
    transformed = transform_data(data)
    
    if transformed is not None:
        print("\n✅ TRANSFORMAÇÃO (PARCIAL) CONCLUÍDA!")
        print(f"   📊 Linhas resultantes: {len(transformed):,}")
        print(f"   📋 Colunas: {len(transformed.columns)}")
        print(f"\n   Primeiras colunas: {list(transformed.columns[:10])}")
        print(f"\n👀 Preview:")
        print(transformed.head(3))
    else:
        print("\n❌ Transformação falhou.")


if __name__ == "__main__":
    test_transformation()