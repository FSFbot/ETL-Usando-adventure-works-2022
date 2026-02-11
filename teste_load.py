"""
Script para testar a carga de dados.
"""

from src.extract import extract_all
from src.transform import transform_data
from src.load import load_data, validate_load


def test_full_etl():
    """Testa o ETL completo (Extract, Transform, Load)."""
    
    print("🧪 TESTANDO ETL COMPLETO\n")
    
    print("FASE 1: EXTRAÇÃO")
    print("="*60)
    data = extract_all()
    
    if not data:
        print(" Falha na extração. Abortando.")
        return
    
    print("\n  FASE 2: TRANSFORMAÇÃO")
    print("="*60)
    transformed = transform_data(data)
    
    if transformed is None:
        print(" Falha na transformação. Abortando.")
        return
    
    print("\n FASE 3: CARGA")
    print("="*60)
    success = load_data(transformed)
    
    if not success:
        print(" Falha na carga.")
        return
    
    print("\n FASE 4: VALIDAÇÃO")
    print("="*60)
    stats = validate_load()
    
    if stats:
        print("\n" + "="*60)
        print("ETL COMPLETO EXECUTADO COM SUCESSO!")
        print("="*60)
    else:
        print("\n  ETL concluído mas validação falhou.")


if __name__ == "__main__":
    test_full_etl()