from datetime import date, timedelta
from src.models import Invoice, TaxItem
from src.validator import validate_invoice

def test_date_validation():
    print("Ejecutando test de validación de fecha...")
    
    # Caso 1: Fecha reciente (no debería cambiar)
    fecha_reciente = date.today() - timedelta(days=30)
    invoice_ok = Invoice(
        cif="A12345678",
        proveedor="Proveedor OK",
        numero_factura="2024-001",
        fecha=fecha_reciente,
        total=121.0,
        impuestos=[TaxItem(base=100.0, tipo=21.0, cuota=21.0)]
    )
    
    validated_ok, warnings_ok = validate_invoice(invoice_ok)
    assert validated_ok.fecha_expedicion == fecha_reciente
    assert len(warnings_ok) == 0
    print("✅ Caso 1 (Fecha reciente) pasado.")

    # Caso 2: Fecha antigua (debería cambiarse a hoy)
    fecha_antigua = date.today() - timedelta(days=400)
    invoice_old = Invoice(
        cif="B87654321",
        proveedor="Proveedor Antiguo",
        numero_factura="2022-999",
        fecha=fecha_antigua,
        total=121.0,
        impuestos=[TaxItem(base=100.0, tipo=21.0, cuota=21.0)]
    )
    
    validated_old, warnings_old = validate_invoice(invoice_old)
    assert validated_old.fecha_expedicion == date.today()
    assert any("anterior a un año" in w for w in warnings_old)
    print("✅ Caso 2 (Fecha antigua) pasado.")

    # Caso 3: Fecha límite (exactamente un año atrás - no debería cambiar o sí según la lógica)
    # Nuestra lógica usa `hoy.replace(year=hoy.year - 1)`
    # Si hoy es 2024-04-23, el límite es 2023-04-23. 
    # Si la fecha es 2023-04-23, `invoice.fecha_expedicion < limite_un_anyo` es False.
    hoy = date.today()
    try:
        limite = hoy.replace(year=hoy.year - 1)
    except ValueError:
        limite = hoy.replace(year=hoy.year - 1, day=28)
        
    invoice_limit = Invoice(
        cif="C12345678",
        proveedor="Proveedor Límite",
        numero_factura="LIMIT-001",
        fecha=limite,
        total=121.0,
        impuestos=[TaxItem(base=100.0, tipo=21.0, cuota=21.0)]
    )
    
    validated_limit, warnings_limit = validate_invoice(invoice_limit)
    assert validated_limit.fecha_expedicion == limite
    print("✅ Caso 3 (Fecha límite) pasado.")

if __name__ == "__main__":
    try:
        test_date_validation()
        print("\nTodos los tests de validación de fecha pasaron correctamente.")
    except AssertionError as e:
        print(f"\n❌ Error en los tests: {e}")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
