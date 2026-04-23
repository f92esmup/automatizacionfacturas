from typing import List, Tuple
from src.models import Invoice

def validate_invoice(invoice: Invoice) -> Tuple[Invoice, List[str]]:
    warnings = []
    # Tipos de IVA legales en España (incluimos 0% para exentos/recargo de equivalencia se maneja aparte si fuera necesario)
    TIPOS_IVA_LEGALES = [21.0, 10.0, 4.0, 0.0]
    
    # 1. Filtrar impuestos con tipos no legales (errores de la IA)
    impuestos_filtrados = []
    for imp in invoice.impuestos:
        if imp.porcentaje_iva in TIPOS_IVA_LEGALES:
            impuestos_filtrados.append(imp)
        else:
            warnings.append(f"Se eliminó impuesto con tipo no legal detectado por IA: {imp.porcentaje_iva}%")
    
    invoice.impuestos = impuestos_filtrados

    # 2. Asegurar que las cuotas de los impuestos válidos sean matemáticamente correctas
    for imp in invoice.impuestos:
        expected_cuota = round(imp.base_imponible * (imp.porcentaje_iva / 100), 2)
        if abs(imp.cuota_iva - expected_cuota) > 0.02: # Margen pequeño por redondeos
            imp.cuota_iva = expected_cuota

    # 3. El Total es la fuente de verdad
    calc_total = sum(round(imp.base_imponible + imp.cuota_iva, 2) for imp in invoice.impuestos)
    calc_total = round(calc_total, 2)
    diff = round(abs(calc_total - invoice.importe_total), 2)

    if diff > 0.05:
        # CASO A: Un solo impuesto -> Ajustamos base e iva al total
        if len(invoice.impuestos) == 1:
            imp = invoice.impuestos[0]
            # Recalcular base: Total / (1 + i/100)
            nueva_base = round(invoice.importe_total / (1 + imp.porcentaje_iva / 100), 2)
            nueva_cuota = round(invoice.importe_total - nueva_base, 2)
            
            warnings.append(f"Ajuste automático al total ({invoice.importe_total}€). Base: {imp.base_imponible}->{nueva_base}, IVA: {imp.cuota_iva}->{nueva_cuota}")
            imp.base_imponible = nueva_base
            imp.cuota_iva = nueva_cuota
        
        # CASO B: Varios impuestos y no cuadra -> Marcamos para revisión
        elif len(invoice.impuestos) > 1:
            warnings.append(f"El total ({invoice.importe_total}€) no coincide con el sumatorio de impuestos ({calc_total}€). Se marca para REVISIÓN.")
            invoice.requiere_revision = 1
        
        # CASO C: No hay impuestos detectados pero hay un total
        else:
            warnings.append("No se detectaron impuestos válidos. Requiere revisión.")
            invoice.requiere_revision = 1

    return invoice, warnings
