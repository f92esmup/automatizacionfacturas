import os
import json
import time
from ocr_engine import OCRProcessor

def run_benchmark():
    test_dir = "test_images"
    output_file = "resultados_test.json"
    ground_truth_file = "ground_truth.json"
    
    if not os.path.exists(test_dir):
        print(f"⚠️ El directorio {test_dir} no existe. Por favor, créalo y añade imágenes de prueba.")
        return

    imagenes = [f for f in os.listdir(test_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
    
    if not imagenes:
        print(f"⚠️ No se encontraron imágenes (.png, .jpg, .jpeg) en el directorio {test_dir}.")
        return

    print(f"🔄 Iniciando Benchmark OCR con {len(imagenes)} imágenes...\n")
    
    # Cargar Ground Truth si existe
    ground_truth = {}
    if os.path.exists(ground_truth_file):
        try:
            with open(ground_truth_file, 'r', encoding='utf-8') as f:
                ground_truth = json.load(f)
            print(f"📊 Ground Truth cargado con {len(ground_truth)} referencias.\n")
        except Exception as e:
            print(f"⚠️ No se pudo leer {ground_truth_file}: {e}\n")
    else:
         print(f"⚠️ Archivo {ground_truth_file} no encontrado. Se omitirá el cálculo de Precisión Real.\n")
    
    ocr_processor = OCRProcessor()
    resultados = []
    
    errores_absolutos = []
    cifs_correctos = 0
    total_evaluados = 0
    
    for i, img_name in enumerate(imagenes, 1):
        img_path = os.path.join(test_dir, img_name)
        print(f"[{i}/{len(imagenes)}] Procesando: {img_name}")
        
        start_time = time.perf_counter()
        
        try:
            resultado_ocr = ocr_processor.procesar_ticket(img_path)
            error_sistema = None
            
            # Evaluación contra el Ground Truth
            discrepancia_datos = False
            detalle_discrepancia = []
            
            if img_name in ground_truth:
                total_evaluados += 1
                gt_data = ground_truth[img_name]
                
                # Validar Total (Error Absoluto)
                gt_total = float(gt_data.get("total", 0.0))
                ocr_total = float(resultado_ocr.get("total", 0.0) if resultado_ocr.get("total") else 0.0)
                error_abs = abs(gt_total - ocr_total)
                errores_absolutos.append(error_abs)
                
                if error_abs > 0.01:
                    discrepancia_datos = True
                    detalle_discrepancia.append(f"Total esperado: {gt_total}, obtenido: {ocr_total}")
                
                # Validar CIF
                gt_cif = str(gt_data.get("cif", "")).strip().upper()
                ocr_cif = str(resultado_ocr.get("cif", "")).strip().upper()
                if gt_cif and gt_cif == ocr_cif:
                    cifs_correctos += 1
                elif gt_cif:
                    discrepancia_datos = True
                    detalle_discrepancia.append(f"CIF esperado: '{gt_cif}', obtenido: '{ocr_cif}'")
                    
        except Exception as e:
            resultado_ocr = {}
            error_sistema = str(e)
            discrepancia_datos = False
            detalle_discrepancia = []
            
        end_time = time.perf_counter()
        tiempo_segundos = round(end_time - start_time, 4)
        
        estado = "Éxito"
        if error_sistema:
            estado = "Error de sistema"
        elif discrepancia_datos:
            estado = "Discrepancia de datos"
            
        print(f"   Tardó {tiempo_segundos}s | Estado: {estado}")
        
        resultados.append({
            "archivo": img_name,
            "tiempo_segundos": tiempo_segundos,
            "estado": estado,
            "error_sistema": error_sistema,
            "discrepancia_datos": " | ".join(detalle_discrepancia) if discrepancia_datos else None,
            "datos_extraidos": resultado_ocr
        })

    # Guardar a JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, indent=4, ensure_ascii=False)
        
    tiempo_total = sum(r["tiempo_segundos"] for r in resultados)
    tiempo_promedio = round(tiempo_total / len(resultados), 4)

    print("\n✅ Benchmark Completado!")
    if total_evaluados > 0:
        mae = round(sum(errores_absolutos) / total_evaluados, 4)
        tasa_cif = round((cifs_correctos / total_evaluados) * 100, 2)
        print(f"📉 Métrica: Error Medio Absoluto (MAE) en Importes: {mae} €")
        print(f"🎯 Métrica: Tasa de Acierto de CIF: {tasa_cif}% ({cifs_correctos}/{total_evaluados})")
    else:
        print("ℹ️ No se calcularon métricas de error porque no hubo coincidencia con el ground_truth.")
        
    print(f"⏱️ Tiempo Medio por Inferencia: {tiempo_promedio} segundos")
    print(f"💾 Resultados guardados detalladamente en: {output_file}")

if __name__ == "__main__":
    run_benchmark()
