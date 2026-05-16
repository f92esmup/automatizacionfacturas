import os
import json
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

# Cargar variables de entorno (API Key de OpenAI)
load_dotenv()

# Inicializar cliente de OpenAI
client = OpenAI()

prompt = """
Eres un asistente que genera datos de entrenamiento para un modelo de Procesamiento del Lenguaje Natural (PLN).
El dominio es la gestión de facturas en la hostelería (bares, restaurantes). El usuario típico es el gestor del bar que no es técnico y tiene prisa.

Necesito que generes 25 ejemplos de frases (consultas) orgánicas para cada una de las siguientes 10 intenciones. 

Las frases deben:
- Ser informales, de lenguaje oral transcrito, y directas.
- Contener errores ortográficos o tipográficos típicos (ausencia de tildes, "q", "k", "xq", "kilo", "q gasto", "provbedor", etc.).
- Incluir variaciones y metonimias (ej. "el de los corderos", "los de la cocacola", "el carnicero", "el paco").
- Contener esporádicamente algo de code-switching con valenciano o expresiones coloquiales (ej. "nano", "xe", "quina factura", "cuant demane").
- Ser muy variadas en longitud. Algunas elípticas o muy cortas (ej. "¿y las de ayer?").

Intenciones a generar:
1. CONSULTA_ULTIMA_FACTURA: Preguntar por la factura más reciente de un proveedor.
2. CONSULTA_GASTO_TOTAL: Preguntar cuánto se ha gastado en total en un periodo y/o proveedor.
3. CONSULTA_FACTURAS_POR_IMPORTE: Buscar facturas por una cantidad exacta o aproximada de dinero.
4. CONSULTA_FACTURA_POR_NUMERO: Buscar una factura específica por su número, código o referencia.
5. PREDICCION_GASTO_PROVEEDOR: Preguntar cuánto se prevé gastar con un proveedor en concreto a futuro.
6. PREVISION_MENSUAL_AGREGADA: Preguntar por la previsión general de gastos para el mes que viene (todos los proveedores).
7. DETECCION_ANOMALIAS_GASTO: Preguntar si hay algún gasto raro, pico de gasto repentino, o si nos hemos pasado este mes.
8. ANALISIS_TENDENCIA_PRECIOS: Preguntar cómo ha evolucionado el precio de un proveedor o si el ticket medio ha subido.
9. ANALISIS_ESTACIONALIDAD: Preguntar por picos históricos dependiendo de la época (ej. en verano, en fallas, en pascua).
10. RESUMEN_HISTORICO_PROVEEDOR: Pedir un resumen general o histórico con un proveedor (frecuencia de compra, resumen del año).

Devuelve el resultado ÚNICAMENTE en formato JSON estricto con la siguiente estructura:
{
  "dataset": [
    {"text": "texto de la frase", "intent": "NOMBRE_DE_LA_INTENCION"},
    ...
  ]
}
"""

print("Llamando a la API de OpenAI para generar el dataset (2 iteraciones)...")

all_data = []

try:
    for i in range(2):
        print(f"Iteración {i+1}...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={ "type": "json_object" },
            temperature=0.7 + (i * 0.1), # Variar ligeramente la temperatura
            max_tokens=4000,
            messages=[
                {"role": "system", "content": prompt}
            ]
        )

        content = response.choices[0].message.content
        data = json.loads(content)
        all_data.extend(data['dataset'])
    
    df_new = pd.DataFrame(all_data)
    
    # Crear directorio si no existe
    os.makedirs('data', exist_ok=True)
    
    output_path = 'data/synthetic_dataset.csv'
    
    if os.path.exists(output_path):
        df_old = pd.read_csv(output_path)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new
        
    # Guardar a CSV
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Éxito: Dataset actualizado y guardado en {output_path}. Total de registros: {len(df)} (Nuevos: {len(df_new)})")
    
    # Mostrar conteo por clase para verificar que están balanceadas
    print("\nDistribución por clase:")
    print(df['intent'].value_counts())

except Exception as e:
    print(f"Error al generar los datos: {e}")
