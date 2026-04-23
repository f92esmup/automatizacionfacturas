import base64
import json
import logging
from pathlib import Path
from typing import Optional

from openai import OpenAI
from src.config import config
from src.models import Invoice

logger = logging.getLogger(__name__)

client = OpenAI(api_key=config.openai_api_key)

SYSTEM_PROMPT = """
Eres un experto contable español. Tu tarea es extraer información de facturas y tickets con precisión absoluta.
Debes devolver un objeto JSON que coincida con el siguiente esquema:
{
  "cif": "CIF del proveedor (ej: B12345678)",
  "proveedor": "Nombre fiscal del proveedor",
  "numero_factura": "Número o código de la factura",
  "fecha": "Fecha en formato YYYY-MM-DD",
  "total": 123.45,
  "impuestos": [
    {
      "base": 100.0,
      "tipo": 21.0,
      "cuota": 21.0
    }
  ]
}
Si hay varios tipos de IVA, lístalos todos en el array de impuestos. 
Si no detectas algún campo, intenta inferirlo o deja un valor razonable, pero nunca inventes datos si la imagen es ilegible.
Responde ÚNICAMENTE con el objeto JSON.
"""

def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def extract_invoice_data(image_path: str) -> Optional[Invoice]:
    """
    Envía la imagen a OpenAI Vision y parsea el resultado a un objeto Invoice.
    """
    try:
        base64_image = encode_image(image_path)
        
        response = client.chat.completions.create(
            model=config.openai_model,
            messages=[
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Extrae los datos de esta factura:"},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            response_format={"type": "json_object"},
            max_completion_tokens=1000
        )

        raw_json = response.choices[0].message.content
        logger.info(f"Respuesta cruda del LLM: {raw_json}")
        
        data = json.loads(raw_json)
        return Invoice(**data)

    except Exception as e:
        logger.error(f"Error extrayendo datos con el LLM: {e}")
        return None
