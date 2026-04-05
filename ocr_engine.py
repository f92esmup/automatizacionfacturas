import os
import re
import logging
from typing import Dict, Any, List
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
OCR_MODE = os.getenv("OCR_MODE", "MOCK").upper()

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if OCR_MODE != "MOCK":
    try:
        import torch
        from PIL import Image
        from transformers import DonutProcessor, VisionEncoderDecoderModel
    except ImportError:
        logger.error("No se encontraron las librerías 'torch', 'transformers' o 'Pillow'.")
        OCR_MODE = "MOCK"

class OCRProcessor:
    """
    Motor extractor de información (KIE) utilizando tu modelo Donut personalizado.
    """

    def __init__(self):
        self.mode = OCR_MODE
        
        if self.mode != "MOCK":
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Entorno OCR Real activado. Dispositivo de cálculo: {self.device}")
            
            # CAMBIO: Ruta a tu modelo entrenado localmente
            self.model_id = "./donut_facturas_model" 
            
            try:
                logger.info(f"Cargando modelo PERSONALIZADO desde {self.model_id}...")
                self.processor = DonutProcessor.from_pretrained(self.model_id)
                self.model = VisionEncoderDecoderModel.from_pretrained(self.model_id).to(self.device)
                self.model.eval()
                logger.info("Modelo personalizado cargado con éxito en memoria.")
            except Exception as e:
                logger.error(f"Fallo crítico al cargar modelo local: {e}. Revirtiendo a MOCK.")
                self.mode = "MOCK"
        else:
            logger.info("Iniciando clase OCRProcessor en modo simulación (MOCK).")

    def procesar_ticket(self, image_path: str) -> Dict[str, Any]:
        if self.mode == "MOCK":
            return self._mock_processing(image_path)
        return self._real_processing(image_path)

    def _real_processing(self, image_path: str) -> Dict[str, Any]:
        logger.info(f"[{image_path}] - Iniciando inferencia con modelo personalizado.")
        
        try:
            image = Image.open(image_path).convert("RGB")
            
            # CAMBIO: El prompt para modelos fine-tuneados suele ser <s_gt_parse>
            task_prompt = "<s_gt_parse>"
            decoder_input_ids = self.processor.tokenizer(task_prompt, add_special_tokens=False, return_tensors="pt").input_ids
            pixel_values = self.processor(image, return_tensors="pt").pixel_values

            with torch.no_grad():
                outputs = self.model.generate(
                    pixel_values.to(self.device),
                    decoder_input_ids=decoder_input_ids.to(self.device),
                    max_length=self.model.decoder.config.max_position_embeddings,
                    pad_token_id=self.processor.tokenizer.pad_token_id,
                    eos_token_id=self.processor.tokenizer.eos_token_id,
                    use_cache=True,
                    bad_words_ids=[[self.processor.tokenizer.unk_token_id]],
                    return_dict_in_generate=True,
                )

            sequence = self.processor.batch_decode(outputs.sequences)[0]
            sequence = sequence.replace(self.processor.tokenizer.eos_token, "").replace(self.processor.tokenizer.pad_token, "")
            sequence = re.sub(r"<.*?>", "", sequence, count=1).strip()
            
            json_salida_crudo = self.processor.token2json(sequence)
            
            # El JSON ya viene con las claves que definiste (cif, proveedor, total...)
            return self._adaptar_estructura_salida(json_salida_crudo)

        except Exception as e:
            logger.error(f"ERROR DURANTE INFERENCIA: {e}")
            return self._mock_processing(image_path)

    def _adaptar_estructura_salida(self, raw_data: Any) -> Dict[str, Any]:
        """
        Adapta la salida del modelo a las claves que espera logic_mapper.py
        """
        # Si raw_data es un diccionario anidado (común en Donut), extraemos el contenido
        if isinstance(raw_data, dict) and "gt_parse" in raw_data:
            data = raw_data["gt_parse"]
        else:
            data = raw_data

        # Mapeamos a las claves definidas en tu export_to_donut.py y manual_labeler.py
        return {
            "cif": data.get("cif", "DESCONOCIDO"),
            "proveedor": data.get("proveedor", "Desconocido"),
            "numero_factura": data.get("numero_factura", ""),
            "serie": data.get("serie", ""),
            "fecha_expedicion": data.get("fecha_expedicion", "2024-01-01"),
            "fecha_operacion": data.get("fecha_operacion", ""),
            "total": data.get("total", 0.0),
            "impuestos": data.get("impuestos", [])  # Lista de dicts con base, pct_iva, cuota_iva...
        }

    def _mock_processing(self, image_path: str) -> Dict[str, Any]:
        return {
            "cif": "B12345678",
            "proveedor": "MOCK Technology S.L.",
            "numero_factura": "MOCK-001",
            "serie": "A",
            "fecha_expedicion": "2025-08-20",
            "fecha_operacion": "2025-08-20",
            "total": 121.00,
            "impuestos": [{"base": 100.00, "pct_iva": 21.0, "cuota_iva": 21.00}]
        }