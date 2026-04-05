import os
import json
from pathlib import Path
from datasets import load_dataset
from transformers import DonutProcessor, VisionEncoderDecoderModel, Seq2SeqTrainer, Seq2SeqTrainingArguments
import torch

# 1. Configuración de rutas y tokens
DATASET_PATH = "donut_dataset" # Carpeta generada por tu exportador
MODEL_BASE = "naver-clova-ix/donut-base"
OUTPUT_DIR = "./donut_facturas_model"

# Tokens basados en tu lógica de manual_labeler
NEW_TOKENS = [
    "<s_cif>", "<s_proveedor>", "<s_numero_factura>", "<s_serie>", 
    "<s_fecha_expedicion>", "<s_fecha_operacion>", "<s_total>", 
    "<s_impuestos>", "<s_base>", "<s_pct_iva>", "<s_cuota_iva>",
    "<s_pct_receq>", "<s_cuota_receq>"
]

# 2. Cargar Procesador y Modelo
processor = DonutProcessor.from_pretrained(MODEL_BASE)
model = VisionEncoderDecoderModel.from_pretrained(MODEL_BASE)

# Añadir tokens especiales al vocabulario
processor.tokenizer.add_tokens(NEW_TOKENS)
model.config.pad_token_id = processor.tokenizer.pad_token_id
model.config.decoder_start_token_id = processor.tokenizer.convert_tokens_to_ids(['<s_gt>'])[0]
model.decoder.resize_token_embeddings(len(processor.tokenizer))

# 3. Cargar Dataset
dataset = load_dataset("imagefolder", data_dir=DATASET_PATH, split="train")

def preprocess_function(sample):
    # --- Imagen ---
    pixel_values = processor(sample["image"], return_tensors="pt").pixel_values

    # --- Ground truth ---
    # ground_truth es un JSON string con wrapper {"gt_parse": {...}}
    gt_js = json.loads(sample["ground_truth"])["gt_parse"]

    # Construimos la secuencia objetivo directamente desde el dict
    # Donut espera: <s_gt_parse>{...campos...}</s_gt_parse>
    target_sequence = json.dumps(gt_js, ensure_ascii=False)

    input_ids = processor.tokenizer(
        target_sequence,
        add_special_tokens=False,
        max_length=512,
        truncation=True,
        padding="max_length",
        return_tensors="pt",
    ).input_ids.squeeze()

    # Enmascarar padding para que no entre en el cálculo del loss
    labels = input_ids.clone()
    labels[labels == processor.tokenizer.pad_token_id] = -100

    return {"pixel_values": pixel_values.squeeze(), "labels": labels}

processed_dataset = dataset.map(
    preprocess_function,
    remove_columns=dataset.column_names,  # elimina image, ground_truth, file_name → solo quedan tensores
)

# 4. Argumentos de Entrenamiento
training_args = Seq2SeqTrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=2,
    gradient_accumulation_steps=4,
    learning_rate=2e-5,
    num_train_epochs=30,
    save_steps=100,
    logging_steps=10,
    fp16=torch.cuda.is_available(),
    push_to_hub=False,
    remove_unused_columns=False,
)

# 5. Entrenar
trainer = Seq2SeqTrainer(
    model=model,
    args=training_args,
    train_dataset=processed_dataset,
    processing_class=processor,   # transformers >= 4.46: tokenizer → processing_class
)

trainer.train()

# 6. Guardar modelo final
model.save_pretrained(OUTPUT_DIR)
processor.save_pretrained(OUTPUT_DIR)
print(f"Modelo guardado en {OUTPUT_DIR}")