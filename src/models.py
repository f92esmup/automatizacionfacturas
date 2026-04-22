from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from datetime import date
import re

class TaxItem(BaseModel):
    base_imponible: float = Field(default=0.0, alias="base")
    porcentaje_iva: float = Field(default=0.0, alias="tipo")
    cuota_iva: float = Field(default=0.0, alias="cuota")
    porcentaje_receq: float = 0.0
    cuota_receq: float = 0.0

class Invoice(BaseModel):
    cif_proveedor: str = Field(alias="cif")
    proveedor_nombre: str = Field(alias="proveedor")
    numero_registro: str = Field(alias="numero_factura")
    serie: str = "1"
    su_factura: Optional[str] = None
    fecha_expedicion: date = Field(alias="fecha")
    fecha_operacion: Optional[date] = None
    importe_total: float = Field(alias="total")
    comentario_sii: str = ""
    hash_archivo: Optional[str] = None
    requiere_revision: int = 0
    impuestos: List[TaxItem] = []

    @field_validator("cif_proveedor")
    @classmethod
    def clean_cif(cls, v: str) -> str:
        return re.sub(r'[^A-Z0-9]', '', v.upper())

    @field_validator("fecha_operacion", mode="before")
    @classmethod
    def set_fecha_operacion(cls, v, info):
        if v is None:
            return info.data.get("fecha_expedicion")
        return v
    
    def model_post_init(self, __context) -> None:
        if not self.su_factura:
            self.su_factura = self.numero_registro
        if not self.fecha_operacion:
            self.fecha_operacion = self.fecha_expedicion
