# ETL de inventarios para pymes · Variedades Aurora S.A.S.

Proceso ETL que consolida las ventas, el inventario y los proveedores de una pyme de Medellín en un data mart listo para tomar decisiones. Proyecto de Minería de Datos, Escolme, 2026-2.

**Equipo:** Jefferson Manrique · Christian Varela

## El problema

Variedades Aurora vende por Instagram, WhatsApp, su página web y su tienda física. Lleva las ventas, el inventario y los proveedores en hojas separadas, con registros duplicados, ventas sin precio y categorías escritas de varias formas. Cada cierre de mes alguien cuadra todo a mano.

## La solución

| Zona | Carpeta | Qué contiene |
|---|---|---|
| Landing | `data/raw/` | Los tres CSV tal como llegan, sin modificar |
| Staging | `data/staging/` | Copias con tipos validados (números, fechas, texto) |
| Transformación | `etl/etl_inventarios.py` | Reglas R1 a R6 de limpieza, normalización e integración |
| Carga | `data/mart/` | Data mart en estrella y vista consolidada |
| Auditoría | `reports/`, `data/rechazados/` | Calidad antes y después, bitácora y registros descartados |

Las carpetas `data/staging/`, `data/mart/`, `data/rechazados/` y `reports/` se regeneran en cada ejecución a partir de los datos de landing.

El mismo proceso está construido en Orange Data Mining en `ETL_Variedades_Aurora.ows`. Orange lee los CSV directamente desde este repositorio y guarda el resultado en `data/mart/data_mart_orange.csv`.

## Resultado

| Tabla | Filas antes | Filas después | Nulos antes → después | Duplicados antes → después |
|---|---|---|---|---|
| Ventas | 156 | 142 | 8 → 0 | 6 → 0 |
| Inventario | 53 | 48 | 9 → 0 | 5 → 0 |
| Proveedores | 6 | 6 | 0 → 0 | 0 → 0 |

## Cómo ejecutarlo

```bash
pip install -r requirements.txt
python etl/etl_inventarios.py --fuente data/raw
```

Para leer los datos directamente desde GitHub:

```bash
python etl/etl_inventarios.py --fuente https://raw.githubusercontent.com/jamanriquen/etl-inventarios-pymes/main/data/raw
```

## URLs para el widget File de Orange

- https://raw.githubusercontent.com/jamanriquen/etl-inventarios-pymes/main/data/raw/ventas.csv
- https://raw.githubusercontent.com/jamanriquen/etl-inventarios-pymes/main/data/raw/inventario.csv
- https://raw.githubusercontent.com/jamanriquen/etl-inventarios-pymes/main/data/raw/proveedores.csv

## Documentación

- [Mapeo de datos](docs/mapeo_datos.md)
- [Auditoría de calidad](reports/auditoria_calidad.csv)
- [Bitácora de la última ejecución](reports/bitacora_etl.log)

## Seguridad

Los datos de este repositorio son de demostración y no contienen información personal. En producción, los datos reales de la empresa irían en un repositorio privado o en una carpeta con acceso restringido. Las zonas `raw` y `mart` se separan para que nadie modifique el origen, y el ETL nunca escribe sobre los archivos de landing.
