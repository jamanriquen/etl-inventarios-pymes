"""
ETL de inventarios y ventas - Variedades Aurora S.A.S.

Consolida tres fuentes (ventas, inventario y proveedores) en un data mart
con esquema en estrella y genera un reporte de calidad de datos antes y
después de la transformación.

Zonas del proceso
  1. Landing         data/raw/          CSV tal como llegan de cada fuente (no se modifican)
  2. Staging         data/staging/      copias tipadas y validadas contra el esquema esperado
  3. Transformación  reglas R1 a R6     limpieza, normalización, integración y validación
  4. Carga           data/mart/         tabla de hechos y dimensiones del data mart
  Auditoría          reports/           calidad antes/después y bitácora de ejecución
  Cuarentena         data/rechazados/   registros descartados con el motivo

Uso
  python etl/etl_inventarios.py                      # lee data/raw del repositorio
  python etl/etl_inventarios.py --fuente <url|ruta>  # lee los CSV desde otra carpeta o URL

Requisitos: Python 3.9+ y pandas.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent

# Esquema esperado de cada fuente: columna -> tipo de destino
ESQUEMAS = {
    "ventas": {"id_venta": "str", "fecha": "date", "id_producto": "str",
               "cantidad": "int", "precio_venta": "float", "canal": "str"},
    "inventario": {"id_producto": "str", "nombre_producto": "str", "categoria": "str",
                   "stock": "float", "precio_lista": "float", "id_proveedor": "str"},
    "proveedores": {"id_proveedor": "str", "proveedor": "str", "ciudad": "str",
                    "tiempo_entrega_dias": "int"},
}
LLAVES = {"ventas": "id_venta", "inventario": "id_producto", "proveedores": "id_proveedor"}


class ErrorETL(Exception):
    """Error controlado: detiene el proceso con un mensaje claro."""


def configurar_bitacora(carpeta: Path) -> logging.Logger:
    carpeta.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("etl")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for h in (logging.StreamHandler(sys.stdout), logging.FileHandler(carpeta / "bitacora_etl.log", mode="w", encoding="utf-8")):
        h.setFormatter(fmt)
        log.addHandler(h)
    return log


def perfil(nombre: str, df: pd.DataFrame, etapa: str) -> dict:
    """Mide la calidad de una tabla: filas, nulos y duplicados."""
    return {
        "tabla": nombre, "etapa": etapa, "filas": len(df),
        "nulos": int(df.isna().sum().sum()),
        "duplicados_exactos": int(df.duplicated().sum()),
        "llaves_repetidas": int(df[LLAVES[nombre]].duplicated().sum()) if LLAVES.get(nombre) in df else 0,
        "detalle_nulos": "; ".join(f"{c}={n}" for c, n in df.isna().sum().items() if n) or "-",
    }


# ---------------------------------------------------------------- 1. LANDING
def extraer(fuente: str, log: logging.Logger) -> dict:
    tablas = {}
    for nombre, esquema in ESQUEMAS.items():
        ruta = f"{fuente.rstrip('/')}/{nombre}.csv"
        try:
            df = pd.read_csv(ruta, dtype=str, keep_default_na=True, encoding="utf-8")
        except FileNotFoundError:
            raise ErrorETL(f"No se encontró la fuente {ruta}")
        except Exception as e:  # URL caída, archivo corrupto, codificación, etc.
            raise ErrorETL(f"No se pudo leer {ruta}: {e}")
        faltan = set(esquema) - set(df.columns)
        if faltan:
            raise ErrorETL(f"{nombre}.csv no tiene las columnas obligatorias: {sorted(faltan)}")
        tablas[nombre] = df[list(esquema)]
        log.info(f"Extracción  | {nombre:<11} | {len(df):>4} filas leídas de {ruta}")
    return tablas


# ---------------------------------------------------------------- 2. STAGING
def tipar(nombre: str, df: pd.DataFrame, log: logging.Logger) -> pd.DataFrame:
    df = df.copy()
    for col, tipo in ESQUEMAS[nombre].items():
        df[col] = df[col].str.strip() if df[col].dtype == object else df[col]
        if tipo in ("int", "float"):
            convertido = pd.to_numeric(df[col], errors="coerce")
            malos = convertido.isna() & df[col].notna()
            if malos.any():
                log.warning(f"Staging     | {nombre}.{col}: {malos.sum()} valores no numéricos pasan a nulo")
            df[col] = convertido
        elif tipo == "date":
            convertido = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d")
            malos = convertido.isna() & df[col].notna()
            if malos.any():
                log.warning(f"Staging     | {nombre}.{col}: {malos.sum()} fechas inválidas pasan a nulo")
            df[col] = convertido
    return df


# ---------------------------------------------------------------- 3. TRANSFORMACIÓN
def transformar(t: dict, rechazos: list, log: logging.Logger) -> dict:
    def rechazar(df, mascara, tabla, regla, motivo):
        if mascara.any():
            r = df[mascara].copy()
            r.insert(0, "motivo", motivo)
            r.insert(0, "regla", regla)
            r.insert(0, "tabla", tabla)
            rechazos.append(r)
        log.info(f"{regla:<11} | {tabla:<11} | {int(mascara.sum()):>4} registros: {motivo}")
        return df[~mascara].copy()

    v, i, p = t["ventas"], t["inventario"], t["proveedores"]

    # R1 Duplicados exactos: se conserva la primera aparición
    v = rechazar(v, v.duplicated(), "ventas", "R1", "duplicado exacto")
    i = rechazar(i, i.duplicated(), "inventario", "R1", "duplicado exacto")
    p = rechazar(p, p.duplicated(), "proveedores", "R1", "duplicado exacto")
    # Si la llave sigue repetida con datos distintos, el conflicto se envía a revisión
    v = rechazar(v, v.id_venta.duplicated(keep=False), "ventas", "R1", "id_venta repetido con datos distintos")
    i = rechazar(i, i.id_producto.duplicated(keep=False), "inventario", "R1", "id_producto repetido con datos distintos")

    # R2 Ventas sin precio o con valores imposibles se eliminan (regla del caso)
    v = rechazar(v, v.precio_venta.isna(), "ventas", "R2", "venta sin precio")
    v = rechazar(v, (v.cantidad <= 0) | (v.precio_venta <= 0) | v.fecha.isna(), "ventas", "R2", "cantidad, precio o fecha inválidos")

    # R3 Normalización de texto: categorías en formato título y sin espacios sobrantes
    #    (PAPELERÍA, tecnología y "deportes " pasan a Papelería, Tecnología y Deportes).
    #    En nombres y canales solo se quitan espacios, para no dañar marcas como WhatsApp o USB-C.
    antes = i["categoria"].copy()
    i["categoria"] = i["categoria"].str.strip().str.title()
    log.info(f"R3          | categoria   | {int((antes != i['categoria']).sum()):>4} valores normalizados")
    for df, col in ((v, "canal"), (i, "nombre_producto"), (p, "proveedor")):
        df[col] = df[col].str.strip()

    # R4 Inventario: stock y precio de lista faltantes se imputan con el promedio,
    #    y el producto queda marcado para verificarlo con el proveedor.
    #    No se eliminan: borrar un producto del catálogo borraría también sus ventas.
    i["requiere_verificacion"] = i.stock.isna() | i.precio_lista.isna()
    for col in ("stock", "precio_lista"):
        prom = i[col].mean()
        n = int(i[col].isna().sum())
        i[col] = i[col].fillna(prom)
        log.info(f"R4          | {col:<11} | {n:>4} nulos imputados con el promedio ({prom:,.2f})")
    i = rechazar(i, i.stock < 0, "inventario", "R4", "stock negativo")

    # R5 Integridad referencial: cada venta debe apuntar a un producto y cada producto a un proveedor
    v = rechazar(v, ~v.id_producto.isin(i.id_producto), "ventas", "R5", "producto no existe en el catálogo")
    i = rechazar(i, ~i.id_proveedor.isin(p.id_proveedor), "inventario", "R5", "proveedor no existe")

    # R6 Campos derivados
    v["total_venta"] = v.cantidad * v.precio_venta
    return {"ventas": v, "inventario": i, "proveedores": p}


# ---------------------------------------------------------------- 4. CARGA (esquema en estrella)
def cargar(t: dict, destino: Path, log: logging.Logger) -> dict:
    destino.mkdir(parents=True, exist_ok=True)
    v, i, p = t["ventas"], t["inventario"], t["proveedores"]

    dim_producto = i[["id_producto", "nombre_producto", "categoria", "stock", "precio_lista", "requiere_verificacion"]].sort_values("id_producto")
    dim_proveedor = p.sort_values("id_proveedor")
    canales = sorted(v.canal.unique())
    dim_canal = pd.DataFrame({"id_canal": [f"C{n + 1:02d}" for n in range(len(canales))], "canal": canales})
    fechas = pd.date_range(v.fecha.min(), v.fecha.max(), freq="D")
    dim_fecha = pd.DataFrame({"id_fecha": fechas.strftime("%Y%m%d").astype(int), "fecha": fechas.date,
                              "anio": fechas.year, "mes": fechas.month, "dia": fechas.day,
                              "dia_semana": fechas.dayofweek + 1, "semana_anio": fechas.isocalendar().week.values})

    fact = (v.merge(i[["id_producto", "id_proveedor"]], on="id_producto", how="left")
             .merge(dim_canal, on="canal", how="left"))
    fact["id_fecha"] = fact.fecha.dt.strftime("%Y%m%d").astype(int)
    fact_ventas = fact[["id_venta", "id_fecha", "id_producto", "id_proveedor", "id_canal",
                        "cantidad", "precio_venta", "total_venta"]].sort_values("id_venta")

    # Vista consolidada (la misma tabla que sale del flujo de Orange)
    consolidado = (v.merge(i, on="id_producto", how="left").merge(p, on="id_proveedor", how="left"))
    consolidado["fecha"] = consolidado.fecha.dt.strftime("%Y-%m-%d")

    salida = {"fact_ventas": fact_ventas, "dim_producto": dim_producto, "dim_proveedor": dim_proveedor,
              "dim_canal": dim_canal, "dim_fecha": dim_fecha, "ventas_consolidadas": consolidado}
    for nombre, df in salida.items():
        df.to_csv(destino / f"{nombre}.csv", index=False, encoding="utf-8")
        log.info(f"Carga       | {nombre:<19} | {len(df):>4} filas -> {destino.name}/{nombre}.csv")

    # Validación final del data mart
    huérfanos = (~fact_ventas.id_producto.isin(dim_producto.id_producto)).sum() + fact_ventas.id_canal.isna().sum()
    if huérfanos or fact_ventas.isna().any().any():
        raise ErrorETL("La tabla de hechos quedó con llaves huérfanas o nulos; revise la bitácora")
    log.info("Validación  | data mart sin nulos ni llaves huérfanas")
    return salida


def main() -> int:
    ap = argparse.ArgumentParser(description="ETL de inventarios - Variedades Aurora S.A.S.")
    ap.add_argument("--fuente", default=str(RAIZ / "data" / "raw"), help="carpeta o URL con ventas.csv, inventario.csv y proveedores.csv")
    args = ap.parse_args()

    log = configurar_bitacora(RAIZ / "reports")
    inicio = datetime.now()
    log.info(f"Inicio del ETL | fuente: {args.fuente}")
    try:
        crudo = extraer(args.fuente, log)
        staging_dir = RAIZ / "data" / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staging = {}
        for n, df in crudo.items():
            staging[n] = tipar(n, df, log)
            staging[n].to_csv(staging_dir / f"{n}.csv", index=False, encoding="utf-8")
        antes = [perfil(n, df, "antes") for n, df in staging.items()]

        rechazos = []
        limpio = transformar(staging, rechazos, log)
        cargar(limpio, RAIZ / "data" / "mart", log)
        despues = [perfil(n, df.drop(columns=["requiere_verificacion", "total_venta"], errors="ignore"), "después") for n, df in limpio.items()]

        auditoria = pd.DataFrame(antes + despues).sort_values(["tabla", "etapa"], ascending=[True, True])
        auditoria.to_csv(RAIZ / "reports" / "auditoria_calidad.csv", index=False, encoding="utf-8")
        rech_dir = RAIZ / "data" / "rechazados"
        rech_dir.mkdir(parents=True, exist_ok=True)
        rech = pd.concat(rechazos, ignore_index=True) if rechazos else pd.DataFrame(columns=["tabla", "regla", "motivo"])
        rech.to_csv(rech_dir / "registros_rechazados.csv", index=False, encoding="utf-8")

        log.info(f"Rechazados  | {len(rech)} registros en cuarentena (data/rechazados/registros_rechazados.csv)")
        for _, r in auditoria.iterrows():
            log.info(f"Calidad     | {r.tabla:<11} | {r.etapa:<7} | filas={r.filas:>4} nulos={r.nulos:>3} duplicados={r.duplicados_exactos:>3}")
        log.info(f"Fin del ETL sin errores en {(datetime.now() - inicio).total_seconds():.2f} s")
        return 0
    except ErrorETL as e:
        log.error(f"ETL detenido: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
