# Mapeo de datos · Variedades Aurora S.A.S.

Origen: tres hojas del archivo de la empresa, exportadas a CSV en `data/raw/`.
Destino: data mart en estrella en `data/mart/` (una tabla de hechos y cuatro dimensiones).

## Fuentes

| Fuente | Archivo | Filas crudas | Llave | Contenido |
|---|---|---|---|---|
| Ventas | `data/raw/ventas.csv` | 156 | `id_venta` | Una fila por venta: fecha, producto, cantidad, precio unitario y canal |
| Inventario | `data/raw/inventario.csv` | 53 | `id_producto` | Catálogo: nombre, categoría, stock, precio de lista y proveedor |
| Proveedores | `data/raw/proveedores.csv` | 6 | `id_proveedor` | Nombre, ciudad y tiempo de entrega |

## Campo por campo

| Fuente | Campo origen | Tipo origen | Tipo destino | Regla aplicada | Tabla destino | Campo destino |
|---|---|---|---|---|---|---|
| ventas | id_venta | texto | texto | R1 duplicados eliminados | fact_ventas | id_venta |
| ventas | fecha | texto AAAA-MM-DD | fecha | validación de formato; llave a dim_fecha | fact_ventas | id_fecha (AAAAMMDD) |
| ventas | id_producto | texto | texto | R5 debe existir en el catálogo | fact_ventas | id_producto |
| ventas | cantidad | entero | entero | R2 debe ser mayor que 0 | fact_ventas | cantidad |
| ventas | precio_venta | decimal | decimal | R2 venta sin precio se elimina | fact_ventas | precio_venta |
| ventas | canal | texto | texto | espacios recortados; llave a dim_canal | fact_ventas | id_canal |
| ventas | (derivado) | – | decimal | R6 cantidad × precio_venta | fact_ventas | total_venta |
| inventario | id_producto | texto | texto | R1 duplicados eliminados | dim_producto | id_producto |
| inventario | nombre_producto | texto | texto | espacios recortados | dim_producto | nombre_producto |
| inventario | categoria | texto | texto | R3 formato título (PAPELERÍA → Papelería) | dim_producto | categoria |
| inventario | stock | decimal | decimal | R4 nulo → promedio; marca de verificación; stock negativo se rechaza | dim_producto | stock |
| inventario | precio_lista | decimal | decimal | R4 nulo → promedio; marca de verificación | dim_producto | precio_lista |
| inventario | (derivado) | – | booleano | R4 verdadero si se imputó stock o precio | dim_producto | requiere_verificacion |
| inventario | id_proveedor | texto | texto | R5 debe existir en proveedores | fact_ventas | id_proveedor |
| proveedores | id_proveedor | texto | texto | R1 duplicados | dim_proveedor | id_proveedor |
| proveedores | proveedor, ciudad | texto | texto | espacios recortados | dim_proveedor | proveedor, ciudad |
| proveedores | tiempo_entrega_dias | entero | entero | sin cambios | dim_proveedor | tiempo_entrega_dias |

## Reglas de limpieza

| Regla | Qué hace | Por qué |
|---|---|---|
| R1 | Elimina duplicados exactos y envía a revisión las llaves repetidas con datos distintos | Una venta contada dos veces infla los ingresos |
| R2 | Elimina ventas sin precio, con cantidad o precio menor o igual a 0, o sin fecha válida | Lo pide el caso: una venta sin precio no se puede valorar |
| R3 | Estandariza las categorías | "PAPELERÍA" y "Papelería" deben sumar juntas |
| R4 | Imputa stock y precio de lista faltantes con el promedio y marca el producto | Borrar el producto borraría también sus ventas; la marca obliga a confirmar con el proveedor |
| R5 | Verifica que cada venta tenga producto y cada producto tenga proveedor | Evita llaves huérfanas en el data mart |
| R6 | Calcula `total_venta` | Medida principal de la tabla de hechos |

Los registros rechazados quedan en `data/rechazados/registros_rechazados.csv` con la tabla, la regla y el motivo.

## Destino: esquema en estrella

```
                 dim_fecha
                     |
dim_canal ---- fact_ventas ---- dim_producto
                     |
               dim_proveedor
```

- **fact_ventas**: una fila por venta válida, con `cantidad`, `precio_venta` y `total_venta`.
- **dim_producto**, **dim_proveedor**, **dim_canal** y **dim_fecha**: describen cada venta.
