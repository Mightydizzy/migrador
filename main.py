import pymysql
import decimal
from pymongo import MongoClient

mysql_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "fluidpro",
    "port": 3306
}

mongo_uri = "mongodb://mongoadmin:pwd_secreta@192.168.106.129:27017/miempresa?authSource=admin"

# Conexión MySQL
conn_sql = pymysql.connect(**mysql_config)
cursor = conn_sql.cursor(pymysql.cursors.DictCursor)

# Conexión MongoDB
client = MongoClient(mongo_uri)
mongo_db = client["miempresa"]

# Limpieza de colecciones por si existen
mongo_db.clientes.drop()
mongo_db.productos.drop()
mongo_db.pedidos.drop()

# MIGRAR CLIENTES
cursor.execute("SELECT * FROM clientes")
clientes = cursor.fetchall()
mongo_db.clientes.insert_many(clientes)

# MIGRAR PRODUCTOS
cursor.execute("SELECT * FROM productos")
productos = cursor.fetchall()

for p in productos:
    if isinstance(p["precio_unitario"], decimal.Decimal):
        p["precio_unitario"] = float(p["precio_unitario"])

mongo_db.productos.insert_many(productos)

# Mapear productos por ID para acceso rápido
productos_map = {prod["id_producto"]: prod for prod in productos}

# MIGRAR PEDIDOS CON DETALLE
cursor.execute("SELECT * FROM pedidos")
pedidos = cursor.fetchall()

# Obtener todos los detalles
cursor.execute("SELECT * FROM detalle_pedidos")
detalles = cursor.fetchall()

# Agrupar detalle por pedido
from collections import defaultdict
detalle_por_pedido = defaultdict(list)
for det in detalles:
    detalle_por_pedido[det["id_pedido"]].append(det)

# Construir documentos de pedidos
pedidos_docs = []
for pedido in pedidos:
    detalles_raw = detalle_por_pedido.get(pedido["id_pedido"], [])
    detalle_con_snapshot = []

    for d in detalles_raw:
        prod = productos_map.get(d["id_producto"], {})
        detalle_con_snapshot.append({
            "id_producto": d["id_producto"],
            "nombre": prod.get("nombre"),
            "precio_unitario": float(d["precio_unitario"]),
            "cantidad": d["cantidad"]
        })

    pedido_doc = {
        "_id": pedido["id_pedido"],
        "fecha_pedido": str(pedido["fecha_pedido"]),
        "estado": pedido["estado"],
        "id_cliente": pedido["id_cliente"],
        "detalle_pedidos": detalle_con_snapshot
    }
    pedidos_docs.append(pedido_doc)

mongo_db.pedidos.insert_many(pedidos_docs)


print("✅ Migración completada con éxito.")


cursor.close()
conn_sql.close()
client.close()
