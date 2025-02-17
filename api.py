import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from psycopg2 import pool
from flask import Flask, request, jsonify
import datetime
from zoneinfo import ZoneInfo
import time

# Obtención de variables de entorno
API_SECRET = os.environ.get("API_SECRET", "A7Zb;zM(#fSmW+x9r6G_8e*jTPp`R~Qk")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres.ahshhbtukahzeemozbtu:Aa7QqzZrguV2YN6esfcpMk@aws-0-sa-east-1.pooler.supabase.com:5432/postgres")

# Inicialización del pool de conexiones
db_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)
if db_pool:
    print("Pool de conexiones creado")

def get_conn():
    conn = db_pool.getconn()
    if conn.closed:
        conn = db_pool.getconn()
    return conn

app = Flask(__name__)

def check_auth(req):
    auth = req.headers.get("Authorization")
    if not auth or auth != f"Bearer {API_SECRET}":
        return False
    return True

@contextmanager
def get_db_connection():
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)

@app.route("/", methods=["GET"])
def home_page():
    return "El bot está funcionando!", 200

# --- Gestión de Usuarios ---
@app.route("/api/registrados", methods=["GET"])
def api_registrados():
    if not check_auth(request):
        return jsonify({"error": "No autorizado"}), 401
    with get_conn().cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM registrations ORDER BY puntuacion DESC")
        rows = cur.fetchall()
    participantes = {}
    for row in rows:
        participantes[row["user_id"]] = row
    return jsonify({"participants": participantes})

@app.route("/api/usuarios", methods=["POST"])
def api_registrar_usuario():
    if not check_auth(request):
        return jsonify({"error": "No autorizado"}), 401

    data = request.get_json()
    required_fields = ["user_id", "discord_name", "fortnite_username", "platform", "country"]
    if not data or not all(field in data for field in required_fields):
        return jsonify({
            "error": "Datos inválidos. Se requieren los campos 'user_id', 'discord_name', 'fortnite_username', 'platform' y 'country'."
        }), 400

    participant = {
        "discord_name": data["discord_name"],
        "fortnite_username": data["fortnite_username"],
        "platform": data["platform"],
        "country": data["country"],
        "puntuacion": data.get("puntuacion", 0),
        "etapa": data.get("etapa", 1),
        "grupo": data.get("grupo", 0)
    }
    try:
        with get_conn().cursor() as cur:
            cur.execute("""
                INSERT INTO registrations (user_id, discord_name, fortnite_username, platform, country, puntuacion, etapa, grupo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    discord_name = EXCLUDED.discord_name,
                    fortnite_username = EXCLUDED.fortnite_username,
                    platform = EXCLUDED.platform,
                    country = EXCLUDED.country,
                    puntuacion = EXCLUDED.puntuacion,
                    etapa = EXCLUDED.etapa,
                    grupo = EXCLUDED.grupo
            """, (data["user_id"], participant["discord_name"], participant["fortnite_username"],
                  participant["platform"], participant["country"], participant["puntuacion"],
                  participant["etapa"], participant["grupo"]))
        get_conn().commit()
        return jsonify({"mensaje": f"Usuario {data['discord_name']} registrado/actualizado correctamente."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/usuarios/<user_id>", methods=["DELETE"])
def api_eliminar_usuario(user_id):
    if not check_auth(request):
        return jsonify({"error": "No autorizado"}), 401

    try:
        with get_conn().cursor() as cur:
            cur.execute("DELETE FROM registrations WHERE user_id = %s", (user_id,))
        get_conn().commit()
        return jsonify({"mensaje": f"Usuario con ID {user_id} eliminado."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Gestión de Puntos ---
@app.route("/api/puntos", methods=["POST"])
def api_actualizar_puntos():
    if not check_auth(request):
        return jsonify({"error": "No autorizado"}), 401

    data = request.get_json()
    if not data or "user_id" not in data or "delta" not in data:
        return jsonify({"error": "Datos inválidos. Se requieren 'user_id' y 'delta'."}), 400

    user_id = data["user_id"]
    delta = data["delta"]

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT puntuacion FROM registrations WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                current = row["puntuacion"] if row and row["puntuacion"] is not None else 0
                nuevos_puntos = int(current) + int(delta)
                cur.execute("UPDATE registrations SET puntuacion = %s WHERE user_id = %s", (nuevos_puntos, user_id))
            conn.commit()
        return jsonify({"user_id": user_id, "nuevos_puntos": nuevos_puntos}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

