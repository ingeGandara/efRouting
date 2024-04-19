import os
import requests
import json
import pymysql
from datetime import datetime, timedelta

def lambda_handler(event, context):
    # Obtener la clave de la API de CoinMarketCap
    api_key = os.getenv('Api_key')

    # Realizar la solicitud a la API de CoinMarketCap
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key
    }

    params = {
        'symbol': 'BTC,ETH',  # Bitcoin, Ethereum
        'convert': 'USD'
    }

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'

    response = requests.get(url, params=params, headers=headers)
    data = response.json()

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        # Extraer los precios de BTC y ETH
        btc_price = data['data']['BTC']['quote']['USD']['price']
        eth_price = data['data']['ETH']['quote']['USD']['price']
        
        # Obtener la estampa de tiempo actual
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        # Conectar a la base de datos
        db_host = os.getenv('dbhost')
        db_user = os.getenv('user')
        db_password = os.getenv('password')
        db_name = os.getenv('dbname')
        connection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)

        # Crear la tabla si no existe
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS precios_criptomonedas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    criptomoneda VARCHAR(10),
                    precio DECIMAL(20, 10),
                    timestamp DATETIME
                )
            """)
            connection.commit()

        # Insertar los precios en la tabla
        with connection.cursor() as cursor:
            sql = "INSERT INTO precios_criptomonedas (criptomoneda, precio, timestamp) VALUES (%s, %s, %s)"
            cursor.execute(sql, ('BTC', btc_price, timestamp))
            cursor.execute(sql, ('ETH', eth_price, timestamp))
            connection.commit()

        # Cerrar la conexión a la base de datos
        connection.close()

        # Devolver los precios y la estampa de tiempo como respuesta
        return {
            'statusCode': 200,
            'body': json.dumps({
                'BTC_price': btc_price,
                'ETH_price': eth_price,
                'timestamp': timestamp
            })
        }
    else:
        # Si la solicitud falla, devolver un mensaje de error
        return {
            'statusCode': response.status_code,
            'body': json.dumps({
                'error': 'Failed to retrieve data from CoinMarketCap API'
            })
        }