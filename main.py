import os
from PIL import Image
import psycopg2
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def connect_to_db():
    connection = None
    try:
        # Conéctate a tu base de datos PostgreSQL usando variables de entorno
        connection = psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME")
        )
        cursor = connection.cursor()
        # Ejecuta una consulta para obtener la versión de PostgreSQL
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        # Crear la tabla si no existe
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS ATRIBUTO_VALOR(
            food_name VARCHAR(30),
            image_id VARCHAR(50),
            PRIMARY KEY(food_name, image_id)
        );
        '''
        cursor.execute(create_table_query)
        connection.commit()
        print("Table ATRIBUTO_VALOR created successfully or already exists.")

        return connection, cursor

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        if connection:
            connection.close()
    return None, None

def verify_images(directory, cursor, connection):
    for subdir, _, files in os.walk(directory):
        food_name = os.path.basename(subdir)
        for filename in files:
            if filename.endswith(".jpg"):
                try:
                    img_path = os.path.join(subdir, filename)
                    img = Image.open(img_path)
                    img.verify()  # Verifica que es una imagen válida
                    print(f"{food_name}, {filename} is a valid image.")
                    
                    # Insertar en la base de datos
                    insert_query = '''
                    INSERT INTO ATRIBUTO_VALOR (food_name, image_id)
                    VALUES (%s, %s)
                    ON CONFLICT (food_name, image_id) DO NOTHING;
                    '''
                    cursor.execute(insert_query, (food_name, filename))
                    connection.commit()
                except (IOError, SyntaxError) as e:
                    print(f"{food_name}, {filename} is not a valid image. Error: {e}")

# Conéctate a la base de datos
connection, cursor = connect_to_db()

if connection and cursor:
    # Verifica las imágenes y realiza las inserciones en la base de datos
    verify_images('food-101', cursor, connection)

    # Cierra la conexión
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")