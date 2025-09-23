import os
import csv
import mysql.connector
import pandas as pd

# ⚙️ Настройки подключения к MySQL
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "1234",  # измени при необходимости
    "database": "uefa_champions_league_2025"
}

CSV_DIR = "csv"  # папка с csv файлами


def detect_delimiter(file_path):
    """Определяет разделитель в CSV"""
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(2048)
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
            return dialect.delimiter
        except Exception:
            return ","


def get_headers_and_data(file_path, delimiter):
    """Считывает CSV и возвращает заголовки и данные"""
    df = pd.read_csv(file_path, delimiter=delimiter, encoding="utf-8", engine="python")

    if df.empty:
        return [], []

    # Если нет нормальных заголовков → делаем col1, col2...
    if any(str(col).startswith("Unnamed") for col in df.columns):
        df.columns = [f"col{i+1}" for i in range(len(df.columns))]

    # Заменяем NaN → None
    df = df.where(pd.notnull(df), None)

    return df.columns.tolist(), df.values.tolist()


def sql_type(value):
    """Определяет SQL тип по значению"""
    if isinstance(value, (int, float)):
        return "DOUBLE"
    return "VARCHAR(255)"


def create_table(cursor, table_name, headers, sample_row):
    """Создаёт таблицу"""
    columns_sql = []
    for col, val in zip(headers, sample_row):
        col = col.replace(" ", "_").replace("-", "_")  # убрать пробелы/дефисы
        columns_sql.append(f"`{col}` {sql_type(val)}")
    columns_sql = ", ".join(columns_sql)

    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
    cursor.execute(f"CREATE TABLE `{table_name}` ({columns_sql});")


def insert_data(cursor, table_name, headers, data):
    """Вставляет данные в таблицу"""
    placeholders = ", ".join(["%s"] * len(headers))
    columns = ", ".join([f"`{col}`" for col in headers])
    sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

    # Превращаем numpy.nan → None
    clean_data = [[None if (isinstance(v, float) and pd.isna(v)) else v for v in row] for row in data]

    cursor.executemany(sql, clean_data)


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("🔄 Подключение к БД успешно. Начинаем загрузку CSV...\n")

    for file in os.listdir(CSV_DIR):
        if not file.endswith(".csv"):
            continue

        file_path = os.path.join(CSV_DIR, file)
        table_name = os.path.splitext(file)[0].lower()

        print(f"📂 Обработка {file}...")

        delimiter = detect_delimiter(file_path)
        headers, data = get_headers_and_data(file_path, delimiter)

        if not headers or not data:
            print(f"⚠️ Пропуск {file}, нет данных.")
            continue

        create_table(cursor, table_name, headers, data[0])
        insert_data(cursor, table_name, headers, data)

        conn.commit()
        print(f"✅ Таблица {table_name} создана и заполнена ({len(data)} строк).")

    cursor.close()
    conn.close()
    print("\n🎉 Все таблицы успешно созданы и загружены!")


if __name__ == "__main__":
    main()
