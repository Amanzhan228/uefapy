import os
import pandas as pd
import pymysql

# 📂 Папка с CSV
CSV_FOLDER = "csv"

# 🔑 Настройки подключения к БД
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "1234",
    "database": "uefa_champions_league_2025",
    "charset": "utf8mb4"
}

def clean_column_name(name: str) -> str:
    """Чистим названия колонок для MySQL"""
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "pct")
        .replace("/", "_per_")
        .replace("-", "_")
        .replace(".", "_")
    )

def load_csv_to_mysql(cursor, file_path, table_name):
    """Загрузка одного CSV в таблицу MySQL"""
    try:
        # Читаем CSV
        df = pd.read_csv(file_path)

        # Убираем пустые колонки (Unnamed: 0 и т.д.)
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

        # Чистим заголовки
        df.columns = [clean_column_name(c) for c in df.columns]

        # Заменяем NaN на None
        df = df.where(pd.notnull(df), None)

        # Создаём таблицу (все колонки VARCHAR(255) для универсальности)
        columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        cursor.execute(f"CREATE TABLE `{table_name}` ({columns});")

        # Подготавливаем SQL для вставки
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO `{table_name}` ({', '.join(df.columns)}) VALUES ({placeholders})"

        # Вставляем данные
        data = []
        for row in df.itertuples(index=False, name=None):
            clean_row = []
            for v in row:
                if v is None:
                    clean_row.append(None)
                else:
                    clean_row.append(str(v))
            data.append(tuple(clean_row))

        cursor.executemany(sql, data)
        print(f"✅ {os.path.basename(file_path)} загружен в таблицу {table_name}")

    except Exception as e:
        print(f"❌ Ошибка при загрузке {os.path.basename(file_path)}: {e}")

def main():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("🔄 Подключение к БД успешно. Загружаем CSV...")

        for file in os.listdir(CSV_FOLDER):
            if file.endswith(".csv"):
                table_name = os.path.splitext(file)[0].lower()
                file_path = os.path.join(CSV_FOLDER, file)
                load_csv_to_mysql(cursor, file_path, table_name)

        conn.commit()
        cursor.close()
        conn.close()
        print("🎉 Все CSV загружены в БД.")
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")

if __name__ == "__main__":
    main()
