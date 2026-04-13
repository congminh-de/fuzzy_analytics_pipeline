import polars as pl 
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
DB_PASS = os.getenv("MYSQL_ROOT_PASSWORD")
DB_NAME = os.getenv("MYSQL_DATABASE")

conn_str = f"mysql+pymysql://root:{DB_PASS}@localhost:3306/{DB_NAME}"
engine = create_engine(conn_str)

def seed_data():

    csv_files = {
        "order_item_refunds": "fuzzy_oltp/order_item_refunds.csv",
        "order_items": "fuzzy_oltp/order_items.csv",
        "orders": "fuzzy_oltp/orders.csv",
        "products": "fuzzy_oltp/products.csv",
        "website_pageviews": "fuzzy_oltp/website_pageviews.csv",
        "website_sessions": "fuzzy_oltp/website_sessions.csv"        
    }

    for table_name, file_path in csv_files.items():
        print(f"Seeding data for {table_name} from {file_path}...")
        df = pl.read_csv(file_path)
        df.to_pandas().to_sql(table_name, engine, if_exists='replace', index=False)

    print("Data seeding completed.")

if __name__ == "__main__":
    seed_data()




