import uuid
import random
import json
import psycopg2
from faker import Faker
from datetime import datetime

DB_CONFIG = {
    "host": "host.docker.internal",
    "dbname": "final-project",
    "user": "postgres",
    "password": "1",
    "port": 5432
}

fake = Faker()

def generate_fake_user():
    gender = random.choice(["male", "female"])
    dob_date = fake.date_of_birth(minimum_age=18, maximum_age=90)

    total_orders = random.randint(0, 200)
    total_spent = round(random.uniform(0, 20000), 2)
    avg_order_value = round(total_spent / total_orders, 2) if total_orders > 0 else 0

    created_at = fake.date_time_between(start_date="-3y", end_date="-1d")
    last_login = fake.date_time_between(start_date=created_at, end_date="now")

    return {
        "id": str(uuid.uuid4()),
        "gender": gender,
        "first_name": fake.first_name_male() if gender == "male" else fake.first_name_female(),
        "last_name": fake.last_name(),
        "username": fake.user_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "cell": fake.phone_number(),
        "dob": dob_date.isoformat(),
        "age": datetime.now().year - dob_date.year,
        "nat": fake.country_code(),
        "street_address": fake.street_address(),
        "city": fake.city(),
        "postal_code": fake.postcode(),
        "country": fake.country(),
        "latitude": float(fake.latitude()),
        "longitude": float(fake.longitude()),
        "timezone": str(fake.timezone()),
        "account_level": random.choice(["bronze","silver","gold","platinum"]),
        "is_active": random.choice([True, True, True, False]),
        "is_verified": random.choice([True, False]),
        "registration_source": random.choice(["web","mobile","referral","ads"]),
        "created_at": created_at.isoformat(),
        "last_updated_at": datetime.now().isoformat(),
        "last_login_at": last_login.isoformat(),
        "total_logins": random.randint(1,1000),
        "total_orders": total_orders,
        "total_spent": total_spent,
        "avg_order_value": avg_order_value,
        "risk_score": round(random.uniform(0,1),3),
        "email_opt_in": random.choice([True, False])
    }

def create_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_users (
            id SERIAL PRIMARY KEY,
            payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
    """)

def insert_user(cursor, user_data):
    cursor.execute(
        "INSERT INTO raw_users (payload) VALUES (%s)",
        (json.dumps(user_data),)
    )

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    create_table_if_not_exists(cursor)
    user = generate_fake_user()
    insert_user(cursor, user)
    conn.commit()
    cursor.close()
    conn.close()

    print("User inserted successfully")

if __name__ == "__main__":
    main()
