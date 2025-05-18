import mysql.connector
from datetime import datetime, timedelta
import random
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(override=True)

# Database connection configuration
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "store_user"),
    "password": os.getenv("DB_PASS", "store_password"),
    "database": os.getenv("DB_NAME", "customer_db"),
}


def create_tables(cursor):
    # Create customer table
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS customer (
        CustomerId INT NOT NULL,
        CustomerName VARCHAR(255) NOT NULL,
        Email VARCHAR(255) UNIQUE,
        PhoneNumber VARCHAR(50),
        BillingAddress VARCHAR(500) NOT NULL,
        IsActive TINYINT NOT NULL DEFAULT 1,
        CreatedAt DATETIME NOT NULL,
        CreatedUser VARCHAR(100) NOT NULL,
        UpdatedAt DATETIME,
        UpdatedUser VARCHAR(100),
        VersionFlag INT NOT NULL DEFAULT 1,
        PRIMARY KEY (CustomerId)
    )
    """
    )

    # Create customer_order table
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS customer_order (
        OrderId INT NOT NULL,
        CustomerId INT NOT NULL,
        OrderDate DATETIME NOT NULL,
        OrderStatus VARCHAR(50) NOT NULL,
        TotalAmount DECIMAL(12,2) NOT NULL,
        Currency VARCHAR(10) NOT NULL,
        PaymentStatus VARCHAR(50) NOT NULL,
        ShippingAddress VARCHAR(500) NOT NULL,
        ShippingTerms VARCHAR(100) NOT NULL,
        EstimatedDeliveryDate DATETIME,
        CreatedAt DATETIME NOT NULL,
        CreatedUser VARCHAR(100) NOT NULL,
        UpdatedAt DATETIME,
        UpdatedUser VARCHAR(100),
        VersionFlag INT NOT NULL,
        PRIMARY KEY (OrderId),
        FOREIGN KEY (CustomerId) REFERENCES customer(CustomerId)
    )
    """
    )


def insert_demo_data(cursor):
    # Sample data for customers
    customers = [
        (
            1,
            "John Smith",
            "john.smith@email.com",
            "+1-555-0123",
            "123 Main St, New York, NY 10001",
            1,
            datetime.now(),
            "SYSTEM",
            None,
            None,
            1,
        ),
        (
            2,
            "Emma Johnson",
            "emma.j@email.com",
            "+1-555-0124",
            "456 Oak Ave, Los Angeles, CA 90001",
            1,
            datetime.now(),
            "SYSTEM",
            None,
            None,
            1,
        ),
        (
            3,
            "Michael Brown",
            "m.brown@email.com",
            "+1-555-0125",
            "789 Pine Rd, Chicago, IL 60601",
            1,
            datetime.now(),
            "SYSTEM",
            None,
            None,
            1,
        ),
        (
            4,
            "Sarah Davis",
            "sarah.d@email.com",
            "+1-555-0126",
            "321 Elm St, Houston, TX 77001",
            1,
            datetime.now(),
            "SYSTEM",
            None,
            None,
            1,
        ),
        (
            5,
            "David Wilson",
            "d.wilson@email.com",
            "+1-555-0127",
            "654 Maple Dr, Miami, FL 33101",
            1,
            datetime.now(),
            "SYSTEM",
            None,
            None,
            1,
        ),
    ]

    # Insert customers
    cursor.executemany(
        """
    INSERT INTO customer (CustomerId, CustomerName, Email, PhoneNumber, BillingAddress, 
                         IsActive, CreatedAt, CreatedUser, UpdatedAt, UpdatedUser, VersionFlag)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        customers,
    )

    # Sample data for orders
    order_statuses = ["Pending", "Confirmed", "Shipped", "Delivered"]
    payment_statuses = ["Paid", "Partially Paid", "Unpaid"]
    shipping_terms = ["Standard", "Express", "Next Day"]
    currencies = ["USD", "EUR", "GBP"]

    orders = []
    for i in range(1, 11):  # Create 10 sample orders
        customer_id = random.randint(1, 5)
        order_date = datetime.now() - timedelta(days=random.randint(0, 30))
        estimated_delivery = order_date + timedelta(days=random.randint(2, 7))

        orders.append(
            (
                i,  # OrderId
                customer_id,  # CustomerId
                order_date,  # OrderDate
                random.choice(order_statuses),  # OrderStatus
                round(random.uniform(100.00, 2000.00), 2),  # TotalAmount
                random.choice(currencies),  # Currency
                random.choice(payment_statuses),  # PaymentStatus
                f"{random.randint(100, 999)} Delivery St, City, State {random.randint(10000, 99999)}",  # ShippingAddress
                random.choice(shipping_terms),  # ShippingTerms
                estimated_delivery,  # EstimatedDeliveryDate
                datetime.now(),  # CreatedAt
                "SYSTEM",  # CreatedUser
                None,  # UpdatedAt
                None,  # UpdatedUser
                1,  # VersionFlag
            )
        )

    # Insert orders
    cursor.executemany(
        """
    INSERT INTO customer_order (OrderId, CustomerId, OrderDate, OrderStatus, TotalAmount,
                               Currency, PaymentStatus, ShippingAddress, ShippingTerms,
                               EstimatedDeliveryDate, CreatedAt, CreatedUser, UpdatedAt,
                               UpdatedUser, VersionFlag)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        orders,
    )


def main():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Create tables
        create_tables(cursor)

        # Insert demo data
        insert_demo_data(cursor)

        # Commit the changes
        conn.commit()
        print("Database initialized successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if "conn" in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
