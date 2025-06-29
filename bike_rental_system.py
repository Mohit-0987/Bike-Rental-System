import sqlite3
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from typing import List, Optional
import os

# Configure SQLite to handle datetime properly (fixes Python 3.12 deprecation warning)
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
sqlite3.register_converter("TIMESTAMP", lambda s: datetime.fromisoformat(s.decode()))


# Database setup and connection
class DatabaseManager:
    def __init__(self, db_name="bike_rental.db"):
        self.db_name = db_name
        self.init_database()

    def get_connection(self):
        conn = sqlite3.connect(self.db_name, detect_types=sqlite3.PARSE_DECLTYPES)
        return conn

    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()

        # Create bikes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bikes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bike_type TEXT NOT NULL,
                model TEXT NOT NULL,
                hourly_rate REAL NOT NULL,
                daily_rate REAL NOT NULL,
                is_available BOOLEAN DEFAULT 1,
                last_maintenance DATE
            )
        ''')

        # Create customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phone TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create rentals table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rentals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                bike_id INTEGER,
                rental_start TIMESTAMP NOT NULL,
                rental_end TIMESTAMP,
                planned_duration_hours INTEGER NOT NULL,
                actual_duration_hours REAL,
                base_cost REAL NOT NULL,
                additional_charges REAL DEFAULT 0,
                total_cost REAL NOT NULL,
                status TEXT DEFAULT 'ACTIVE',
                FOREIGN KEY (customer_id) REFERENCES customers (id),
                FOREIGN KEY (bike_id) REFERENCES bikes (id)
            )
        ''')

        conn.commit()
        conn.close()

        # Insert sample data if tables are empty
        self.insert_sample_data()

    def insert_sample_data(self):
        conn = self.get_connection()
        cursor = conn.cursor()

        # Check if bikes table is empty
        cursor.execute("SELECT COUNT(*) FROM bikes")
        if cursor.fetchone()[0] == 0:
            sample_bikes = [
                ('Mountain', 'Trek X-Caliber', 15.0, 80.0, '2024-01-15'),
                ('Road', 'Giant Defy', 12.0, 65.0, '2024-01-20'),
                ('Hybrid', 'Cannondale Quick', 10.0, 55.0, '2024-01-18'),
                ('Electric', 'Rad Power RadCity', 25.0, 120.0, '2024-01-22'),
                ('Mountain', 'Specialized Rockhopper', 14.0, 75.0, '2024-01-10'),
                ('Road', 'Cannondale CAAD', 13.0, 70.0, '2024-01-25')
            ]

            cursor.executemany('''
                INSERT INTO bikes (bike_type, model, hourly_rate, daily_rate, last_maintenance)
                VALUES (?, ?, ?, ?, ?)
            ''', sample_bikes)

        conn.commit()
        conn.close()


# Abstract base class for bikes
class Bike(ABC):
    def __init__(self, bike_id: int, bike_type: str, model: str, hourly_rate: float, daily_rate: float):
        self.bike_id = bike_id
        self.bike_type = bike_type
        self.model = model
        self.hourly_rate = hourly_rate
        self.daily_rate = daily_rate

    @abstractmethod
    def calculate_rental_cost(self, hours: int) -> float:
        pass

    @abstractmethod
    def get_description(self) -> str:
        pass

    def __str__(self):
        return f"{self.bike_type} - {self.model} (ID: {self.bike_id})"


# Concrete bike classes demonstrating inheritance and polymorphism
class MountainBike(Bike):
    def calculate_rental_cost(self, hours: int) -> float:
        if hours <= 4:
            return hours * self.hourly_rate
        else:
            days = hours // 24
            remaining_hours = hours % 24
            return (days * self.daily_rate) + (
                        remaining_hours * self.hourly_rate * 0.8)  # 20% discount for partial days

    def get_description(self) -> str:
        return f"Mountain Bike - Perfect for off-road adventures and rugged terrain"


class RoadBike(Bike):
    def calculate_rental_cost(self, hours: int) -> float:
        if hours <= 3:
            return hours * self.hourly_rate
        else:
            days = hours // 24
            remaining_hours = hours % 24
            return (days * self.daily_rate) + (remaining_hours * self.hourly_rate)

    def get_description(self) -> str:
        return f"Road Bike - Designed for speed and efficiency on paved surfaces"


class HybridBike(Bike):
    def calculate_rental_cost(self, hours: int) -> float:
        if hours <= 6:
            return hours * self.hourly_rate
        else:
            # More favorable daily rate calculation for hybrid bikes
            days = hours // 24
            remaining_hours = hours % 24
            daily_cost = days * self.daily_rate
            hourly_cost = remaining_hours * self.hourly_rate * 0.9  # 10% discount
            return daily_cost + hourly_cost

    def get_description(self) -> str:
        return f"Hybrid Bike - Versatile option combining comfort and performance"


class ElectricBike(Bike):
    def calculate_rental_cost(self, hours: int) -> float:
        base_cost = hours * self.hourly_rate if hours <= 8 else (hours // 24) * self.daily_rate + (
                    hours % 24) * self.hourly_rate
        # Add battery usage fee for electric bikes
        battery_fee = hours * 2.0
        return base_cost + battery_fee

    def get_description(self) -> str:
        return f"Electric Bike - Eco-friendly with pedal assistance (includes battery fee)"


# Factory pattern for creating bike objects
class BikeFactory:
    @staticmethod
    def create_bike(bike_data: tuple) -> Bike:
        bike_id, bike_type, model, hourly_rate, daily_rate = bike_data[:5]

        bike_classes = {
            'Mountain': MountainBike,
            'Road': RoadBike,
            'Hybrid': HybridBike,
            'Electric': ElectricBike
        }

        bike_class = bike_classes.get(bike_type, HybridBike)
        return bike_class(bike_id, bike_type, model, hourly_rate, daily_rate)


# Customer class
class Customer:
    def __init__(self, customer_id: int, name: str, email: str, phone: str):
        self.customer_id = customer_id
        self.name = name
        self.email = email
        self.phone = phone

    def __str__(self):
        return f"{self.name} ({self.email})"


# Main rental system class
class BikeRentalSystem:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.current_customer = None

    def register_customer(self, name: str, email: str, phone: str) -> bool:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO customers (name, email, phone)
                VALUES (?, ?, ?)
            ''', (name, email, phone))
            conn.commit()
            print(f"✓ Customer {name} registered successfully!")
            return True
        except sqlite3.IntegrityError:
            print(f"✗ Email {email} already exists!")
            return False
        finally:
            conn.close()

    def login_customer(self, email: str) -> bool:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM customers WHERE email = ?', (email,))
        customer_data = cursor.fetchone()

        if customer_data:
            self.current_customer = Customer(*customer_data[:4])
            print(f"✓ Welcome back, {self.current_customer.name}!")
            conn.close()
            return True
        else:
            print("✗ Customer not found!")
            conn.close()
            return False

    def get_available_bikes(self) -> List[Bike]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM bikes WHERE is_available = 1')
        bike_data = cursor.fetchall()
        conn.close()

        return [BikeFactory.create_bike(bike) for bike in bike_data]

    def display_available_bikes(self):
        bikes = self.get_available_bikes()
        if not bikes:
            print("No bikes available for rent.")
            return

        print("\n" + "=" * 80)
        print("AVAILABLE BIKES FOR RENT")
        print("=" * 80)

        for bike in bikes:
            print(f"\n🚴 ID: {bike.bike_id}")
            print(f"   {bike.get_description()}")
            print(f"   Model: {bike.model}")
            print(f"   Rates: ${bike.hourly_rate:.2f}/hour | ${bike.daily_rate:.2f}/day")

            # Show sample pricing
            sample_costs = [
                (2, bike.calculate_rental_cost(2)),
                (8, bike.calculate_rental_cost(8)),
                (24, bike.calculate_rental_cost(24))
            ]
            print(f"   Sample pricing: ", end="")
            print(" | ".join([f"{h}h: ${c:.2f}" for h, c in sample_costs]))

    def rent_bike(self, bike_id: int, duration_hours: int) -> bool:
        if not self.current_customer:
            print("Please login first!")
            return False

        # Validate input
        if duration_hours <= 0:
            print("Duration must be greater than 0 hours!")
            return False

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        # Check if bike exists and is available
        cursor.execute('SELECT * FROM bikes WHERE id = ? AND is_available = 1', (bike_id,))
        bike_data = cursor.fetchone()

        if not bike_data:
            print("✗ Bike not available or doesn't exist!")
            conn.close()
            return False

        try:
            # Create bike object and calculate cost
            bike = BikeFactory.create_bike(bike_data)
            total_cost = bike.calculate_rental_cost(duration_hours)

            # Create rental record
            rental_start = datetime.now()
            cursor.execute('''
                INSERT INTO rentals (customer_id, bike_id, rental_start, planned_duration_hours, base_cost, total_cost)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (self.current_customer.customer_id, bike_id, rental_start, duration_hours, total_cost, total_cost))

            # Mark bike as unavailable
            cursor.execute('UPDATE bikes SET is_available = 0 WHERE id = ?', (bike_id,))

            conn.commit()
            rental_id = cursor.lastrowid

            print(f"\n✓ RENTAL CONFIRMED!")
            print(f"   Rental ID: {rental_id}")
            print(f"   Bike: {bike}")
            print(f"   Duration: {duration_hours} hours")
            print(f"   Total Cost: ${total_cost:.2f}")
            print(f"   Rental Start: {rental_start.strftime('%Y-%m-%d %H:%M')}")

            return True

        except Exception as e:
            conn.rollback()
            print(f"✗ Error processing rental: {e}")
            return False
        finally:
            conn.close()

    def return_bike(self, rental_id: int) -> bool:
        if not self.current_customer:
            print("Please login first!")
            return False

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            # Get rental information with proper column selection
            cursor.execute('''
                SELECT r.id, r.customer_id, r.bike_id, r.rental_start, r.rental_end, 
                       r.planned_duration_hours, r.base_cost, r.additional_charges, r.total_cost, r.status,
                       b.bike_type, b.model, b.hourly_rate, b.daily_rate
                FROM rentals r
                JOIN bikes b ON r.bike_id = b.id
                WHERE r.id = ? AND r.customer_id = ? AND r.status = 'ACTIVE'
            ''', (rental_id, self.current_customer.customer_id))

            rental_data = cursor.fetchone()

            if not rental_data:
                print("✗ Active rental not found!")
                return False

            # Extract data with correct indices
            (rental_id_db, customer_id, bike_id, rental_start, rental_end,
             planned_duration, base_cost, additional_charges, total_cost, status,
             bike_type, model, hourly_rate, daily_rate) = rental_data

            # Calculate actual duration and any additional charges
            rental_end_time = datetime.now()
            actual_duration = (rental_end_time - rental_start).total_seconds() / 3600  # in hours

            # Create bike object for cost calculation
            bike = BikeFactory.create_bike((bike_id, bike_type, model, hourly_rate, daily_rate))

            additional_charges_new = 0
            if actual_duration > planned_duration:
                overtime_hours = actual_duration - planned_duration
                additional_charges_new = overtime_hours * bike.hourly_rate * 1.5  # 50% penalty for overtime

            final_cost = base_cost + additional_charges_new

            # Update rental record
            cursor.execute('''
                UPDATE rentals SET 
                    rental_end = ?, 
                    actual_duration_hours = ?, 
                    additional_charges = ?, 
                    total_cost = ?,
                    status = 'COMPLETED'
                WHERE id = ?
            ''', (rental_end_time, actual_duration, additional_charges_new, final_cost, rental_id))

            # Mark bike as available
            cursor.execute('UPDATE bikes SET is_available = 1 WHERE id = ?', (bike_id,))

            conn.commit()

            print(f"\n✓ BIKE RETURNED SUCCESSFULLY!")
            print(f"   Rental ID: {rental_id}")
            print(f"   Bike: {bike}")
            print(f"   Planned Duration: {planned_duration} hours")
            print(f"   Actual Duration: {actual_duration:.2f} hours")
            if additional_charges_new > 0:
                print(f"   Overtime Charges: ${additional_charges_new:.2f}")
            print(f"   Final Cost: ${final_cost:.2f}")

            return True

        except Exception as e:
            conn.rollback()
            print(f"✗ Error processing return: {e}")
            return False
        finally:
            conn.close()

    def view_rental_history(self):
        if not self.current_customer:
            print("Please login first!")
            return

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT r.id, r.bike_id, r.rental_start, r.rental_end, r.planned_duration_hours,
                   r.actual_duration_hours, r.total_cost, r.status,
                   b.bike_type, b.model
            FROM rentals r
            JOIN bikes b ON r.bike_id = b.id
            WHERE r.customer_id = ?
            ORDER BY r.rental_start DESC
        ''', (self.current_customer.customer_id,))

        rentals = cursor.fetchall()
        conn.close()

        if not rentals:
            print("No rental history found.")
            return

        print(f"\n" + "=" * 80)
        print(f"RENTAL HISTORY FOR {self.current_customer.name}")
        print("=" * 80)

        for rental in rentals:
            (rental_id, bike_id, start_time, end_time, planned_duration,
             actual_duration, total_cost, status, bike_type, model) = rental

            print(f"\n📋 Rental ID: {rental_id}")
            print(f"   Bike: {bike_type} - {model}")
            print(f"   Start: {start_time.strftime('%Y-%m-%d %H:%M')}")
            if end_time:
                print(f"   End: {end_time.strftime('%Y-%m-%d %H:%M')}")
                print(f"   Duration: {actual_duration:.2f} hours")
            else:
                print(f"   Planned Duration: {planned_duration} hours")
            print(f"   Cost: ${total_cost:.2f}")
            print(f"   Status: {status}")

    def generate_business_report(self):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        # Total revenue
        cursor.execute('SELECT SUM(total_cost) FROM rentals WHERE status = "COMPLETED"')
        total_revenue = cursor.fetchone()[0] or 0

        # Total rentals
        cursor.execute('SELECT COUNT(*) FROM rentals')
        total_rentals = cursor.fetchone()[0]

        # Active rentals
        cursor.execute('SELECT COUNT(*) FROM rentals WHERE status = "ACTIVE"')
        active_rentals = cursor.fetchone()[0]

        # Most popular bike type
        cursor.execute('''
            SELECT b.bike_type, COUNT(*) as rental_count
            FROM rentals r
            JOIN bikes b ON r.bike_id = b.id
            GROUP BY b.bike_type
            ORDER BY rental_count DESC
            LIMIT 1
        ''')
        popular_bike = cursor.fetchone()

        # Average rental duration
        cursor.execute('SELECT AVG(actual_duration_hours) FROM rentals WHERE actual_duration_hours IS NOT NULL')
        avg_duration = cursor.fetchone()[0] or 0

        conn.close()

        print(f"\n" + "=" * 60)
        print("BUSINESS ANALYTICS REPORT")
        print("=" * 60)
        print(f"📊 Total Revenue: ${total_revenue:.2f}")
        print(f"📈 Total Rentals: {total_rentals}")
        print(f"🔄 Active Rentals: {active_rentals}")
        print(f"🚴 Most Popular Bike Type: {popular_bike[0] if popular_bike else 'N/A'}")
        print(f"⏱️  Average Rental Duration: {avg_duration:.2f} hours")


# Main application interface
def main():
    system = BikeRentalSystem()

    while True:
        print(f"\n" + "=" * 60)
        print("🚴 BIKE RENTAL MANAGEMENT SYSTEM 🚴")
        print("=" * 60)

        if system.current_customer:
            print(f"Logged in as: {system.current_customer.name}")

        print("\n1. Register New Customer")
        print("2. Login Customer")
        print("3. Rent a Bike")
        print("4. Return a Bike")
        print("5. Business Report")
        print("6. Exit")

        try:
            choice = input("\nEnter your choice (1-6): ").strip()

            if choice == '1':
                name = input("Enter name: ")
                email = input("Enter email: ")
                phone = input("Enter phone: ")
                system.register_customer(name, email, phone)

            elif choice == '2':
                email = input("Enter email: ")
                system.login_customer(email)

            elif choice == '3':
                if not system.current_customer:
                    print("Please login first!")
                    continue
                system.display_available_bikes()
                try:
                    bike_id = int(input("\nEnter bike ID to rent: "))
                    duration = int(input("Enter rental duration (hours): "))
                    system.rent_bike(bike_id, duration)
                except ValueError:
                    print("✗ Please enter valid numbers for bike ID and duration!")

            elif choice == '4':
                if not system.current_customer:
                    print("Please login first!")
                    continue
                try:
                    rental_id = int(input("Enter rental ID to return: "))
                    system.return_bike(rental_id)
                except ValueError:
                    print("✗ Please enter a valid rental ID!")

            elif choice == '5':
                system.generate_business_report()

            elif choice == '6':
                print("Thank you for using Bike Rental System!")
                break

            else:
                print("Invalid choice! Please try again.")

        except (ValueError, KeyboardInterrupt):
            print("\nInvalid input! Please try again.")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()