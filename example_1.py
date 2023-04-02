import simpy
import random
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


# Create a sample PySpark DataFrame
spark = SparkSession.builder.master("local").appName("Inventory Simulation").getOrCreate()

data = [
    ("sku1", 5, "2023-01-01", "2023-01-02", "2023-01-04", [2, 1, 2]),
    ("sku2", 7, "2023-01-02", "2023-01-03", "2023-01-06", [3, 2, 2]),
    ("sku3", 3, "2023-01-03", "2023-01-05", "2023-01-08", [1, 1, 1]),
]

schema = "sku string, number_of_units int, date_of_arrival_at_manufacturer string, ship_to_distribution_center string, distribution_center_arrival_date string, units_to_customers array<int>"

df = spark.createDataFrame(data, schema=schema)

# Parameters
num_manufacturers = 2
num_distribution_partners = 2
num_customers = 3

customer_min_inventory = 5
customer_max_inventory = 15
simulation_time = 100

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

def manufacturer(env, distribution_centers, shipment_data):
    for row in shipment_data:
        sku, num_units, arrival_date, ship_date, distribution_center_arrival_date, units_to_customers = row
        
        # Wait until the date of arrival at the manufacturer
        # yield env.timeout((parse_date(arrival_date) - datetime.timedelta(days=env.now)).days)
        
        # Produce the items
        for _ in range(num_units):
            # Send the item to a random distribution center
            selected_dc = random.choice(distribution_centers)
            selected_dc.put((sku, env.now))
            print(f'Time {env.now}: Manufacturer {id(env.active_process)} produced an item ({sku}).')
        
        # Wait until the date to ship to the distribution center
        # yield env.timeout((parse_date(ship_date) - env.now).days)
        # Wait a while before sending inventory
        yield env.timeout(1)

def distribution_partner(env, distribution_centers, customer_inventories, shipment_data):
    for row in shipment_data:
        sku, num_units, _, _, distribution_center_arrival_date, units_to_customers = row
        
        # Wait until the date of arrival at the distribution center
        # yield env.timeout((parse_date(distribution_center_arrival_date) - env.now).days)
        
        # Process the items at the distribution center
        for idx, units in enumerate(units_to_customers):
            for _ in range(units):
                # Choose a random distribution center
                selected_dc = random.choice(distribution_centers)
                yield selected_dc.get()
                print(f'Time {env.now}: Distribution partner {id(env.active_process)} received an item ({sku}).')
                
                # Send the item to the specified customer
                yield env.timeout(random.randint(1, 3))
                selected_customer = customer_inventories[idx]
                selected_customer.put((sku, env.now))
                print(f'Time {env.now}: Customer {id(selected_customer)} received an item ({sku}).')

def customer(env, customer_inventory):
    while True:
        # Check if the inventory is within the desired range
        current_inventory = len(customer_inventory.items)
        if current_inventory < customer_min_inventory:
            print(f'Time {env.now}: Customer {id(customer_inventory)} inventory below minimum, need to order more items.')
        elif current_inventory > customer_max_inventory:
            print(f'Time {env.now}: Customer {id(customer_inventory)} inventory above maximum, need to stop ordering items.')
        else:
            print(f'Time {env.now}: Customer {id(customer_inventory)} inventory within acceptable range.')
        
        # Wait a while before checking the inventory again
        yield env.timeout(1)

# Create the simulation environment
env = simpy.Environment()

# Collect data from the DataFrame
shipment_data = df.collect()

# Create the notional entities
distribution_centers = [simpy.Store(env) for _ in range(num_distribution_partners)]
customer_inventories = [simpy.Store(env) for _ in range(num_customers)]

# Add the processes to the environment
for _ in range(num_manufacturers):
    env.process(manufacturer(env, distribution_centers, shipment_data))

for _ in range(num_distribution_partners):
    env.process(distribution_partner(env, distribution_centers, customer_inventories, shipment_data))

for customer_inventory in customer_inventories:
    env.process(customer(env, customer_inventory))

# Run the simulation
env.run(until=simulation_time)
