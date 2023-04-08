import simpy
from datetime import datetime
from pyspark.sql import SparkSession

simulation_start_date = "2023-01-01"

REFERENCE_DATE = datetime(2023, 1, 1)  # Adjust this to the appropriate date for your simulation

customer_min_inventory = 5
customer_max_inventory = 7

def datetime_to_days(date):
    return (date - REFERENCE_DATE).days

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")


def manufacturer(env, distribution_centers, shipment_data):
    for row in shipment_data:
        sku, num_units, arrival_date, ship_date, distribution_center_arrival_date, units_to_customers, customer_arrival_date = row
        
        # Produce the items
        for _ in range(num_units):
            # Send the item to a random distribution center
            manufacturere = manufactureres[0]
            manufacturere.put((sku, env.now))
            print(f'Time {env.now}: Manufacturer {id(manufacturere)} produced an item ({sku}).')

        # Wait until the shipment date
        yield env.timeout(datetime_to_days(parse_date(ship_date)) - env.now)

        for dc, units, customer_inventory in zip(distribution_centers, units_to_customers, customer_inventories):
        
            env.process(distribution_partner(env, dc, customer_inventory, units, distribution_center_arrival_date, customer_arrival_date, sku))


def distribution_partner(env, distribution_center, customer_inventory, units, distribution_center_arrival_date, customer_arrival_date, sku):
    # Wait until the distribution center arrival date
    yield env.timeout(datetime_to_days(parse_date(distribution_center_arrival_date)) - env.now)
    # Increase the customer inventory level
    # Receive the items
    for _ in range(units):
        distribution_center.put((sku, env.now))
        print(f'Time {env.now}: Distribution partner {id(distribution_center)} receive an item ({sku}).')
    
    # TODO yield for the distrubtion center ship date and decrement inventory
    
    # Call the customer function
    env.process(customer(env, customer_inventory, units, customer_arrival_date, sku))


def customer(env, customer_inventory, units, customer_arrival_date, sku):
    # Wait until the distribution center arrival date
    yield env.timeout(datetime_to_days(parse_date(customer_arrival_date)) - env.now)
    # Receive the items
    for _ in range(units):
        customer_inventory.put((sku, env.now))
        print(f'Time {env.now}: Customer {id(customer_inventory)} receive an item ({sku}).')
    # Increase the customer inventory level
    customer_inventory.put(units)

     # Check if the inventory is within the desired range
    inventory_level = len(customer_inventory.items)
    print(f'Time {env.now}: Customer {id(customer_inventory)} inventory is: {inventory_level}.')
    if inventory_level < customer_min_inventory:
        print(f'Time {env.now}: Customer {id(customer_inventory)} inventory below minimum, need to order more items.')
    elif inventory_level > customer_max_inventory:
        print(f'Time {env.now}: Customer {id(customer_inventory)} inventory above maximum, need to stop ordering items.')
    else:
        print(f'Time {env.now}: Customer {id(customer_inventory)} inventory within acceptable range.')


# Create a sample PySpark DataFrame
spark = SparkSession.builder.master("local").appName("Inventory Simulation").getOrCreate()

data = [
    ("sku1", 5, "2023-01-01", "2023-01-02", "2023-01-04", [2, 1, 2], "2023-01-08"),
    ("sku2", 7, "2023-01-02", "2023-01-03", "2023-01-06", [3, 2, 2], "2023-01-11"),
    ("sku3", 3, "2023-01-03", "2023-01-05", "2023-01-08", [1, 1, 1], "2023-01-15"),
]

schema = "sku string, number_of_units int, date_of_arrival_at_manufacturer string, ship_to_distribution_center string, distribution_center_arrival_date string, units_to_customers array<int>, customer_arrival_date string"

df = spark.createDataFrame(data, schema=schema)
shipment_data = df.collect()

# Create the simulation environment
simulation_start_date_datetime = parse_date(simulation_start_date)
env = simpy.Environment(initial_time=datetime_to_days(simulation_start_date_datetime))

# Create the notional entities
num_manufacturers = 1
num_distribution_partners = 3
num_customers = 3

manufactureres = [simpy.Store(env) for _ in range(num_manufacturers)]
distribution_centers = [simpy.Store(env) for _ in range(num_distribution_partners)]
customer_inventories = [simpy.Store(env) for _ in range(num_customers)]

# Find the maximum date in the DataFrame
max_date = "2023-01-15"

# Convert the maximum date to days from the start date
simulation_time = (parse_date(max_date) - parse_date("2023-01-01")).days

# Set up the environment
env = simpy.Environment()

# Add the processes to the environment
for _ in range(num_manufacturers):
    env.process(manufacturer(env, distribution_centers, shipment_data))

# Run the simulation
env.run(until=simulation_time + 1)
