from class_object_example import Car

class ElectricCar(Car):  # Inherit from Car class
    def __init__(self, make, model, year, battery_size):
        super().__init__(make, model, year)  # Call the parent class constructor
        self.battery_size = battery_size  # New attribute specific to ElectricCar

    def display_battery_info(self):
        print(f"This car has a {self.battery_size}-kWh battery.")

# Creating an instance of ElectricCar
my_electric_car = ElectricCar("Tesla", "Model S", 2023, 100)

# Accessing methods from both the parent (Car) and child (ElectricCar) classes
my_electric_car.display_info()        # From Car class
my_electric_car.display_battery_info()  # From ElectricCar class
