# Python class is a blueprint for creating objects (instances).
# It defines a set of attributes (variables) and methods (functions) that the objects
# created from the class will have. A class encapsulates data for the object and functions
# that operate on that data.

class Car:
    # Constructor method to initialize the object
    def __init__(self, make, model, year):
        self.make = make  # Attribute for the car's make
        self.model = model  # Attribute for the car's model
        self.year = year  # Attribute for the car's year

    # Method to display car information
    def display_info(self):
        print(f"{self.year} {self.make} {self.model}")

    # Method to start the car
    def start(self):
        print(f"The {self.year} {self.make} {self.model} is starting.")


# Creating an object (instance) of the Car class
my_car = Car("Toyota", "Corolla", 2020)

# Accessing the methods of the object
my_car.display_info()  # Outputs: 2020 Toyota Corolla
my_car.start()  # Outputs: The 2020 Toyota Corolla is starting.
