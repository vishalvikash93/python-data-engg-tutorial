class Bird:
    def fly(self):
        print("Bird is flying")

class Airplane:
    def fly(self):
        print("Airplane is flying")

# Polymorphism through duck typing
def make_it_fly(thing):
    thing.fly()  # Calls the fly method on any object that has a fly method

# Creating instances of Bird and Airplane
bird = Bird()
plane = Airplane()

# Both objects can be passed to the make_it_fly function because they both have a fly method
make_it_fly(bird)   # Outputs: Bird is flying
make_it_fly(plane)  # Outputs: Airplane is flying
