from abc import ABC, abstractmethod

# Abstract base class
class Animal(ABC):
    @abstractmethod
    def sound(self):
        pass

    @abstractmethod
    def move(self):
        pass

# Concrete subclass of Animal
class Dog(Animal):
    def sound(self):
        return "Bark"

    def move(self):
        return "Run"

# Another concrete subclass of Animal
class Bird(Animal):
    def sound(self):
        return "Chirp"

    def move(self):
        return "Fly"

# Creating instances of Dog and Bird
dog = Dog()
bird = Bird()

# Using polymorphism and abstraction
print(f"Dog sound: {dog.sound()}")  # Outputs: Dog sound: Bark
print(f"Bird sound: {bird.sound()}")  # Outputs: Bird sound: Chirp

print(f"Dog movement: {dog.move()}")  # Outputs: Dog movement: Run
print(f"Bird movement: {bird.move()}")  # Outputs: Bird movement: Fly






# Explanation:
# Animal is an abstract class (it cannot be instantiated directly) and has two abstract methods: sound() and move().
# Dog and Bird are concrete subclasses that provide implementations for the sound() and move() methods.
# The user of the class only needs to know about the sound() and move() methods, without being concerned about how they are implemented.