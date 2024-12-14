class Animal:
    def speak(self):
        print("Animal speaks")

class Dog(Animal):
    def speak(self):
        print("Dog barks")

class Cat(Animal):
    def speak(self):
        print("Cat meows")

# Creating instances of Dog and Cat
dog = Dog()
cat = Cat()

# Polymorphism: Both objects respond to the same method in their own way
dog.speak()  # Outputs: Dog barks
cat.speak()  # Outputs: Cat meows
