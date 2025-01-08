class Account:
    def __init__(self, owner, balance=0):
        self.owner = owner
        self.__balance = balance  # Private attribute (indicated by __)

    # Getter method to access private balance
    def get_balance(self):
        return self.__balance

    # Setter method to modify the private balance
    def deposit(self, amount):
        if amount > 0:
            self.__balance += amount
        else:
            print("Deposit amount must be positive.")

    def withdraw(self, amount):
        if 0 < amount <= self.__balance:
            self.__balance -= amount
        else:
            print("Invalid withdrawal amount.")

# Creating an object of Account
account = Account("John Doe", 1000)
# account=10001
print(account.withdraw(1010))
# Accessing the balance through the getter method
# print(account.get_balance())  # Outputs: 1000
#
# # Modifying the balance through the setter methods
# account.deposit(500)  # Deposits 500
# print(account.get_balance())  # Outputs: 1500
#
# account.withdraw(200)  # Withdraws 200
# print(account.get_balance())  # Outputs: 1300

# Trying to access or modify the balance directly would result in an error:
# print(account.__balance)  # This would raise an AttributeError




# The __balance attribute is private, meaning it cannot be accessed directly from outside the class.
# You interact with __balance through public methods like get_balance(), deposit(), and withdraw().
# This encapsulates the data and ensures that the balance can only be modified in a controlled manner, preserving its integrity.


