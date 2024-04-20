import random

# A list of common Indian names for demonstration purposes
names = [
    "Aarav", "Vihaan", "Vivaan", "Ananya", "Diya", "Ishaan", "Pranav", "Saanvi",
    "Arjun", "Veer", "Sara", "Riya", "Yash", "Aditi", "Kavya", "Rohan", "Mohit",
    "Anika", "Raj", "Simran", "Arnav", "Ayaan", "Krishna", "Surya", "Tanvi", "Gaurav",
    "Priya", "Ritika", "Nikhil", "Amit", "Rahul", "Sanjay", "Karan", "Varun", "Rishi",
    "Sidharth", "Abhinav", "Rajesh", "Kriti", "Pooja", "Deepa", "Lakshmi", "Aarti",
    "Parth", "Siddharth", "Manish", "Dinesh", "Anil", "Sunil", "Harish", "Gopal",
    "Jai", "Vikram", "Ravi", "Alok", "Vishal", "Sachin", "Kiran", "Narendra", "Lalit",
    "Avinash", "Suresh", "Harsha", "Anjali", "Chitra", "Esha", "Geeta", "Heena",
    "Indira", "Jyoti", "Kamala", "Leela", "Mina", "Nina", "Omana", "Punita", "Radha",
    "Shanti", "Tara", "Uma", "Vani", "Yamini", "Zara", "Bhavna", "Chandni", "Dhara",
    "Falguni", "Gargi", "Hansa", "Ilina", "Jasmin", "Kalpana", "Lalima", "Mehak",
    "Neha", "Ojasvi", "Pallavi", "Rashmi", "Smita", "Tanisha", "Urvashi", "Vidya"
]

# Generate 100 unique random integers for keys
keys = random.sample(range(1, 10000), 100)

# Create SQL INSERT commands
sql_commands = [f"INSERT INTO employee VALUES({key}, '{name}');" for key, name in zip(keys, names)]

for sql in sql_commands:
    print(sql)
    print("\n")
