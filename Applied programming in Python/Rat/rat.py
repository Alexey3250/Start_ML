import turtle

# Create a turtle and set its speed to the maximum possible value
t = turtle.Turtle()
t.speed(0)

# Set the size of the rat image
t.shapesize(2, 2, 2)

# Load the rat image as the turtle's shape
t.shape("rat.gif")

# Rotate the turtle 45 degrees to the left
t.left(45)

# Move the turtle in a diagonal direction
t.forward(100)

# Continue rotating and moving the turtle in a diagonal direction indefinitely
while True:
    t.right(90)
    t.forward(100)
