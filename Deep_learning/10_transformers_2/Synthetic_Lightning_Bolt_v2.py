# -*- coding: utf-8 -*-
"""
Created on Thu Dec 29 15:57:14 2022

@author: Shane
"""

import numpy as np
import matplotlib.pyplot as plt

# Set up the transition matrix
transition_matrix = np.array([[-0.9, 0.1, 0, 0, 0],  # from forming to forming, moving, splitting, striking, discharging
                              [0, -0.9, 0.1, 0, 0],  # from moving to forming, moving, splitting, striking, discharging
                              [0, 0, -0.9, 0.1, 0],  # from splitting to forming, moving, splitting, striking, discharging
                              [0, 0, 0, -0.9, 0.1],  # from striking to forming, moving, splitting, striking, discharging
                              [0, 0, 0, 0, 0]])  # from discharging to all states

# Set up the initial probabilities
initial_probabilities = np.array([1, 0, 0, 0, 0])  # starts in the forming state

# Set up the time steps
time_steps = np.linspace(0, 10, 101)

# Calculate the probabilities at each time step
probabilities = []
for t in time_steps:
    probabilities.append(initial_probabilities @ np.linalg.matrix_power(transition_matrix, int(t / 0.1)))

# Extract the probabilities of each state
forming_probabilities = [p[0] for p in probabilities]
moving_probabilities = [p[1] for p in probabilities]
splitting_probabilities = [p[2] for p in probabilities]
striking_probabilities = [p[3] for p in probabilities]
discharging_probabilities = [p[4] for p in probabilities]

# Plot the results
plt.plot(time_steps, forming_probabilities, label="Forming")
plt.plot(time_steps, moving_probabilities, label="Moving")
plt.plot(time_steps, splitting_probabilities, label="Splitting")
plt.plot(time_steps, striking_probabilities, label="Striking")
plt.plot(time_steps, discharging_probabilities, label="Discharging")
plt.xlabel("Time (s)")
plt.ylabel("Probability")
plt.legend()
plt.ylim(0, 1)  # set the y-axis limits to 0-100%
plt.yticks(np.arange(0, 1.1, 0.1))  # set the y-axis tick marks to 0%, 10%, ..., 100%
plt.grid()  # show a grid
plt.show()
