import matplotlib.pyplot as plt

# Data for plotting
parallelism_levels = [256, 128, 64, 32]
execution_times = [13.0, 10.1, 9.3, 10.1]

# Create a line graph
plt.figure(figsize=(10, 5))
plt.plot(parallelism_levels, execution_times, marker='o', linestyle='-', color='b')

# Setting graph labels and title
plt.xlabel('Parallelism Levels')
plt.ylabel('Execution Time (seconds)')
plt.title('Execution Time vs. Parallelism Levels')
plt.grid(True)

# Set the x-axis to have the labels and match them with the data points
plt.xticks(parallelism_levels)
plt.gca().invert_xaxis()  # Invert x axis to show the highest level of parallelism on the left

# Show the plot
plt.show()
