import matplotlib.pyplot as plt
import csv
import statistics
import numpy as np


def parse_data(filename):
    data = []
    with open(filename, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            timestamp, value = row
            data.append((float(timestamp), float(value)))  # Convert to int and float

    minTimestamp = min(x[0] for x in data)
    normalizedData = []

    for timestamp, value in data:
        normalizedData.append((timestamp - minTimestamp, value))

    return normalizedData




def plot_bar_chart(data, title):
    timestamps, values = zip(*data)
    plt.bar(timestamps, values, color="skyblue")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.title(title)
    plt.show()


def make_statistics(values):
    # Calculate statistics
    mean_value = statistics.mean(values)
    std_dev = statistics.stdev(values)
    variance = statistics.variance(values)
    percentile_99 = np.percentile(values, 99)

    # Create a bar chart
    print(f"Mean: {mean_value} Std Dev: {std_dev} Variance: {variance} 99th Percentile: {percentile_99}")


latency_data = parse_data("latency.txt")
throughput_data = parse_data("throughput.txt")

latency_no_timestamp = [x[1] for x in latency_data]
throughout_no_timestamp = [x[1] for x in throughput_data]

make_statistics(latency_no_timestamp)
make_statistics(throughout_no_timestamp)


plot_bar_chart(latency_data, "Latency Data")
plot_bar_chart(throughput_data, "Throughput Data")


