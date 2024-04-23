#
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
#                                                 * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.


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


def downsample_data(data, max_points=1000):
    if len(data) <= max_points:
        return data
    else:
        indices = np.round(np.linspace(0, len(data) - 1, max_points)).astype(int)
        return [data[i] for i in indices]


def plot_histogram(values, title):
    plt.hist(values, bins=15, edgecolor="black")
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.title(title)
    plt.grid(True)
    plt.show()


def plot_dot_plot(data, title):
    downsampled_data = downsample_data(data)
    timestamps, values = zip(*downsampled_data)
    plt.plot(timestamps, values, "o", color="skyblue", markersize=3)
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
    print(
        f"Mean: {mean_value} Std Dev: {std_dev} Variance: {variance} 99th Percentile: {percentile_99}"
    )


latency_data = parse_data("latency.txt")
throughput_data = parse_data("throughput.txt")

latency_no_timestamp = [x[1] for x in latency_data]
throughout_no_timestamp = [x[1] for x in throughput_data]

make_statistics(latency_no_timestamp)
make_statistics(throughout_no_timestamp)

plot_histogram(latency_no_timestamp, "Latency Histogram")
plot_histogram(throughout_no_timestamp, "Throughput Histogram")

plot_dot_plot(latency_data, "Latency Data")
plot_dot_plot(throughput_data, "Throughput Data")
