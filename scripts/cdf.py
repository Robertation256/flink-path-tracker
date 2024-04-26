from typing import List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def read_log_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return content
    except FileNotFoundError:
        return "File does not exist"
    except Exception as e:
        return f"Exception raised during file reading: {e}"

def draw_latency_CDF(data_list: List[List[float]], labels: List[str], output: str, title: str, xlabel: str, ylabel: str):
    plt.figure(figsize=(8, 4))
    for data, label in zip(data_list, labels):
        data_sorted = np.sort(data)
        p = np.arange(1, len(data) + 1) / len(data)
        plt.step(data_sorted, p, where="post", label=label)

    plt.xlabel(xlabel, fontsize=20)
    plt.ylabel(ylabel, fontsize=20)
    plt.title(title, fontsize=24)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    plt.savefig(output)


if __name__ == '__main__':
    dataList = []
    labels = []

    log_name = '1million-cycles'
    conflux_metric = pd.read_csv("./{}.csv".format(log_name), dtype={
    'create_time': int,
    'process_completion_time': int,
    'sink_time': int,
    'consume_time': int,
    'heap_push_time': int,
    'emit_time': int
    })

    metric1_name = 'Source to Sink Latency'
    conflux_metric[metric1_name] = conflux_metric['process_completion_time'] - conflux_metric['create_time']
    dataList.append(conflux_metric[metric1_name].to_list())
    labels.append(metric1_name)

    metric2_name = "Source to Buffer Queue Latency"
    conflux_metric[metric2_name] = conflux_metric['consume_time'] - conflux_metric['create_time']
    dataList.append(conflux_metric[metric2_name].to_list())
    labels.append(metric2_name)

    metric3_name = "End-to-End Latency"
    conflux_metric[metric3_name] = conflux_metric['emit_time'] - conflux_metric['create_time']
    dataList.append(conflux_metric[metric3_name].to_list())
    labels.append(metric3_name)

    draw_latency_CDF(dataList, labels, "{}.png".format(log_name), title="Latency CDF", xlabel="Latency (microseconds)", ylabel="P(X ≤ time)")
