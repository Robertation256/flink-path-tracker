package org.example;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.statistics.HistogramDataset;

import javax.swing.*;

import java.awt.*;

public class HistogramExample extends JFrame {

    public HistogramExample(String title, double[] data, int bins) {
        super(title);
        createHistogram(title, data, bins, "throughput");
    }

    private double[] convertToDouble(long[] data) {
        double[] doubleData = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            doubleData[i] = (double) data[i];
        }
        return doubleData;
    }


    public HistogramExample(String title, long[] data, int bins) {
        super(title);
        double[] doubleData = convertToDouble(data);
        createHistogram(title, doubleData, bins, "Latency");
    }


    private void createHistogram(String title, double[] data, int bins, String xAxisText) {
        HistogramDataset dataset = new HistogramDataset();
        dataset.addSeries("Data", data, bins);

        // Create a histogram
        JFreeChart chart = ChartFactory.createHistogram(
                title,
                xAxisText,
                "Frequency",
                dataset
        );

        // Create a chart panel
        ChartPanel chartPanel = new ChartPanel(chart);

        // Set size
        chartPanel.setPreferredSize(new Dimension(800, 600));

        // Add chart panel to JFrame
        setContentPane(chartPanel);
    }
}
