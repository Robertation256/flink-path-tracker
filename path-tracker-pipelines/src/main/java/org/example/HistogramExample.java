package org.example;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.statistics.HistogramDataset;

import javax.swing.*;

import java.awt.*;

public class HistogramExample extends JFrame {

    public HistogramExample(String title, double[] data, int bins, int fontSize) {
        super(title);
        createHistogram(title, data, bins, "Throughput (records/second)", fontSize);
    }

    private double[] convertToDouble(long[] data) {
        double[] doubleData = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            doubleData[i] = (double) data[i];
        }
        return doubleData;
    }


    public HistogramExample(String title, long[] data, int bins, int fontSize) {
        super(title);
        double[] doubleData = convertToDouble(data);
        createHistogram(title, doubleData, bins, "Latency (ms)", fontSize);
    }


    private void createHistogram(String title, double[] data, int bins, String xAxisText, int fontSize) {
        HistogramDataset dataset = new HistogramDataset();
        dataset.addSeries("Data", data, bins);

        // Create a histogram
        JFreeChart chart = ChartFactory.createHistogram(
                title,
                xAxisText,
                "Frequency",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                false,
                false
        );
        chart.getXYPlot().getDomainAxis().setTickLabelFont(new Font("SansSerif", Font.PLAIN, fontSize));
        // Get the plot and set the font size for the range axis (Y-axis)
        chart.getXYPlot().getRangeAxis().setTickLabelFont(new Font("SansSerif", Font.PLAIN, fontSize));


        // Create a chart panel
        ChartPanel chartPanel = new ChartPanel(chart);

        // Set size
        chartPanel.setPreferredSize(new Dimension(1600, 1200));

        // Add chart panel to JFrame
        setContentPane(chartPanel);
    }
}
