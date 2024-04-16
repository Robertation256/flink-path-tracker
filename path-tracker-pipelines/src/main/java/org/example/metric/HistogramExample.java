/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.example.metric;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
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
