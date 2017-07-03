/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.postgresintl.logicaldecoding;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Created: 30/05/14
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class CountriesLanguage {
    private final Collection<String> countries;
    private final String language;
    private final double averagePercentage;

    public CountriesLanguage(String[] countries, String language, double averagePercentage) {
        this.countries = Collections.unmodifiableCollection(Arrays.asList(countries));
        this.language = language;
        this.averagePercentage = averagePercentage;
    }

    public Collection<String> getCountries() {
        return countries;
    }

    public String getLanguage() {
        return language;
    }

    public double getAveragePercentage() {
        return averagePercentage;
    }
}
