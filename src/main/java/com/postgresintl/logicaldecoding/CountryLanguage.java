/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.postgresintl.logicaldecoding;
/**
 * Created: 14/03/17
 *
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class CountryLanguage {
    private final String countryCode;
    private final String language;
    private final boolean isOfficial;
    private final double percentage;

    public CountryLanguage(String countryCode, String language, boolean isOfficial, double percentage) {
        this.countryCode = countryCode;
        this.language = language;
        this.isOfficial = isOfficial;
        this.percentage = percentage;
    }

    public String getCountryCode() {
      return countryCode;
    }

    public String getLanguage() {
        return language;
    }

    public boolean isOfficial() {
      return isOfficial;
    }

    public double getPercentage() {
        return percentage;
    }
}
