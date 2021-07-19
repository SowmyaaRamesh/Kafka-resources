package com.app.model;

public class HealthData {
    private String year;
    private String brandName;
    private String genericName;
    private String coverageType;
    private String totalSpending;
    private String sno;

    public HealthData() {

    }

    public HealthData(String year, String brandName, String genericName, String coverageType, String totalSpending, String sno) {
        this.year = year;
        this.brandName = brandName;
        this.genericName = genericName;
        this.coverageType = coverageType;
        this.totalSpending = totalSpending;
        this.sno = sno;
    }
    public String getBrandName() {
        return brandName;
    }
    public String getYear() {
        return year;
    }
    public String getGenericName() {
        return genericName;
    }
    public String getCoverageType(){
        return coverageType;
    }
    public String getTotalSpending(){
        return totalSpending;
    }
    public String getSno(){
        return sno;
    }

}
