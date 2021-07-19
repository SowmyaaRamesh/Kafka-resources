package com.app.util;

import com.app.model.HealthData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Convertor {
    public static HealthData convertJsonToHealthDataObj(String jsonRecord) {
        ObjectMapper mapper = new ObjectMapper();
        HealthData healthRecord = new HealthData();
        try {
            healthRecord= mapper.readValue(jsonRecord,HealthData.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return healthRecord;
    }
}
