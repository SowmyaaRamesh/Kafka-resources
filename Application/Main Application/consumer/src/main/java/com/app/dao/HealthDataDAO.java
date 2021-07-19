package com.app.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.app.model.HealthData;
import com.app.util.DatabaseConnection;

public class HealthDataDAO {
    private static Connection connection;
    private static DatabaseConnection DBConnectionInstance;

    static {
        try {
            DBConnectionInstance = DatabaseConnection.getInstance();
            connection = DBConnectionInstance.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void addRecords(HealthData record){

        String year = record.getYear();
        String brandName = record.getBrandName();
        String genericName = record.getGenericName();
        String coverageType = record.getCoverageType();
        Double totalSpending = Double.parseDouble(record.getTotalSpending());
        int sno = Integer.parseInt(record.getSno());

        System.out.println(year+" "+brandName+" "+genericName+" "+coverageType+" "+totalSpending+" "+sno);

        try {

            PreparedStatement stmt=connection.prepareStatement("Insert into health_data values(?,?,?,?,?,?)");

            stmt.setInt(1,sno);
            stmt.setString(2,year);
            stmt.setString(3,brandName);
            stmt.setString(4,genericName);
            stmt.setString(5,coverageType);
            stmt.setDouble(6,totalSpending);

            int result = stmt.executeUpdate();
            if(result == 1){
                System.out.println("Insertion successful.");
            }

        }catch(SQLException se) {
            se.printStackTrace();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}
