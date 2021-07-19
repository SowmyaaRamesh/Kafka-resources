package com.app.util;


import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnection {

    private static Connection connection;
    private static DatabaseConnection instance;

    static
    {
        try {
            String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
            String DB_URL="jdbc:mysql://localhost:3306/test";

            FileReader reader=new FileReader("C:\\Users\\Sowmya\\IdeaProjects\\consumer\\src\\main\\java\\db.properties");

            Properties p=new Properties();
            p.load(reader);

            // Database credentials
            String USER = p.getProperty("user");
            String PASS = p.getProperty("password");

            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connection successful.");
        }catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public static DatabaseConnection getInstance() throws SQLException {
        if (instance == null) {
            instance = new DatabaseConnection();
        } else if (instance.getConnection().isClosed()) {
            instance = new DatabaseConnection();
        }

        return instance;
    }
}