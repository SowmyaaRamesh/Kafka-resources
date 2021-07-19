import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;


public class SqlWriter{
    private static Connection conn;
    public static void setUpDatabaseConnection(){

        try {
            String JDBC_DRIVER = "com.mysql.jdbc.Driver";
            String DB_URL="jdbc:mysql://localhost:3306/test";

            FileReader reader=new FileReader("C:\\Users\\Sowmya\\IdeaProjects\\consumer\\src\\main\\java\\db.properties");

            Properties p=new Properties();
            p.load(reader);

            // Database credentials
            String USER = p.getProperty("user");
            String PASS = p.getProperty("password");

            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connection successful.");
        }catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        }
    }

    public static void closeDatabaseConnection(){
        try {
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void writeToDb(ArrayList<String> valuesList){

        String year = valuesList.get(0);
        String brandName = valuesList.get(1);
        String genericName = valuesList.get(2);
        String coverageType = valuesList.get(3);
        Double totalSpending = Double.parseDouble(valuesList.get(4));
        int sno = Integer.parseInt(valuesList.get(5));

        System.out.println(year+" "+brandName+" "+genericName+" "+coverageType+" "+totalSpending+" "+sno);



//        try {
//
//            PreparedStatement stmt=conn.prepareStatement("Insert into health_data values(?,?,?,?,?,?)");
//
//            stmt.setInt(1,sno);
//            stmt.setString(2,year);
//            stmt.setString(3,brandName);
//            stmt.setString(4,genericName);
//            stmt.setString(5,coverageType);
//            stmt.setDouble(6,totalSpending);
//
//
//            int result = stmt.executeUpdate();
//            if(result == 1){
//                System.out.println("Insertion successful.");
//            }
//
//
//
//        }catch(SQLException se) {
//            se.printStackTrace();
//        }catch(Exception e) {
//            //Handle errors for Class.forName
//            e.printStackTrace();
//        }
    }
}