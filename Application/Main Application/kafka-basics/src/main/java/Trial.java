
import java.sql.*;

class SqlWriter{
    public static void writeToDb(String item){
        int i = item.lastIndexOf('[');
        String valuesStr = item.substring(i+1);
        valuesStr = valuesStr.substring(0,valuesStr.length()-2);

        String[] valuesList = valuesStr.split(", ");

        String year = valuesList[0];
        String brandName = valuesList[1];
        String genericName = valuesList[2];
        String coverageType = valuesList[3];
        Double totalSpending = Double.parseDouble(valuesList[4]);
        System.out.println(totalSpending);
        int sno = Integer.parseInt(valuesList[18]);

        System.out.println(year+" "+brandName+" "+genericName+" "+coverageType+" "+totalSpending+" "+sno);

        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL="jdbc:mysql://localhost:3306/test";

        // Database credentials
        String USER = "root";
        String PASS = "root";

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);


            PreparedStatement stmt=conn.prepareStatement("Insert into health_data values(?,?,?,?,?,?)");

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

            conn.close();

        }catch(SQLException se) {
            se.printStackTrace();
        }catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        }
    }
}

public class Trial {
    public static void main(String[] args) {
        String item = "CSVRecord [comment='null', recordNumber=27, values=[2014, Advair Diskus, Fluticasone/Salmeterol, Part D, 2276374749, 1420748, 460372139, 211936509.1, 1602.24, 4.94, , 6094244, 668600, 752148, 12718030.25, 199218478.9, 19.02, 264.87, 27]]";
        SqlWriter.writeToDb(item);
        item ="CSVRecord [comment='null', recordNumber=29, values=[2011, Afinitor, Everolimus, Part D, 45140530.91, 1573, 179682, 2290729.94, 28697.1, 251.58, , 6076, 620, 953, 36885.89, 2253844.05, 59.49, 2365, 29]]";
        SqlWriter.writeToDb(item);
    }
}
