/*
Написать процедуру
На вход получает
Connection1 соединение с одной БД
Connection2 соединение с другой БД
Select – строка по выборке в первой БД например select name,address from my_scheme.clients
Insert для второй БД например insert into second_scheme.table2(name2,add2)


Написать java код который перенесет все строки выборки из одной БД в другую
 */

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws SQLException {
        final Connection connection1 = DriverManager.getConnection
                ("jdbc:h2:./base1", "user1", "password1");

        final Connection connection2 = DriverManager.getConnection
                ("jdbc:h2:./base2", "user2", "password2");

        dataCopy(connection1,
                connection2,
                "select name, address from sch.clients1",
                "insert into second_scheme.table2(name2,addr2)");


//        String selectQuery = "SELECT * FROM public.clients1";
//        PreparedStatement statement = connection1.prepareStatement(selectQuery);
//        boolean hasResult = statement.execute();
//
//        ResultSet resultSet = statement.getResultSet();
//
//        while (resultSet.next()) {
//            System.out.println(resultSet.getString("name") + "\t" + resultSet.getString("address"));
////            System.out.println(resultSet.getObject("int_field"));
//        }
//
//        DatabaseMetaData metaData = connection1.getMetaData();
//        resultSet = metaData.getTables("BASE1", "PUBLIC", "%", null);
////        while (resultSet.next()){
//            resultSet.next();
//            System.out.println(resultSet.getRowId(2));
//            System.out.println(resultSet.getString(1));
//            System.out.println(resultSet.getString(2));
//            System.out.println(resultSet.getString(3));
//            System.out.println(resultSet.getString(4));
//            System.out.println(resultSet.getString(5));
//            System.out.println(resultSet.getString(7));
////        }
//
////        System.out.println(statement.execute());
//

    }

    private static void dataCopy(Connection conn1, Connection conn2, String selectQuery, String insertQuery) {
        // получаем поля, которые надо скопировать

        // парсинг Select
        String selectFieldsStr = selectQuery.toLowerCase();
        int start = selectFieldsStr.indexOf("select") + "select".length();
        int stop = selectFieldsStr.indexOf("from");
        selectFieldsStr = selectFieldsStr.substring(start, stop);

        String[] selectFields = Arrays.stream(selectFieldsStr.split(","))
                .map(String::strip)
                .toArray(String[]::new);

        // парсинг Insert
        String insertFieldsStr = insertQuery.toLowerCase();
        start = insertFieldsStr.indexOf("into") + 4;
        insertFieldsStr = insertQuery.substring(start);
        start = insertFieldsStr.indexOf('(') + 1;
        insertFieldsStr = insertFieldsStr.substring(start);
        insertFieldsStr = insertFieldsStr.replaceAll("[)]", "");

        String[] insertFields = Arrays.stream(insertFieldsStr.split(","))
                .map(String::strip)
                .toArray(String[]::new);

        // если кол-во полей для выборки и вставки не совпало - ошибка
        if (selectFields.length != insertFields.length){
            throw new IllegalArgumentException("Select and Insert field qty must be the same");
        }

        try(
                PreparedStatement selStatement = conn1.prepareStatement(selectQuery);
                PreparedStatement insStatement = conn2.prepareStatement(insertQuery);
        ) {

            boolean hasResult = selStatement.execute();
            ResultSet resultSet = selStatement.getResultSet();

            while (resultSet.next()) {
                System.out.println(resultSet.getString("name") + "\t" + resultSet.getString("address"));
//            System.out.println(resultSet.getObject("int_field"));
            }
        }catch (SQLException e){

        }



        System.out.println(Arrays.toString(insertFields));


    }
}
