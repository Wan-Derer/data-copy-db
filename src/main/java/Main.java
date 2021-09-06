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
import java.time.LocalDateTime;
import java.util.Arrays;

public class Main {
  public static void main(String[] args) throws SQLException {
    final Connection connection1 = DriverManager.getConnection
      ("jdbc:h2:./base1", "user1", "password1");

    final Connection connection2 = DriverManager.getConnection
      ("jdbc:h2:./base2", "user2", "password2");

    dataCopy(connection1,
      connection2,
      "select name,address from sch1.clients1",
      "insert into sch2.clients2(name2,addr2)");


  }

  private static void dataCopy(Connection conn1, Connection conn2, String selectQuery, String insertQuery) {

    // получаем поля, которые надо скопировать
    // парсинг Select
    String[] selectFields = Arrays.stream(selectQuery.toLowerCase().split("[ ,()]"))
      .dropWhile(s -> !"select".equals(s))
      .skip(1)
      .takeWhile(s -> !"from".equals(s))
      .filter(s -> !s.isEmpty())
      .toArray(String[]::new);

    try (PreparedStatement selStatement = conn1.prepareStatement(selectQuery)) {

      selStatement.execute();
      ResultSet resultSet = selStatement.getResultSet();

      // при выборке из БД любое поле может быть представлено
      // как String или Object независимо от его реального типа
      StringBuilder insQueryBuilder;

      while (resultSet.next()) {
        insQueryBuilder = new StringBuilder(insertQuery);
        insQueryBuilder.setLength(insQueryBuilder.lastIndexOf(")"));
        insQueryBuilder.append(", updated_at)\n values ('");    // добавляем дату/время вставки данных

        for (String selectField : selectFields) {
          insQueryBuilder.append(resultSet.getString(selectField)).append("', '");
        }

        insQueryBuilder.append(LocalDateTime.now()).append("');\n");

        PreparedStatement insStatement = conn2.prepareStatement(insQueryBuilder.toString());
        insStatement.execute();
        insStatement.close();

//        System.out.println(insQueryBuilder);
      }

    } catch (SQLException e) {
      // здесь могут возникнуть ошибки несоответствия типов полей выборки и вставки, синтаксические ошибки в
      // пришедших строках-запросах, просто ошибки SQL. Вопрос - что с ними делать (помимо логирования). Можно
      // сформировать своё исключение и или "пробросить" возникшее, или обработать как-то ещё

      e.printStackTrace();
    }


  }
}
