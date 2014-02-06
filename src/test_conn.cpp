#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <mysql_driver.h>
#include <mysql_connection.h>
/* Connection parameter and sample data */

#include <iostream>
#include <memory>
#include <sstream>

using namespace std;

int main(int argc, const char **argv) {
  string url{"localhost"};
  const string user{"root"};
  const string pass{"lanasucks"};
  const string database{"mlaas"};

  /* sql::ResultSet.rowsCount() returns size_t */
  size_t row;
  stringstream sql;
  stringstream msg;
  int i, affected_rows;

  cout << boolalpha;
  cout << "1..1" << endl;
  cout << "# Connector/C++ connect basic usage example.." << endl;
  cout << "#" << endl;

  try {
    sql::Driver* driver{ sql::mysql::get_driver_instance() };
    std::unique_ptr<sql::Connection> conn{ driver->connect(url, user, pass) };
    conn->setSchema(database);

    /* Creating a "simple" statement - "simple" = not a prepared statement */
    std::unique_ptr<sql::Statement> stmt{ conn->createStatement() };

    /* Create a test table demonstrating the use of sql::Statement.execute() */
    stmt->execute("SET NAMES utf8;");
    stmt->execute("DROP TABLE IF EXISTS test");
    stmt->execute("CREATE TABLE test(id INT, name VARCHAR(256) CHARACTER SET utf8)");
    cout << "#\t Test table created" << endl;

    sql.str("");
    sql << "INSERT INTO test(id, name) VALUES (";
    sql << "1" << ", '" << "test_name話" << "')";
    cout << "#\tstatement: " << sql.str() << endl;
    stmt->execute(sql.str());
    cout << "#\t Test table populated" << endl;

      /*
        Run a query which returns exactly one result set like SELECT
        Stored procedures (CALL) may return more than one result set
      */
    std::unique_ptr<sql::ResultSet> res{
      stmt->executeQuery("SELECT id, name FROM test ORDER BY id ASC")};
    cout << "#\t Running 'SELECT id, label FROM test ORDER BY id ASC'" << endl;

      /* Number of rows in the result set */
      cout << "#\t\t Number of rows\t";
      cout << "res->rowsCount() = " << res->rowsCount() << endl;

      /* Fetching data */
      row = 0;
      while (res->next()) {
        cout << "#\t\t Fetching row " << row << "\t";
        /* You can use either numeric offsets... */
        cout << "id = " << res->getInt(1);
        /* ... or column names for accessing results. The latter is recommended. */
        cout << ", name = '" << res->getString("name") << "'" << endl;
        row++;
      }

  //   {
  //     /* Fetching again but using type convertion methods */
  //     std::auto_ptr< sql::ResultSet > res(stmt->executeQuery("SELECT id FROM test ORDER BY id DESC"));
  //     cout << "#\t Fetching 'SELECT id FROM test ORDER BY id DESC' using type conversion" << endl;
  //     row = 0;
  //     while (res->next()) {
  //       cout << "#\t\t Fetching row " << row;
  //       cout << "#\t id (int) = " << res->getInt("id");
  //       cout << "#\t id (boolean) = " << res->getBoolean("id");
  //       cout << "#\t id (long) = " << res->getInt64("id") << endl;
  //       row++;
  //     }
  //   }

  //   /* Usage of UPDATE */
  //   stmt->execute("INSERT INTO test(id, label) VALUES (100, 'z')");
  //   affected_rows = stmt->executeUpdate("UPDATE test SET label = 'y' WHERE id = 100");
  //   cout << "#\t UPDATE indicates " << affected_rows << " affected rows" << endl;
  //   if (affected_rows != 1) {
  //     msg.str("");
  //     msg << "Expecting one row to be changed, but " << affected_rows << "change(s) reported";
  //     throw runtime_error(msg.str());
  //   }

  //   {
  //     std::auto_ptr< sql::ResultSet > res(stmt->executeQuery("SELECT id, label FROM test WHERE id = 100"));

  //     res->next();
  //     if ((res->getInt("id") != 100) || (res->getString("label") != "y")) {
  //       msg.str("Update must have failed, expecting 100/y got");
  //       msg << res->getInt("id") << "/" << res->getString("label");
  //       throw runtime_error(msg.str());
  //     }

  //     cout << "#\t\t Expecting id = 100, label = 'y' and got id = " << res->getInt("id");
  //     cout << ", label = '" << res->getString("label") << "'" << endl;
  //   }

  //   /* Clean up */
  //   stmt->execute("DROP TABLE IF EXISTS test");
  //   stmt.reset(NULL); /* free the object inside  */

  //   cout << "#" << endl;
  //   cout << "#\t Demo of connection URL syntax" << endl;
  //   try {
  //     /*s This will implicitly assume that the host is 'localhost' */
  //     url = "unix://path_to_mysql_socket.sock";
  //     con.reset(driver->connect(url, user, pass));
  //   } catch (sql::SQLException &e) {
  //     cout << "#\t\t unix://path_to_mysql_socket.sock caused expected exception" << endl;
  //     cout << "#\t\t " << e.what() << " (MySQL error code: " << e.getErrorCode();
  //     cout << ", SQLState: " << e.getSQLState() << " )" << endl;
  //   }

  //   try {
  //     url = "tcp://hostname_or_ip[:port]";
  //     con.reset(driver->connect(url, user, pass));
  //   } catch (sql::SQLException &e) {
  //     cout << "#\t\t tcp://hostname_or_ip[:port] caused expected exception" << endl;
  //     cout << "#\t\t " << e.what() << " (MySQL error code: " << e.getErrorCode();
  //     cout << ", SQLState: " << e.getSQLState() << " )" << endl;
  //   }

  //   try {
  //     /*
  //       Note: in the MySQL C-API host = localhost would cause a socket connection!
  //       Not so with the C++ Connector. The C++ Connector will translate
  //       tcp://localhost into tcp://127.0.0.1 and give you a TCP connection
  //       url = "tcp://localhost[:port]";
  //     */
  //     con.reset(driver->connect(url, user, pass));
  //   } catch (sql::SQLException &e) {
  //     cout << "#\t\t tcp://hostname_or_ip[:port] caused expected exception" << endl;
  //     cout << "#\t\t " << e.what() << " (MySQL error code: " << e.getErrorCode();
  //     cout << ", SQLState: " << e.getSQLState() << " )" << endl;
  //   }

  //   cout << "# done!" << endl;

  } catch (sql::SQLException &e) {
    /*
      The MySQL Connector/C++ throws three different exceptions:

      - sql::MethodNotImplementedException (derived from sql::SQLException)
      - sql::InvalidArgumentException (derived from sql::SQLException)
      - sql::SQLException (derived from std::runtime_error)
    */
    /* Use what() (derived from std::runtime_error) to fetch the error message */
    cout << "# ERR: " << e.what();
    cout << " (MySQL error code: " << e.getErrorCode();
    cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    cout << "not ok 1 - examples/connect.php" << endl;

    return EXIT_FAILURE;
  } catch (std::runtime_error &e) {

    cout << "# ERR: " << e.what() << endl;
    cout << "not ok 1 - examples/connect.php" << endl;

    return EXIT_FAILURE;
  }

  // cout << "ok 1 - examples/connect.php" << endl;
  // return EXIT_SUCCESS;
}
