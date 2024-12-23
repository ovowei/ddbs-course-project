/* Standard C++ includes */
#include <stdlib.h>

#include <iostream>

/*
  Include directly the different
  headers from cppconn/ and mysql_driver.h + mysql_util.h
  (and mysql_connection.h). This will reduce your build time!
*/
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

#include "mysql_connection.h"

using namespace std;

int main(void) {
    cout << endl;
    cout << "Running 'SELECT 'Hello World!' AS _message'..." << endl;
    try {
        sql::Driver *driver;
        sql::Connection *con;
        sql::Statement *stmt;
        sql::ResultSet *res;

        /* Create a connection */
        driver = get_driver_instance();
        con = driver->connect("tcp://127.0.0.1:3310", "root", "123456");

        stmt = con->createStatement();

        // 丢弃旧的 schema "test"
        stmt->execute("DROP DATABASE IF EXISTS test");

        // 创建新的 schema "test"
        stmt->execute("CREATE DATABASE test");

        // 切换到新的 schema "test"
        con->setSchema("test");

        res = stmt->executeQuery("SELECT 'Hello World!' AS _message");  // replace with
                                                                        // your statement
        while (res->next()) {
            cout << "\t... MySQL replies: ";
            /* Access column data by alias or column name */
            cout << res->getString("_message") << endl;
            cout << "\t... MySQL says it again: ";
            /* Access column fata by numeric offset, 1 is the
             * first column */
            cout << res->getString(1) << endl;
        }
        delete res;
        delete stmt;
        delete con;

    } catch (sql::SQLException &e) {
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }

    cout << endl;

    return EXIT_SUCCESS;
}