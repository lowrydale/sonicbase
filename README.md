# SonicBase

SonicBase is an in-memory distributed relational database. It is linearly scalable and can hold many billions of records. SonicBase is cross-platform supporting MacOS, Linux, Windows and Cygwin. Extreme performance can be achieved with stored procedures. SonicBase support cross-shard transactions and cross-shard joins. An administrative client is included. SonicBase provides a JDBC driver.

## For the full **documentation**, follow this [link](https://sonicbase.com/documentation.html)

-----



#    Tutorial - Local Deploy
In this tutorial we will walk you through the process of starting a local cluster and inserting into and reading from it.

###     Download The Software
 Download the latest software from this site.
 
###  Unpack Package
Linux/Mac: Type "tar -xzf sonicbase-&lt;version&gt;.tgz" in the parent directory where you want the software to go. A directory named "sonicbase" will be created.

Windows: Unzip the file sonicbase-&lt;version&gt;.zip in the parent directory where you want the software to go. A directory named "sonicbase" will be created.

### Start Admin Client
Change to the sonicbase/bin directory and type "./cli" for MacOS and Linux and "./cli.bat" for cygwin and "cli.bat" for Windows. This will start the SonicBase admin client.

### Use Cluster
In the client, type:
    "use cluster 1-local"
This will allow you to use the "1-local" cluster. All subsequent operations will be performed on this cluster. You can create other clusters by adding a config file for each cluster in the sonicbase/config directory. The name of the file must be "config-&lt;cluster name&gt;.json". 

### Start Cluster
In the client, type:
    "start cluster"
This will start the cluster on the local machine.

### Create Database
In the client, type:
    "create database db"
This will create the database named "db".
    
### Create Table
In the client, type:
    "create table persons (name VARCHAR, age INTEGER, ssn VARCHAR, PRIMARY KEY (ssn))"
    
### Insert Record
In the client, type:
    "insert into persons (name, age, ssn) VALUES ('bob', 22, '555-66-7777')"

### Read Record
In the client, type:
    "select * from persons"
You should see the inserted record displayed in the client.
    
### Access From JDBC Driver
Include the SonicBase jdbc driver in your application. The jar is located in the "lib" directory of the install directory. It is named "sonicbase-jdbc-\[version\].jar". Or you can include the jar from the Maven Cental Repository.

If you are using maven, add the jdbc jar to your project pom.xml file. In the dependencies section add the following:

~~~
<dependency>
    <groupId>com.sonicbase</groupId>
    <artifactId>sonicbase-jdbc</artifactId>
    <version>[version]</version>
</dependency>
~~~
        The jdbc version should match your SonicBase server version.

Create and run the following class:

~~~
package foo;

public class Tutorial {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.sonicbase.jdbcdriver.Driver");
        try (Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010/db");
            PreparedStatement stmt = conn.prepareStatement("select * from persons");
            ResultSet rs = stmt.executeQuery()) {
            rs.next();
            System.out.println(rs.getString("name") + " " + rs.getInt("age") + " " + rs.getString("ssn"));
        }
    }
}
~~~

## For a tutorial using a remote cluster follow this [link](https://sonicbase.com/documentation.html)

