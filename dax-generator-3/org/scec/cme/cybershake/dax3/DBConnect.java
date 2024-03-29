package org.scec.cme.cybershake.dax3;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p>Title:DBConnect </p>
 * <p>Description: Interface to the CME database</p>
 * @author: Nitin Gupta & Vipin Gupta & Phil Maechling
 * @version 1.0
 */
public class DBConnect
{

  private Connection conn = null;
  private String hostName;
  private String dbName;
  private String userName;
  private String password;
  private int port = 3306;

  /**
   *class constructor
   */
  public DBConnect(String hostname, String dbName, String userName, String password)
  {
    this.hostName = hostname;
    this.dbName = dbName;
    this.userName = userName;
    this.password = password;
    try {
      getConnection();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Runs the select query on the database
   * @param query
   * @return
   */
  public ResultSet selectData(String query)
  {
    ResultSet result =null;
    Statement stat= null;
    try
    {
      getConnection();
      stat = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.HOLD_CURSORS_OVER_COMMIT);
      //System.out.println(query);
      //gets the resultSet after running the query
      result = stat.executeQuery(query+";");
    }
    catch (SQLException ex)
    {
      ex.printStackTrace();
      while (ex != null)
      {
        ex.printStackTrace();
        //ex = ex.getNextException();
      }
    }
    catch (Exception ex)
    {
        ex.printStackTrace();
    }
    finally
    {
      try
      {
        //stat.close();
        //conn.close();
      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    return result;
  }


  /**
   * Runs the inserts query on the database
   * @param query
   */
  public boolean insertData(String query)
  {

    Statement stat = null;

    try
    {
       getConnection();
      stat = conn.createStatement();
      //System.out.println(query);
      //executes the query
      stat.executeUpdate(query+";");
      return true;
    }
    catch (SQLException ex)
    {
      while (ex != null)
      {
        ex.printStackTrace();
        ex = ex.getNextException();
      }
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }
    finally
    {
      try
      {
        //stat.close();
        conn.close();
      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    return false;
  }


  /**
   * Runs the Delete and update operation on the tables in the database
   * @param query
   */
  public boolean deleteOrUpdateData(String query)
  {
    Statement stat = null;
    try
    {
      getConnection();
      stat = conn.createStatement();
      //executes the delete or update query
      stat.execute(query+";");
      //commits the result of the query to the database
      stat.execute("commit;");
      stat.close();
      return true;
    }
    catch (SQLException ex)
    {
      while (ex != null)
      {
        ex.printStackTrace();
        ex = ex.getNextException();
      }
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }
    finally
    {
      try
      {
        stat.close();
        conn.close();
      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    return false;
  }

  /**
   * Establishes the connection with the database using the mysql driver
   *
   * @return the database connection
   * @return
   * @throws SQLException
   * @throws IOException
   * @throws Exception
   */


  private void getConnection() throws SQLException, IOException,Exception {

    if(conn!=null && !conn.isClosed()) return ;
    String drivers = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://"+hostName+":"+port+"/"+dbName;

    //Try to load the driver, if this fails then print an error
    //and the contents of the stack
    try
    {
      Class.forName(drivers).getDeclaredConstructor((Class<?>[])null).newInstance();
      //Class.forName(drivers).newInstance();
    }
    catch (Exception E)
    {
      E.printStackTrace();
      throw new Exception("*** Unable to load database driver ***\n");
    }

     try
     {
       conn = DriverManager.getConnection(url,this.userName,this.password);
     }
     //Catch the exception, throw a new DBException
     catch (SQLException E)
     {
       throw new Exception("*** Unable to connect to the database ***"+
                           "\nSQL Message: "+E.getMessage()+
                           "\nSQL ErrorCode: "+E.getErrorCode()+
                           "\nSQL State: "+E.getSQLState()+"\n");
     }
     return;
  }
  
  public void closeConnection() {
	  try {
		  if (conn!=null && !conn.isClosed()) {
			  if (conn.getAutoCommit()==false) {
				  conn.commit();
			  }
			  conn.close();
		  }
	  } catch (SQLException ex) {
		  ex.printStackTrace();
	  }
  }
}
