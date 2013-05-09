/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jboss.arquillian.persistence.script;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ResourceAccessor;
import org.jboss.arquillian.persistence.dbunit.exception.DBUnitDataSetHandlingException;

/**
 *
 * @author kumm
 */
public class ChangelogExecutor
{

   private Connection connection;

   public ChangelogExecutor(Connection connection)
   {
      this.connection = connection;
   }

   public void execute(String changelogFile)
   {
      Database database;
      Liquibase liquibase;
      try
      {
         database = createDatabase(connection);
      }
      catch (DatabaseException ex)
      {
         throw new RuntimeException("Can't use database!", ex);
      }
      try
      {
         liquibase = new Liquibase(changelogFile, new ChangelogResourceAccessor(), database);
      }
      catch (LiquibaseException ex)
      {
         throw new RuntimeException("Error while parsing changelog file: " + changelogFile, ex);
      }
      try
      {
         liquibase.update(null);
      }
      catch (LiquibaseException ex)
      {
         throw new DBUnitDataSetHandlingException("Unable to execute changelogfile: " + changelogFile, ex);
      }
   }

   private Database createDatabase(Connection connection) throws DatabaseException
   {
      DatabaseConnection liquibaseConnection = new NoTransactionControlJdbcConnection(connection);
      return DatabaseFactory.getInstance().findCorrectDatabaseImplementation(liquibaseConnection);
   }

   private static class NoTransactionControlJdbcConnection extends JdbcConnection
   {

      public NoTransactionControlJdbcConnection(Connection connection)
      {
         super(connection);
      }

      @Override
      public void commit() throws DatabaseException
      {
      }

      @Override
      public void releaseSavepoint(Savepoint savepoint) throws DatabaseException
      {
      }

      @Override
      public void rollback() throws DatabaseException
      {
      }

      @Override
      public void rollback(Savepoint savepoint) throws DatabaseException
      {
      }

      @Override
      public Savepoint setSavepoint() throws DatabaseException
      {
         return null;
      }

      @Override
      public Savepoint setSavepoint(String name) throws DatabaseException
      {
         return null;
      }

      @Override
      public void setTransactionIsolation(int level) throws DatabaseException
      {
      }

      @Override
      public void setAutoCommit(boolean autoCommit) throws DatabaseException
      {
      }
   }

   private static class ChangelogResourceAccessor implements ResourceAccessor
   {

      @Override
      public InputStream getResourceAsStream(String file) throws IOException
      {
         return Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
      }

      @Override
      public Enumeration<URL> getResources(final String packageName) throws IOException
      {
         return null;
      }

      @Override
      public ClassLoader toClassLoader()
      {
         return Thread.currentThread().getContextClassLoader();
      }
   }
}
