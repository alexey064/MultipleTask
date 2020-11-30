package com.TestTask.MultipleTask;


import java.io.*;
import java.sql.*;

class SQLProcedure
{
    /**данный метод создает базу данных в той же папке что и исполняемый файл*/
     static boolean SQLDataBaseCreate()
    {
        String str;
        str = "CREATE DATABASE Collection ON PRIMARY " +
                "(NAME = Collection, " +
                "FILENAME = '"+ System.getProperty("user.dir")+"\\Collection.mdf', " +
                "SIZE = 2MB, MAXSIZE = 100MB, FILEGROWTH = 10%) " +
                "LOG ON (NAME = Collection_Log, " +
                "FILENAME = '"+System.getProperty("user.dir")+"\\Collection.ldf', " +
                "SIZE = 1MB, " +
                "MAXSIZE = 100MB, " +
                "FILEGROWTH = 10%)";

       Logs.WriteLog("запрос sql: "+str);
        Connection myConn = null;
        try
        {
            myConn= DriverManager.getConnection("jdbc:sqlserver://localhost;integratedSecurity=true;");
            Statement myCommand= myConn.createStatement();
            //Log.Trace("Открываем соединение с базой данных");
            myCommand.execute(str);
            System.out.println("База данных успешно создана");
        }
        catch (SQLException ex)
        {
            System.out.println("В данном каталоге нельзя создать базу данных. Попробуйте перенести программу на другой диск, либо измените права доступа");
            Logs.WriteLog(ex.getMessage());
            System.out.println(ex.toString());
            return false;
        }
        finally
        {
            Logs.WriteLog("Отключаем соединение с базой данных");
            try {
                if (myConn != null) {
                    myConn.close();
                }
            } catch (SQLException e)
            {
                Logs.WriteLog("не удалось правильно закрыть соединение.");
            }
        }
        Logs.WriteLog("Метод SQLDataBaseCreate выполнен");
        return true;
    }
    /** возвращает строку подключения к базе данных
     * @return строка подключения
     * */
    private static String GetConnectionString()
    {
        Logs.WriteLog("Вызван метод GetConnectionString");
        String ConnectionString= "jdbc:sqlserver://localhost;integratedSecurity=true;databasename=Collection;AttachDBFilename="+System.getProperty("user.dir")+"\\\\Collection.mdf";
        Logs.WriteLog("Метод GetConnectionString завершен и возвращает connectionstring="+ConnectionString);
        return ConnectionString;
    }
    /** проверяет базу данных на наличие.
     * @return true - база существует, false - база не существует
     */
      static boolean IsDBCreated() throws Exception {
        Logs.WriteLog("Вызван метод IsDBCreated");
        boolean IsCreated = false;
        Connection sql= null;
        try
        {
            sql = DriverManager.getConnection(GetConnectionString());
            Logs.WriteLog("Открывается подключение к базе данных");
            IsCreated = true;
        }
        catch (SQLException ex)
        {
            Logs.WriteLog(ex.getMessage());
            return IsCreated;
        }
            Logs.WriteLog("закрывается соединение с базой данных");
                if (sql!=null) {
                    sql.close();
                }
        Logs.WriteLog("Метод IsDBCreated завершен и вернул значение "+IsCreated);
        return IsCreated;
    }

    /** записывает данные из xml файла в файл формата sql
    @param xml файл, содержащий информацию о БД
    @param values содержит одно значение для каждого столбца БД */
     static boolean WriteDataToProcedure(XmlFileReader xml, String[] values) {
        Logs.WriteLog("Вызван метод WriteDataTtoProcedure с значениями values=");
        //if not exists (select top(1) from Param1 where stolb1 = param2 )
        //insert param1(stolb1, stolb2...) values(value1, value2, value3...)
        String Stroka = " if not exists (select top(1) * from " + xml.TableName + " where " + xml.Column[0].getName() + "=" + values[0] + ") insert "+xml.TableName+"(";
        for (int i=0; i<values.length; i++)
        {
            Stroka += xml.Column[i].getName();
            if (values.length!=i+1)
            {
                Stroka += ", ";
            }
        }
        Stroka += ") values(";
        for (int i = 0; i < values.length; i++)
        {
            if (xml.Column[i].getTipe().contains("nvarchar"))
            {
                Stroka += "\'" + values[i] + "\'";
            }
            else Stroka += values[i];
            if (values.length != i + 1)
            {
                Stroka += ", ";
            }
        }
        Stroka += ")";
        Logs.WriteLog("строка с sql кодом:"+Stroka);
        Logs.WriteLog("Записываем строку в файл "+xml.FilePath+".sql");
        try(BufferedWriter bfw= new BufferedWriter(new FileWriter(xml.FilePath + ".sql", true)))
        {
            bfw.write(Stroka);
        }catch (IOException e)
        {
            Logs.WriteLog("не удалось записать данные в файл "+xml.FilePath + ".sql");
            Logs.WriteLog(e.getMessage());
            return false;
        }
        Logs.WriteLog("метод WriteDataToProcedure завершен");
        return true;
    }

    /** данный метод выполняет хранимую процедуру
    @param TableName определяет какая процедура будет вызвана
    @return возвращает успешность выполнения процедуры
    */
     static boolean AccomplishProcedure(String TableName)
    {
        //Log.Trace("вызван метод AccomplishProcedure");
        //Log.Trace("вызывается процедура "+command.CommandText);
        try (Connection sql = DriverManager.getConnection(GetConnectionString())) {
            Statement command = sql.createStatement();
            //Log.Trace("Подключаемся к базе данных");
            command.execute(TableName + "Pro");
        } catch (Exception ex) {
            Logs.WriteLog("не удалось подключиться к базе данных. Ошибка:" + ex.getMessage());
            return false;
        }
        return true;
    }
    /** записывает в процедуру создание таблицы
    @param xml содержит данные для создания таблицы
    */
     static boolean WriteTableHeaderProcedure(XmlFileReader xml) {
        Logs.WriteLog("Вызван метод WriteTableHeaderProcedure");
        String stroka = "IF NOT EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='"+xml.TableName+"' AND xtype='U') Create table dbo." + xml.TableName + "(" + xml.Column[0].getName() + " " + xml.Column[0].getTipe() + " primary key ";
        for (int i = 1; i < xml.Column.length; i++)
        {
            stroka+=","+xml.Column[i].getName()+"  "+xml.Column[i].getTipe();
        }
        stroka += ");";
        Logs.WriteLog("строка sql запроса: " + stroka);
        Logs.WriteLog("записываем в файл "+xml.FilePath+"TableCreate.sql");
        try(BufferedWriter bfw = new BufferedWriter(new FileWriter(xml.FilePath+"TableCreate.sql",false)))
        {
            bfw.write(stroka);
        } catch (IOException e)
        {
            Logs.WriteLog(e.getMessage());
            return false;
        }
        Logs.WriteLog("завершен метод WriteTableHeaderProcedure");
        return true;
     }

    /** Выполняет создание таблицы на основе уже созданного sql запроса.
    @param FilePath путь sql файлу, который создает таблицу
     */
     static boolean CreateTable(String FilePath) {
         Logs.WriteLog("вызван метод CreateTable");
         Connection sql = null;
         try (BufferedReader bfr = new BufferedReader(new FileReader(FilePath + "TableCreate.sql"))) {
             try {
                 sql = DriverManager.getConnection(GetConnectionString());
                 Statement command = sql.createStatement();
                 //Log.Trace("открываем соединение с базой данных");
                 //Log.Trace("выполняем sql:"+command.CommandText);
                 command.execute(bfr.readLine());
             } catch (Exception ex) {
                 Logs.WriteLog("Не удалось создать таблицу");
                 Logs.WriteLog(ex.getMessage());
                 return false;
             } finally {
                 try {
                     if (sql != null) {
                         sql.close();
                     }
                 } catch (SQLException e) {
                     Logs.WriteLog(e.getMessage());
                 }
             }
         }catch (IOException e){ Logs.WriteLog(e.getMessage());return false;}
         return true;
     }
    /** возвращает начальную часть хранимой процедуры
    @param TableName название таблицы, используется для названия процедуры
     @return возвращает шапку процедуры
    */
    static String GetProcedureHeader(String TableName)
    {
        Logs.WriteLog("вызван метод GetProcedureHeader");
        String stroka = "CREATE PROCEDURE "+TableName+"Pro";
        stroka += " as begin";
        Logs.WriteLog("метод GetProcedureHeader завершен и вернул: "+stroka);
        return stroka;
    }
    /** проверяет наличие хранимой процедуры, и если нету - отправляет на сервер.
     * @param FilePath содержит файл, который хранит sql команды
     * @param TableName название таблицы, используется для определения процедуры
     * @return возвращает успешность выполнения функции
    */
    static boolean WriteToDB(String TableName, String FilePath) {
        Logs.WriteLog("Вызван метод WriteToDB");
        String stroka = "select COUNT(*) from sysobjects where name = '" + TableName + "Pro' and xtype = 'P'";
        boolean IsCreated = false;
        Connection sql=null;
        Logs.WriteLog("подключаемся к базе данных и выполняем команду:"+stroka);
        try
        {
            sql = DriverManager.getConnection(GetConnectionString());
            Statement command = sql.createStatement();
            ResultSet data = command.executeQuery(stroka);
            data.next();
            int count=data.getInt(1);
            if(count>0)
            {
                return true;
            }
        }
        catch (Exception e)
        {
            Logs.WriteLog("Не удалось узнать наличие хранимой процедуры");
            Logs.WriteLog(e.getMessage());
            return false;
        }
        finally { Logs.WriteLog("закрывается подключение к базе данных"); }
        try(BufferedReader bfr = new BufferedReader(new FileReader(FilePath+".sql")))
        {
            try
            {
                String sqlProc="";
                String temp="";
                Logs.WriteLog("открывается подключение к базе данных");
                while ((temp=bfr.readLine())!=null)
                {
                    sqlProc += temp;
                }
                Logs.WriteLog("выполняем sql код:"+sqlProc);
                sql.createStatement().execute(sqlProc);
            }
            catch (Exception ex)
            {
                Logs.WriteLog(ex.getMessage());
                return false;
            }
            finally {
                try{sql.close();
                }catch (SQLException ex){ Logs.WriteLog(ex.getMessage()); return false;}
            }
        } catch (IOException e)
        {
            Logs.WriteLog(e.getMessage());
            return false;
        }
        Logs.WriteLog("выполнена функция WriteToDB");
        return true;
    }

    /** выполняет sql запрос на согласно второму дополнительному заданию
    @param xml определяет название таблицы для запроса
    */
     static boolean Output(XmlFileReader xml) {
        Logs.WriteLog("Вызван метод Output");
        String stroka="";
        stroka += "select ";
        for (int i = 0; i < xml.Column.length; i++)
        {
            stroka += xml.Column[i].getName()+"";
            if (xml.Column.length-1>i)
            {
                stroka += ", ";
            }
        }
        stroka += " from " + xml.TableName +" order by "+xml.Column[2].getName()+", "+ xml.Column[1].getName();
        Connection sql =null;
        Logs.WriteLog("код sql запроса:" +stroka);
        try
        {
            Logs.WriteLog("Открываем соединение с базой данных");
            sql=DriverManager.getConnection(GetConnectionString());
            Statement command = sql.createStatement();
            ResultSet reader = command.executeQuery(stroka);
            for (int i = 0; i < xml.Column.length; i++)
            {
                System.out.print(xml.Column[i].getName()+"\t");
            }
            System.out.print("\n");
            while (reader.next())
            {
                for (int i = 1; i <= xml.Column.length; i++)
                {
                    System.out.print(reader.getString(i)+"\t");
                }
                System.out.print("\n");
            }
        }
        catch (Exception ex)
        {
            Logs.WriteLog(ex.getMessage());
            return false;
        }
        finally {
            if (sql != null) {
                try {
                    sql.close();
                } catch (SQLException e) {
                    Logs.WriteLog(e.getMessage());
                }
            }
        }
        Logs.WriteLog("подключение к бд закрыто, метод output завершен");
        return true;
    }
}
