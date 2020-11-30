package com.TestTask.MultipleTask;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class XmlFileReader
{
    private String CurrentThread="MainThread";

    public String getFilePath() {
        return FilePath;
    }

    private void setFilePath(String filePath) {
        FilePath = filePath;
    }

    public String getTableName() {
        return TableName;
    }

    private void setTableName(String tableName) {
        TableName = tableName;
    }

    public String FilePath; //путь к файлу.
    public ColumnInfo[] Column; //набор столбцов, которые будут использованы при создании бд
    public String TableName=""; //название таблицы, которое используется для sql запросов

    public XmlFileReader(String filePath, String Currentthread)
    {
        CurrentThread = Currentthread;
        FilePath = filePath;
    }
    public XmlFileReader(String filePath)
    {
        FilePath = filePath;

    }
     boolean XmlInit()
    {
        if(!GetTableName()){return false;}
        return GetColumnInfo();
    }

    /**читает xml подобный файл, и записывает его содержимое в храниму процедуру.
    */
    boolean ReadFile()
    {//метод читает скачанные файлы и создаёт хранимую процедуру для sql server.
        Logs.WriteLog(CurrentThread+": вызван метод ReadFile");
        String stroka="";
        if (Files.exists(Paths.get(this.FilePath + ".sql")))
        {
            Logs.WriteLog(CurrentThread+": файл "+FilePath+".sql существует. Удаляем его");
            try {
                Files.delete(Paths.get(this.FilePath + ".sql"));
            } catch (IOException e)
            {
                System.out.println("не удалось удалить/обновить файл "+ this.FilePath + ".sql");
                Logs.WriteLog(e.getMessage());
                return false;
            }
        }
        String[] values = new String[Column.length];
        Logs.WriteLog(CurrentThread + ": записываем в файл " +FilePath+".sql");
        try(BufferedWriter bfw = new BufferedWriter(new FileWriter(FilePath+".sql")))
        {
            bfw.write(SQLProcedure.GetProcedureHeader(this.TableName));
        } catch (IOException e)
        {
            System.out.println("Не удалось записать \"шапку\" процедуры ");
            return false;
        }
        Logs.WriteLog(CurrentThread + ": Закрываем файл и открываем " + FilePath + " для чтения");
        try(BufferedReader bfw = new BufferedReader(new FileReader(FilePath)))
        {
            bfw.readLine();
            int k=0;
            while ((stroka=bfw.readLine())!=null)
            {
                k++;
                if (k==18)
                {
                    k=k;
                }
                Logs.WriteLog(CurrentThread + ": считали строку: " +stroka);
                if (!stroka.equals("</" + this.TableName + ">"))
                {
                    for (int i = 0; i < Column.length; i++)
                    {
                        values[i] = GetValue(stroka, i);
                    }
                    if(!SQLProcedure.WriteDataToProcedure(this, values))
                    {return false;}
                } else break;
            }
        } catch (IOException e)
        {
            System.out.println("Не удалось записать данные в процедуру");
            return false;
        }
        Logs.WriteLog(CurrentThread + ": закрываем файл" +FilePath+"и открываем файл "+FilePath+".sql");
        try(BufferedWriter bfw = new BufferedWriter(new FileWriter(FilePath + ".sql", true)))
        {
            bfw.write("end");
        } catch (IOException e){ return false;}
        Logs.WriteLog(CurrentThread + ": завершен метод ReadFile");
        return true;
    }


    /** Определяет название таблицы для sql запросов на основе первой строки xml файла*/
    private boolean GetTableName()
    {
        Logs.WriteLog(CurrentThread + ": вызван метод GetTableName для обьекта");
        Logs.WriteLog(CurrentThread + ": открываем файл " +FilePath);
        try(BufferedReader bfw = new BufferedReader(new FileReader(FilePath)))
        {
            String temp = bfw.readLine();
            Logs.WriteLog(CurrentThread + ": Прочитали строку: " +temp);
            for (int i = 1; i < temp.length()-1; i++)
            {
                TableName += temp.charAt(i);
            }
        } catch (IOException e)
        { Logs.WriteLog(e.toString()); return false;}
        Logs.WriteLog(CurrentThread + ": закончен метод GetTableName");
        return true;
    }
    
    /** возвращает набор столбцов таблицы, на основе выбранного файла*/
    private boolean GetColumnInfo()
    {
        Logs.WriteLog(CurrentThread + ": вызван метод GetColumnInfo");
        //получает информацию о  названиях и количестве столбцов, которые должны использоваться в таблице.
        //Возвращает набор столбцов, без определения их типов.
        Logs.WriteLog(CurrentThread + ": Открываем файл " +FilePath);
        try(BufferedReader bfr = new BufferedReader(new FileReader(FilePath)))
        {
            bfr.readLine();
            String stroka = bfr.readLine();
            Logs.WriteLog(CurrentThread + ": прочитали строку: " +stroka);
            int ParamCount = GetParamCount(stroka);
            Column = new ColumnInfo[ParamCount];
            for (int i = 0; i < ParamCount; i++)
            {
                Column[i]= new ColumnInfo(getName(stroka, i), GetSingleTipe(GetValue(stroka,i)));
            }
        } catch (IOException e){System.out.println("Не удалось прочитать файл FilePath"); return false;}
        Logs.WriteLog(CurrentThread + ": завершен метод GetColumnInfo");
        return true;
    }

    /** возвращает количество столбцов, используемых в одной записи sql
     @param stroka - строка, которая содержит все столбцы в необработанном виде*/
    private int GetParamCount(String stroka)
    {
        Logs.WriteLog(CurrentThread + ": вызван метод GetParamCount с параметрами stroka=" +stroka);
        int output = 0;
        for (int i = 0; i < stroka.length(); i++)
        {
            if (stroka.charAt(i) == '=' && stroka.charAt(i+1) == '\"')
            {
                output++;
            }
        }
        Logs.WriteLog(CurrentThread + ": метод GetParamCount завершен и вернул output=" +output);
        return output;
    }

    /** получает название одного столбца
     @param stroka необработанная строка из которой надо извлечь одно название столбца
     @param index порядковый номер столбца записи*/
    private String getName(String stroka, int index)
    {
        Logs.WriteLog(CurrentThread + ": вызван метод getName с параметрами stroka=" +stroka+" index="+index);
        String output = "";
        int counter = 0;
        for (int i = 0; i < stroka.length(); i++)
        {
            if (stroka.charAt(i) == '=' && stroka.charAt(i+1) == '\"')
            {
                if (counter!=index)
                {
                    counter++;
                    continue;
                }
                for (int j = 1; j < i; j++)
                {
                    if (stroka.charAt(i-j) == ' ')
                    {
                        for (int k = i - j + 1; k < i; k++)
                        {
                            output += stroka.charAt(k);
                        }
                        Logs.WriteLog(CurrentThread + ":закончен метод getName и вернул результат output=" +output);
                        return output;
                    }
                }
            }
        }
        Logs.WriteLog(CurrentThread + ": Закончен метод getName и вернул результат null");
        return null;
    }

    /** возвращает значение определенного столбца xml файла
     * @param Index номер столбца из которого извлекается значение
     * @param Stroka необработанная строка из которой надо получить значение столбца
     * @return значение заданного столбца
*/
    private String GetValue(String Stroka, int Index)
    {
        Logs.WriteLog(CurrentThread + ": вызван метод GetValue с параметрами stroka=" +Stroka+" Index="+Index);
        String output="";
        int Count = 0;
        for (int j = 0; j < Stroka.length(); j++)
        {
            if (Stroka.charAt(j) == '=' && Stroka.charAt(j+1) == '\"')
            {
                if (Count!=Index)
                {
                    Count++;
                    continue;
                }
                j = j + 2;
                while (Stroka.charAt(j) != '\"')
                {
                    output += Stroka.charAt(j);
                    j++;
                }
                Logs.WriteLog(CurrentThread + ": метод getValue завершен и вернул output=" +output);
                return output;
            }
        }
        Logs.WriteLog(CurrentThread + ": метод getValue завершен и вернул null");
        return null;
    }


    /** возвращает название типа данных столбца. Испоьзуется при создании запроса на создание таблицы.
    * @param Value значение определенного столбца, в котором надо определить тип
     * @return  название типа, которое будет использоваться в sql server
    */
    private String GetSingleTipe(String Value)
    {
        //Log.Trace(CurrentThread + ": вызван метод GetSingleTipe с параметром value=" +Value);

        //функция возвращает тип содержимого в виде строки.
        String Tipe = "int";
        if ((Value.length() == 8 || Value.length() == 10) && Value.charAt(2) == '.' && Value.charAt(5) == '.')
        {
            //Log.Trace(CurrentThread + ":метод GetSingleTipe завершен и вернул: date");
            return "date";
        }
        if (Value.charAt(0) >= 48 && Value.charAt(0) <= 57 || Value.charAt(0)=='-') //нужно проверить эту часть кода. Если символ является числом
        {
            for (int i = 1; i < Value.length(); i++)
            {
                if (Value.charAt(i) >= 48 && Value.charAt(0) <= 57) //если символ является числом
                {
                    if (Value.length() - 1 == i) //если все вимволы - числа
                    {
                        //Log.Trace(CurrentThread + ": метод GetSingleTipe завершен и вернул: " + Tipe);
                        return Tipe;
                    }
                }
                else if (Value.charAt(i) == ',') //Если в последовательности чисел есть запятая - это признак double
                {
                    Tipe = "Decimal(18, 2)";
                }
                else
                {
                    //Log.Trace(CurrentThread + ": метод GetSingleTipe завершен и вернул: nvarchar(100)");
                    return "nvarchar(100)";
                }//если первый символ - число, а второе - текст, возвращаем тип Строка
            }
        }
        //Log.Trace("метод GetSingleTipe завершен и вернул: nvarchar(100)");
        return "nvarchar(100)";
    }
}
